//! Bounded, replayable text patches. Codex's ordered line matching is adapted from
//! OpenAI Codex apply-patch (Apache-2.0, Copyright 2025 OpenAI); Pi's replacement
//! matching is implemented independently from its published edit semantics.
use super::*;
use crate::MAX_FILE_BYTES;
use std::collections::{BTreeMap, HashMap};

pub const MAX_PATCH_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_PATCH_FILES: usize = 32;
pub const MAX_PATCH_EDITS: usize = 256;
pub const MAX_PATCH_AGGREGATE_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_PATCH_DIFF_BYTES: usize = 64 * 1024;
pub const MAX_PATCH_RESULT_BYTES: usize = 1_700_000;
const MAX_MATCH_WORK: usize = 30_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyPatchRequest {
    pub mutation_id: String,
    pub cwd: Option<PathBuf>,
    pub patch: PatchInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
pub enum PatchInput {
    Codex { text: String },
    TextReplacements { files: Vec<ReplacementFile> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementFile {
    pub path: PathBuf,
    pub edits: Vec<TextEdit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextEdit {
    pub old_text: String,
    pub new_text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchStatus {
    Applied,
    Rejected,
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchChangeKind {
    Add,
    Update,
    Delete,
    Move,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchChange {
    pub kind: PatchChangeKind,
    pub path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<PathBuf>,
    pub before_sha256: Option<String>,
    pub after_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_before_sha256: Option<String>,
    pub bytes_before: Option<usize>,
    pub bytes_after: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_bytes_before: Option<usize>,
    pub first_changed_line: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchFailure {
    pub code: ErrorCode,
    pub message: String,
    pub section: Option<usize>,
    pub edit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchReceipt {
    pub mutation_id: String,
    pub status: PatchStatus,
    pub changes_exact: bool,
    pub changes: Vec<PatchChange>,
    pub diff: String,
    pub diff_truncated: bool,
    pub error: Option<PatchFailure>,
}

impl PatchReceipt {
    fn new(id: String) -> Self {
        Self {
            mutation_id: id,
            status: PatchStatus::Applied,
            changes_exact: true,
            changes: Vec::new(),
            diff: String::new(),
            diff_truncated: false,
            error: None,
        }
    }
    fn fail(&mut self, error: Error, section: Option<usize>, edit: Option<usize>, uncertain: bool) {
        self.status = if uncertain || !self.changes.is_empty() {
            PatchStatus::Partial
        } else {
            PatchStatus::Rejected
        };
        self.changes_exact = !uncertain;
        self.error = Some(PatchFailure {
            code: error.code,
            message: error.message,
            section,
            edit,
        });
    }
}

#[derive(Clone)]
struct View {
    original: Option<Vec<u8>>,
    current: Option<Vec<u8>>,
}
struct Step {
    kind: PatchChangeKind,
    path: PathBuf,
    destination: Option<PathBuf>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    destination_before: Option<Vec<u8>>,
    section: usize,
}

#[derive(Default)]
struct PlanState {
    view: BTreeMap<PathBuf, View>,
    aliases: HashMap<PathBuf, PathBuf>,
    aggregate: usize,
    work: usize,
}

impl ProcessExecutionCore {
    pub async fn apply_patch(&self, request: ApplyPatchRequest) -> Result<PatchReceipt> {
        self.ensure_available()?;
        validate_mutation_id(&request.mutation_id)?;
        let raw_bytes = match &request.patch {
            PatchInput::Codex { text } => text.len(),
            PatchInput::TextReplacements { files } => files.iter().fold(0usize, |total, file| {
                file.edits.iter().fold(
                    total.saturating_add(file.path.as_os_str().len()),
                    |n, edit| {
                        n.saturating_add(edit.old_text.len())
                            .saturating_add(edit.new_text.len())
                    },
                )
            }),
        };
        if raw_bytes > MAX_PATCH_BYTES * 2 {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "raw patch request exceeds 4 MiB",
            ));
        }
        // Fingerprinting is independent of filesystem state, so replay is checked
        // before any file contents or metadata are consulted.
        let cwd = self.resolve_file_path(request.cwd.as_deref(), Path::new("."))?;
        let encoded = serde_json::to_vec(&request).map_err(|e| Error::invalid(e.to_string()))?;
        let fingerprint = MutationFingerprint::Patch {
            digest: sha256(&encoded),
        };
        let mut registry = self.0.file_mutations.lock().await;
        if let Some((previous, outcome)) = registry.entries.get(&request.mutation_id) {
            if previous != &fingerprint {
                return Err(idempotency_conflict());
            }
            return match outcome {
                MutationOutcome::Patch(receipt) => Ok(receipt.clone()),
                _ => Err(idempotency_conflict()),
            };
        }
        if encoded.len() > MAX_PATCH_BYTES * 2 {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "serialized patch request exceeds 4 MiB",
            ));
        }
        ensure_receipt_capacity(&registry, self.0.config.limits.max_file_mutation_receipts)?;
        let id = request.mutation_id.clone();
        let limit = self
            .0
            .config
            .limits
            .max_file_read_bytes
            .min(self.0.config.limits.max_file_write_bytes)
            .min(MAX_FILE_BYTES);
        let receipt = tokio::task::spawn_blocking(move || execute(request, cwd, limit))
            .await
            .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?;
        registry
            .entries
            .insert(id, (fingerprint, MutationOutcome::Patch(receipt.clone())));
        Ok(receipt)
    }
}

fn execute(request: ApplyPatchRequest, cwd: PathBuf, limit: usize) -> PatchReceipt {
    let mut receipt = PatchReceipt::new(request.mutation_id);
    let sections = match &request.patch {
        PatchInput::Codex { text } => {
            if text.len() > MAX_PATCH_BYTES {
                Err(Error::new(
                    ErrorCode::ResourceLimit,
                    "patch text exceeds 2 MiB",
                ))
            } else {
                parse_codex(text)
            }
        }
        PatchInput::TextReplacements { files } => {
            if files.is_empty()
                || files.len() > MAX_PATCH_FILES
                || files.iter().any(|f| f.edits.is_empty())
            {
                Err(Error::invalid(
                    "replacement files and edits must be nonempty and within limits",
                ))
            } else {
                Ok(files.iter().cloned().map(Section::Replace).collect())
            }
        }
    };
    let sections = match sections {
        Ok(s) => s,
        Err(e) => {
            receipt.fail(e, None, None, false);
            return receipt;
        }
    };
    let mut plan = PlanState::default();
    let mut steps = Vec::new();
    let edit_count: usize = sections
        .iter()
        .map(|s| match s {
            Section::Replace(f) => f.edits.len(),
            Section::Update { chunks, .. } => chunks.len(),
            _ => 1,
        })
        .sum();
    if sections.is_empty() || sections.len() > MAX_PATCH_FILES || edit_count > MAX_PATCH_EDITS {
        receipt.fail(
            Error::new(
                ErrorCode::ResourceLimit,
                "patch section/edit limit exceeded",
            ),
            None,
            None,
            false,
        );
        return receipt;
    }
    for (index, section) in sections.iter().enumerate() {
        let planned = plan_section(section, index, &cwd, limit, &mut plan);
        match planned {
            Ok(step) => steps.push(step),
            Err((error, edit)) => {
                receipt.fail(error, Some(index), edit, false);
                return receipt;
            }
        }
    }
    // Recheck all observed paths before the first write; this catches predictable
    // stale inputs without committing an earlier section.
    for (path, state) in &plan.view {
        if let Err(e) = verify(path, state.original.as_deref(), limit) {
            receipt.fail(e, None, None, false);
            return receipt;
        }
    }
    for step in steps {
        let source = &step.path;
        if let Err(e) = verify(source, step.before.as_deref(), limit) {
            receipt.fail(e, Some(step.section), None, false);
            break;
        }
        if let Some(destination) = &step.destination
            && let Err(e) = verify(destination, step.destination_before.as_deref(), limit)
        {
            receipt.fail(e, Some(step.section), None, false);
            break;
        }
        let mut destination_written = false;
        let result: Result<()> = (|| {
            match step.kind {
                PatchChangeKind::Delete => fs::remove_file(source).map_err(map_io)?,
                PatchChangeKind::Move => {
                    let destination = step.destination.as_ref().expect("move destination");
                    commit_planned_write(
                        destination,
                        step.after.as_deref().unwrap(),
                        step.destination_before.as_deref(),
                        true,
                        limit,
                    )?;
                    destination_written = true;
                    // A failed deletion leaves a destination that was written: partial.
                    verify(source, step.before.as_deref(), limit)?;
                    fs::remove_file(source).map_err(map_io)?;
                }
                PatchChangeKind::Add | PatchChangeKind::Update => {
                    commit_planned_write(
                        source,
                        step.after.as_deref().unwrap(),
                        step.before.as_deref(),
                        matches!(step.kind, PatchChangeKind::Add),
                        limit,
                    )?;
                }
            }
            Ok(())
        })();
        if let Err(e) = result {
            if destination_written {
                receipt.changes.push(change(
                    if step.destination_before.is_some() {
                        PatchChangeKind::Update
                    } else {
                        PatchChangeKind::Add
                    },
                    step.destination.clone().unwrap(),
                    None,
                    step.destination_before.as_deref(),
                    step.after.as_deref(),
                    None,
                ));
            }
            let uncertain = destination_written || e.code != ErrorCode::PreconditionFailed;
            receipt.fail(e, Some(step.section), None, uncertain);
            break;
        }
        let record = change(
            step.kind,
            source.clone(),
            step.destination.clone(),
            step.before.as_deref(),
            step.after.as_deref(),
            step.destination_before.as_deref(),
        );
        append_diff(&mut receipt, &step);
        receipt.changes.push(record);
    }
    // Paths are capped at 4096 bytes and there are at most 32 records, so
    // dropping the optional diff bounds even worst-case JSON-escaped receipts
    // below this cap. Never turn a committed mutation into an opaque error.
    if serde_json::to_vec(&receipt).is_ok_and(|bytes| bytes.len() > MAX_PATCH_RESULT_BYTES) {
        receipt.diff.clear();
        receipt.diff_truncated = true;
    }
    receipt
}

fn commit_planned_write(
    path: &Path,
    after: &[u8],
    before: Option<&[u8]>,
    create_parent_directories: bool,
    limit: usize,
) -> Result<()> {
    write_file_at(
        String::new(),
        path.to_path_buf(),
        after.to_vec(),
        create_parent_directories,
        WriteFileMode::Conditional(planned_precondition(before)),
        limit,
    )?;
    Ok(())
}

fn planned_precondition(before: Option<&[u8]>) -> FilePrecondition {
    match before {
        Some(bytes) => FilePrecondition::Sha256 {
            sha256: sha256(bytes),
        },
        None => FilePrecondition::Missing,
    }
}

fn change(
    kind: PatchChangeKind,
    path: PathBuf,
    destination: Option<PathBuf>,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
    destination_before: Option<&[u8]>,
) -> PatchChange {
    let first_changed_line = if let (Some(a), Some(b)) = (before, after) {
        let offset = a.iter().zip(b).take_while(|(x, y)| x == y).count();
        Some(1 + b[..offset].iter().filter(|&&c| c == b'\n').count())
    } else {
        Some(1)
    };
    PatchChange {
        kind,
        path,
        destination_path: destination,
        before_sha256: before.map(sha256),
        after_sha256: after.map(sha256),
        destination_before_sha256: destination_before.map(sha256),
        bytes_before: before.map(<[u8]>::len),
        bytes_after: after.map(<[u8]>::len),
        destination_bytes_before: destination_before.map(<[u8]>::len),
        first_changed_line,
    }
}

fn append_diff(receipt: &mut PatchReceipt, step: &Step) {
    if receipt.diff_truncated {
        return;
    }
    // One coarse but valid unified hunk per section. Computing a minimal diff
    // could be quadratic; stop emitting as soon as the display budget is spent.
    let before = step.before.as_deref().unwrap_or_default();
    let after = step.after.as_deref().unwrap_or_default();
    if before.len().saturating_add(after.len()) > 256 * 1024 {
        receipt.diff_truncated = true;
        return;
    }
    let (Ok(a), Ok(b)) = (std::str::from_utf8(before), std::str::from_utf8(after)) else {
        receipt.diff_truncated = true;
        return;
    };
    let old: Vec<&str> = a.lines().collect();
    let new: Vec<&str> = b.lines().collect();
    let mut prefix = 0;
    while prefix < old.len().min(new.len()) && old[prefix] == new[prefix] {
        prefix += 1;
    }
    let mut suffix = 0;
    while suffix < old.len().min(new.len()) - prefix
        && old[old.len() - 1 - suffix] == new[new.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let label = step.path.display();
    let header = format!(
        "--- a/{label}\n+++ b/{}\n@@ -{},{} +{},{} @@\n",
        step.destination.as_ref().unwrap_or(&step.path).display(),
        prefix + 1,
        old.len() - prefix - suffix,
        prefix + 1,
        new.len() - prefix - suffix
    );
    if !append_bounded(receipt, &header) {
        return;
    }
    for line in &old[prefix..old.len() - suffix] {
        if !append_line(receipt, '-', line) {
            return;
        }
    }
    for line in &new[prefix..new.len() - suffix] {
        if !append_line(receipt, '+', line) {
            return;
        }
    }
}

fn append_line(receipt: &mut PatchReceipt, marker: char, line: &str) -> bool {
    if receipt.diff.len() + line.len() + 2 > MAX_PATCH_DIFF_BYTES {
        receipt.diff_truncated = true;
        return false;
    }
    receipt.diff.push(marker);
    receipt.diff.push_str(line);
    receipt.diff.push('\n');
    true
}

fn append_bounded(receipt: &mut PatchReceipt, fragment: &str) -> bool {
    if receipt.diff.len() + fragment.len() > MAX_PATCH_DIFF_BYTES {
        receipt.diff_truncated = true;
        return false;
    }
    receipt.diff.push_str(fragment);
    true
}

fn verify(path: &Path, expected: Option<&[u8]>, limit: usize) -> Result<()> {
    let current = read_optional(path, limit)?;
    if current.as_deref() == expected {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::PreconditionFailed,
            format!("file changed before commit: {}", path.display()),
        ))
    }
}

fn read_optional(path: &Path, limit: usize) -> Result<Option<Vec<u8>>> {
    match read_file_at(path.to_path_buf(), limit) {
        Ok(result) => Ok(Some(result.data)),
        Err(e) if e.code == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// Canonicalize existing ancestors, following final symlinks for content writes.
// A distinct spelling of the same target is rejected even across sections.
fn target(cwd: &Path, input: &Path, aliases: &mut HashMap<PathBuf, PathBuf>) -> Result<PathBuf> {
    if input.as_os_str().is_empty() {
        return Err(Error::invalid("empty patch path"));
    }
    let joined = if input.is_absolute() {
        input.to_path_buf()
    } else {
        cwd.join(input)
    };
    let joined = if joined.is_absolute() {
        joined
    } else {
        std::env::current_dir().map_err(map_io)?.join(joined)
    };
    if joined.as_os_str().len() > 4096 {
        return Err(Error::new(
            ErrorCode::ResourceLimit,
            "patch path exceeds 4096 bytes",
        ));
    }
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            other => normalized.push(other.as_os_str()),
        }
    }
    let mut resolved = PathBuf::new();
    for component in joined.components() {
        match component {
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            std::path::Component::CurDir => {}
            other => {
                resolved.push(other.as_os_str());
                for _ in 0..40 {
                    let Ok(meta) = fs::symlink_metadata(&resolved) else {
                        break;
                    };
                    if !meta.file_type().is_symlink() {
                        break;
                    }
                    let link = fs::read_link(&resolved).map_err(map_io)?;
                    resolved = if link.is_absolute() {
                        link
                    } else {
                        resolved
                            .parent()
                            .ok_or_else(|| Error::invalid("invalid patch path"))?
                            .join(link)
                    };
                }
                if fs::symlink_metadata(&resolved).is_ok_and(|m| m.file_type().is_symlink()) {
                    return Err(Error::invalid("too many symbolic links"));
                }
                if resolved.exists() {
                    resolved = fs::canonicalize(&resolved).map_err(map_io)?;
                }
            }
        }
    }
    if let Some(prior) = aliases.insert(resolved.clone(), normalized.clone())
        && prior != normalized
    {
        return Err(Error::invalid(format!(
            "conflicting path aliases: {} and {}",
            prior.display(),
            normalized.display()
        )));
    }
    Ok(resolved)
}

fn state<'a>(
    path: &Path,
    view: &'a mut BTreeMap<PathBuf, View>,
    limit: usize,
    aggregate: &mut usize,
) -> Result<&'a mut View> {
    if !view.contains_key(path) {
        let original = read_optional(path, limit)?;
        *aggregate += original.as_ref().map_or(0, Vec::len);
        if *aggregate > MAX_PATCH_AGGREGATE_BYTES {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "aggregate original bytes exceed 20 MiB",
            ));
        }
        view.insert(
            path.to_path_buf(),
            View {
                current: original.clone(),
                original,
            },
        );
    }
    Ok(view.get_mut(path).expect("inserted"))
}

fn plan_section(
    section: &Section,
    index: usize,
    cwd: &Path,
    limit: usize,
    plan: &mut PlanState,
) -> std::result::Result<Step, (Error, Option<usize>)> {
    let path = target(cwd, section.path(), &mut plan.aliases).map_err(|e| (e, None))?;
    if matches!(section, Section::Replace(_)) && plan.view.contains_key(&path) {
        return Err((
            Error::invalid("replacement file appears more than once"),
            None,
        ));
    }
    if matches!(
        section,
        Section::Delete { .. }
            | Section::Update {
                move_to: Some(_),
                ..
            }
    ) {
        let lexical = if section.path().is_absolute() {
            section.path().to_path_buf()
        } else {
            cwd.join(section.path())
        };
        if fs::symlink_metadata(lexical).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err((
                Error::invalid("delete and move source must not be a symlink"),
                None,
            ));
        }
    }
    let before = state(&path, &mut plan.view, limit, &mut plan.aggregate)
        .map_err(|e| (e, None))?
        .current
        .clone();
    let mut destination = None;
    let mut destination_before = None;
    let (kind, after) = match section {
        Section::Add { content, .. } => (PatchChangeKind::Add, Some(content.as_bytes().to_vec())),
        Section::Delete { .. } => {
            if before.is_none() {
                return Err((
                    Error::new(ErrorCode::NotFound, "file to delete does not exist"),
                    None,
                ));
            }
            (PatchChangeKind::Delete, None)
        }
        Section::Update {
            chunks, move_to, ..
        } => {
            let data = before.as_ref().ok_or_else(|| {
                (
                    Error::new(ErrorCode::NotFound, "file to update does not exist"),
                    None,
                )
            })?;
            let text = std::str::from_utf8(data)
                .map_err(|_| (Error::invalid("patch source is not UTF-8"), None))?;
            let modified = apply_codex(text, chunks, &mut plan.work).map_err(|e| (e, None))?;
            if let Some(to) = move_to {
                let to = target(cwd, to, &mut plan.aliases).map_err(|e| (e, None))?;
                if to == path {
                    return Err((Error::invalid("move destination is the source"), None));
                }
                destination_before = state(&to, &mut plan.view, limit, &mut plan.aggregate)
                    .map_err(|e| (e, None))?
                    .current
                    .clone();
                destination = Some(to);
            }
            (
                if destination.is_some() {
                    PatchChangeKind::Move
                } else {
                    PatchChangeKind::Update
                },
                Some(modified.into_bytes()),
            )
        }
        Section::Replace(file) => {
            let data = before.as_ref().ok_or_else(|| {
                (
                    Error::new(ErrorCode::NotFound, "replacement target does not exist"),
                    None,
                )
            })?;
            let text = std::str::from_utf8(data)
                .map_err(|_| (Error::invalid("replacement source is not UTF-8"), None))?;
            let modified = apply_replacements(text, &file.edits, &mut plan.work)
                .map_err(|(e, edit)| (e, Some(edit)))?;
            (PatchChangeKind::Update, Some(modified.into_bytes()))
        }
    };
    if after.as_ref().is_some_and(|bytes| bytes.len() > limit) {
        return Err((
            Error::new(
                ErrorCode::ResourceLimit,
                "resulting file exceeds the configured limit (at most 5 MiB)",
            ),
            None,
        ));
    }
    plan.aggregate += after.as_ref().map_or(0, Vec::len);
    if plan.aggregate > MAX_PATCH_AGGREGATE_BYTES {
        return Err((
            Error::new(
                ErrorCode::ResourceLimit,
                "aggregate planned bytes exceed 20 MiB",
            ),
            None,
        ));
    }
    state(&path, &mut plan.view, limit, &mut plan.aggregate)
        .map_err(|e| (e, None))?
        .current = if destination.is_some() {
        None
    } else {
        after.clone()
    };
    if let Some(to) = &destination {
        state(to, &mut plan.view, limit, &mut plan.aggregate)
            .map_err(|e| (e, None))?
            .current = after.clone();
    }
    Ok(Step {
        kind,
        path,
        destination,
        before,
        after,
        destination_before,
        section: index,
    })
}

enum Section {
    Add {
        path: PathBuf,
        content: String,
    },
    Delete {
        path: PathBuf,
    },
    Update {
        path: PathBuf,
        move_to: Option<PathBuf>,
        chunks: Vec<Chunk>,
    },
    Replace(ReplacementFile),
}
impl Section {
    fn path(&self) -> &Path {
        match self {
            Self::Add { path, .. } | Self::Delete { path } | Self::Update { path, .. } => path,
            Self::Replace(f) => &f.path,
        }
    }
}
#[derive(Default)]
struct Chunk {
    anchor: Option<String>,
    old: Vec<String>,
    new: Vec<String>,
    eof: bool,
}

fn parse_codex(text: &str) -> Result<Vec<Section>> {
    let lines: Vec<&str> = text
        .trim()
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    if lines.first().is_none_or(|l| l.trim() != "*** Begin Patch")
        || lines.last().is_none_or(|l| l.trim() != "*** End Patch")
    {
        return Err(Error::invalid("expected *** Begin Patch and *** End Patch"));
    }
    let mut sections = Vec::new();
    let mut i = 1;
    while i < lines.len() - 1 {
        let header = lines[i].trim();
        i += 1;
        if header.starts_with("*** Environment ID:")
            || header.starts_with("*** Workdir:")
            || header.starts_with("*** Workdir ")
        {
            return Err(Error::new(
                ErrorCode::UnsupportedOperation,
                "embedded patch routing directives are unsupported",
            ));
        }
        if let Some(path) = header.strip_prefix("*** Add File: ") {
            let mut content = String::new();
            while i < lines.len() - 1 && !lines[i].starts_with("*** ") {
                let line = lines[i]
                    .strip_prefix('+')
                    .ok_or_else(|| Error::invalid(format!("invalid add line {}", i + 1)))?;
                content.push_str(line);
                content.push('\n');
                i += 1;
            }
            sections.push(Section::Add {
                path: PathBuf::from(path),
                content,
            });
        } else if let Some(path) = header.strip_prefix("*** Delete File: ") {
            sections.push(Section::Delete {
                path: PathBuf::from(path),
            });
        } else if let Some(path) = header.strip_prefix("*** Update File: ") {
            let mut move_to = None;
            if i < lines.len() - 1
                && let Some(to) = lines[i].trim().strip_prefix("*** Move to: ")
            {
                move_to = Some(PathBuf::from(to));
                i += 1;
            }
            let mut chunks = Vec::new();
            let mut chunk = Chunk::default();
            let mut active = false;
            while i < lines.len() - 1 {
                let line = lines[i];
                if line.starts_with("*** ") && line != "*** End of File" {
                    break;
                }
                if line == "@@" || line.starts_with("@@ ") {
                    if active && (!chunk.old.is_empty() || !chunk.new.is_empty()) {
                        chunks.push(chunk);
                        chunk = Chunk::default();
                    }
                    chunk.anchor = line.strip_prefix("@@ ").map(str::to_owned);
                    active = true;
                } else if line == "*** End of File" {
                    chunk.eof = true;
                } else if let Some(s) = line.strip_prefix('+') {
                    chunk.new.push(s.to_owned());
                    active = true;
                } else if let Some(s) = line.strip_prefix('-') {
                    chunk.old.push(s.to_owned());
                    active = true;
                } else if let Some(s) = line.strip_prefix(' ') {
                    chunk.old.push(s.to_owned());
                    chunk.new.push(s.to_owned());
                    active = true;
                } else {
                    return Err(Error::invalid(format!("invalid update line {}", i + 1)));
                }
                i += 1;
            }
            if active && (!chunk.old.is_empty() || !chunk.new.is_empty()) {
                chunks.push(chunk);
            }
            if chunks.is_empty() {
                return Err(Error::invalid("update section has no changes"));
            }
            sections.push(Section::Update {
                path: PathBuf::from(path),
                move_to,
                chunks,
            });
        } else {
            return Err(Error::invalid(format!(
                "unexpected patch directive at line {}",
                i
            )));
        }
    }
    Ok(sections)
}

// Historical Codex mode normalizes updated files to LF and terminates them.
fn apply_codex(text: &str, chunks: &[Chunk], work: &mut usize) -> Result<String> {
    let mut lines: Vec<String> = text.split('\n').map(str::to_owned).collect();
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    let mut replacements = Vec::new();
    let mut cursor = 0;
    let mut last_end = 0;
    for (n, chunk) in chunks.iter().enumerate() {
        if let Some(anchor) = &chunk.anchor {
            cursor = seek(&lines, std::slice::from_ref(anchor), cursor, false, work)?
                .ok_or_else(|| Error::invalid(format!("hunk {n}: context anchor not found")))?
                + 1;
        }
        if chunk.old.is_empty() {
            // Codex uses the anchor only to validate its presence. A pure
            // insertion still goes at EOF and does not advance the matcher.
            replacements.push((lines.len(), 0, chunk.new.clone()));
            continue;
        }
        let mut old = chunk.old.as_slice();
        let mut new = chunk.new.as_slice();
        let mut index = seek(&lines, old, cursor, chunk.eof, work)?;
        if index.is_none() && old.last().is_some_and(String::is_empty) {
            // Codex treats a trailing empty context line as an EOF newline
            // sentinel when the source line representation omits it.
            old = &old[..old.len() - 1];
            if new.last().is_some_and(String::is_empty) {
                new = &new[..new.len() - 1];
            }
            index = seek(&lines, old, cursor, chunk.eof, work)?;
        }
        let index =
            index.ok_or_else(|| Error::invalid(format!("hunk {n}: expected lines not found")))?;
        let end = index
            .checked_add(old.len())
            .ok_or_else(|| Error::invalid(format!("hunk {n}: invalid range")))?;
        if index < cursor || index < last_end || end > lines.len() {
            return Err(Error::invalid(format!(
                "hunk {n}: overlapping or out-of-order range"
            )));
        }
        cursor = end;
        last_end = end;
        replacements.push((index, old.len(), new.to_vec()));
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    for (start, count, new) in replacements.into_iter().rev() {
        lines.splice(start..start + count, new);
    }
    if lines.last().is_none_or(|s| !s.is_empty()) {
        lines.push(String::new());
    }
    Ok(lines.join("\n"))
}

fn seek(
    lines: &[String],
    pattern: &[String],
    start: usize,
    eof: bool,
    work: &mut usize,
) -> Result<Option<usize>> {
    if pattern.len() > lines.len() {
        return Ok(None);
    }
    let end = lines.len() - pattern.len();
    let begin = if eof { end } else { start };
    for pass in 0..4 {
        for i in begin..=end {
            *work += pattern.len();
            if *work > MAX_MATCH_WORK {
                return Err(Error::new(
                    ErrorCode::ResourceLimit,
                    "patch matching work limit exceeded",
                ));
            }
            let matches =
                lines[i..i + pattern.len()]
                    .iter()
                    .zip(pattern)
                    .all(|(a, b)| match pass {
                        0 => a == b,
                        1 => a.trim_end() == b.trim_end(),
                        2 => a.trim() == b.trim(),
                        _ => punctuation(a.trim()) == punctuation(b.trim()),
                    });
            if matches {
                return Ok(Some(i));
            }
        }
    }
    Ok(None)
}
fn punctuation(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{2018}'..='\u{201B}' => '\'',
            '\u{201C}'..='\u{201F}' => '"',
            '\u{00A0}' | '\u{2002}'..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}' => ' ',
            _ => c,
        })
        .collect()
}

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;

fn apply_replacements(
    text: &str,
    edits: &[TextEdit],
    work: &mut usize,
) -> std::result::Result<String, (Error, usize)> {
    let bom = text.starts_with('\u{feff}');
    let text = if bom {
        &text['\u{feff}'.len_utf8()..]
    } else {
        text
    };
    let crlf = text
        .find("\r\n")
        .is_some_and(|at| text.find('\n') == Some(at + 1));
    let base = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut ranges = Vec::new();
    for (i, edit) in edits.iter().enumerate() {
        let old = edit.old_text.replace("\r\n", "\n").replace('\r', "\n");
        if old.is_empty() {
            return Err((Error::invalid("oldText must not be empty"), i));
        }
        *work += base.len();
        if *work > MAX_MATCH_WORK {
            return Err((
                Error::new(
                    ErrorCode::ResourceLimit,
                    "replacement matching work limit exceeded",
                ),
                i,
            ));
        }
        let mut matches = base.match_indices(&old);
        let first = matches
            .next()
            .ok_or_else(|| (Error::invalid("oldText not found"), i))?
            .0;
        if matches.next().is_some() {
            return Err((Error::invalid("oldText is ambiguous"), i));
        }
        ranges.push((
            first,
            first + old.len(),
            edit.new_text.replace("\r\n", "\n").replace('\r', "\n"),
            i,
        ));
    }
    ranges.sort_by_key(|r| r.0);
    if let Some(pair) = ranges.windows(2).find(|p| p[0].1 > p[1].0) {
        return Err((Error::invalid("replacement ranges overlap"), pair[1].3));
    }
    let mut result = base.clone();
    for (start, end, new, _) in ranges.into_iter().rev() {
        result.replace_range(start..end, &new);
    }
    if result == base {
        return Err((Error::invalid("replacement produced no changes"), 0));
    }
    let result = if crlf {
        result.replace('\n', "\r\n")
    } else {
        result
    };
    Ok(if bom {
        format!("\u{feff}{result}")
    } else {
        result
    })
}
