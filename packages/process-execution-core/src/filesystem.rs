use crate::{Error, ErrorCode, ProcessExecutionCore, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePathRequest {
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadFileRequest {
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FilePrecondition {
    Missing,
    Sha256 { sha256: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteFileMode {
    Conditional(FilePrecondition),
    Overwrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteFileRequest {
    pub mutation_id: String,
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub data: Vec<u8>,
    pub create_parent_directories: bool,
    pub mode: WriteFileMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveFileRequest {
    pub mutation_id: String,
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub precondition: FilePrecondition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMetadata {
    pub is_file: bool,
    pub is_directory: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub modified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFileResult {
    pub path: PathBuf,
    pub metadata: FileMetadata,
    pub data: Vec<u8>,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationDisposition {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteFileReceipt {
    pub mutation_id: String,
    pub path: PathBuf,
    pub sha256: String,
    pub bytes_written: usize,
    pub disposition: MutationDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveFileReceipt {
    pub mutation_id: String,
    pub path: PathBuf,
    pub disposition: MutationDisposition,
}

#[derive(Clone, PartialEq, Eq)]
enum MutationFingerprint {
    Write {
        path: PathBuf,
        data_sha256: String,
        create_parent_directories: bool,
        mode: WriteFileMode,
    },
    Remove {
        path: PathBuf,
        precondition: FilePrecondition,
    },
}

#[derive(Clone)]
enum MutationOutcome {
    Write(Result<WriteFileReceipt>),
    Remove(Result<RemoveFileReceipt>),
}

#[derive(Default)]
pub(crate) struct FileMutationRegistry {
    entries: HashMap<String, (MutationFingerprint, MutationOutcome)>,
}

impl ProcessExecutionCore {
    pub async fn get_file_metadata(&self, request: FilePathRequest) -> Result<FileMetadata> {
        self.ensure_available()?;
        let path = self.resolve_file_path(request.cwd.as_deref(), &request.path)?;
        tokio::task::spawn_blocking(move || metadata(&path))
            .await
            .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?
    }

    pub async fn read_file(&self, request: ReadFileRequest) -> Result<ReadFileResult> {
        self.ensure_available()?;
        let limit = self.file_limit(request.max_bytes, self.0.config.limits.max_file_read_bytes)?;
        let path = self.resolve_file_path(request.cwd.as_deref(), &request.path)?;
        tokio::task::spawn_blocking(move || read_file_at(path, limit))
            .await
            .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?
    }

    pub async fn write_file(&self, request: WriteFileRequest) -> Result<WriteFileReceipt> {
        self.ensure_available()?;
        validate_mutation_id(&request.mutation_id)?;
        if request.data.len() > self.0.config.limits.max_file_write_bytes {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "file contents exceed the configured write limit",
            ));
        }
        if let WriteFileMode::Conditional(precondition) = &request.mode {
            validate_precondition(precondition)?;
        }
        let path = self.resolve_file_path(request.cwd.as_deref(), &request.path)?;
        let fingerprint = MutationFingerprint::Write {
            path: path.clone(),
            data_sha256: sha256(&request.data),
            create_parent_directories: request.create_parent_directories,
            mode: request.mode.clone(),
        };
        let mut registry = self.0.file_mutations.lock().await;
        if let Some((stored_fingerprint, outcome)) = registry.entries.get(&request.mutation_id) {
            if stored_fingerprint != &fingerprint {
                return Err(idempotency_conflict());
            }
            return match outcome {
                MutationOutcome::Write(result) => result.clone(),
                MutationOutcome::Remove(_) => Err(idempotency_conflict()),
            };
        }
        ensure_receipt_capacity(&registry, self.0.config.limits.max_file_mutation_receipts)?;
        let mutation_id = request.mutation_id.clone();
        let limit = self.0.config.limits.max_file_read_bytes;
        let outcome = tokio::task::spawn_blocking(move || {
            write_file_at(
                mutation_id,
                path,
                request.data,
                request.create_parent_directories,
                request.mode,
                limit,
            )
        })
        .await
        .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?;
        registry.entries.insert(
            request.mutation_id,
            (fingerprint, MutationOutcome::Write(outcome.clone())),
        );
        outcome
    }

    pub async fn remove_file(&self, request: RemoveFileRequest) -> Result<RemoveFileReceipt> {
        self.ensure_available()?;
        validate_mutation_id(&request.mutation_id)?;
        validate_precondition(&request.precondition)?;
        let path = self.resolve_file_path(request.cwd.as_deref(), &request.path)?;
        let fingerprint = MutationFingerprint::Remove {
            path: path.clone(),
            precondition: request.precondition.clone(),
        };
        let mut registry = self.0.file_mutations.lock().await;
        if let Some((stored_fingerprint, outcome)) = registry.entries.get(&request.mutation_id) {
            if stored_fingerprint != &fingerprint {
                return Err(idempotency_conflict());
            }
            return match outcome {
                MutationOutcome::Remove(result) => result.clone(),
                MutationOutcome::Write(_) => Err(idempotency_conflict()),
            };
        }
        ensure_receipt_capacity(&registry, self.0.config.limits.max_file_mutation_receipts)?;
        let mutation_id = request.mutation_id.clone();
        let limit = self.0.config.limits.max_file_read_bytes;
        let outcome = tokio::task::spawn_blocking(move || {
            remove_file_at(mutation_id, path, request.precondition, limit)
        })
        .await
        .map_err(|error| Error::new(ErrorCode::Io, error.to_string()))?;
        registry.entries.insert(
            request.mutation_id,
            (fingerprint, MutationOutcome::Remove(outcome.clone())),
        );
        outcome
    }

    fn ensure_available(&self) -> Result<()> {
        if self.0.shutdown.is_cancelled() {
            return Err(Error::new(
                ErrorCode::Unavailable,
                "runtime is shutting down",
            ));
        }
        Ok(())
    }

    fn resolve_file_path(&self, cwd: Option<&Path>, path: &Path) -> Result<PathBuf> {
        if path.as_os_str().is_empty() {
            return Err(Error::invalid("file path is empty"));
        }
        let cwd = match cwd {
            Some(path) if path.is_absolute() => path.to_path_buf(),
            Some(path) => self.0.config.cwd.join(path),
            None => self.0.config.cwd.clone(),
        };
        Ok(if path.is_absolute() {
            path.to_path_buf()
        } else {
            cwd.join(path)
        })
    }

    fn file_limit(&self, requested: Option<usize>, maximum: usize) -> Result<usize> {
        let limit = requested.unwrap_or(maximum);
        if limit == 0 || limit > maximum {
            return Err(Error::invalid(
                "file byte limit must be positive and within the configured maximum",
            ));
        }
        Ok(limit)
    }
}

fn write_file_at(
    mutation_id: String,
    path: PathBuf,
    data: Vec<u8>,
    create_parent_directories: bool,
    mode: WriteFileMode,
    read_limit: usize,
) -> Result<WriteFileReceipt> {
    let desired_sha256 = sha256(&data);
    let target = match &mode {
        WriteFileMode::Conditional(_) => path.clone(),
        WriteFileMode::Overwrite => follow_final_symlinks(&path)?,
    };
    let existing = match &mode {
        WriteFileMode::Conditional(_) => existing_hash(&target, read_limit)?,
        WriteFileMode::Overwrite => readable_existing_hash(&target, read_limit)?,
    };
    if existing.as_deref() == Some(&desired_sha256) {
        return Ok(WriteFileReceipt {
            mutation_id,
            path,
            sha256: desired_sha256,
            bytes_written: data.len(),
            disposition: MutationDisposition::AlreadyApplied,
        });
    }
    if let WriteFileMode::Conditional(precondition) = &mode {
        check_precondition(existing.as_deref(), precondition)?;
    }
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| Error::invalid("file path has no parent directory"))?;
    if create_parent_directories {
        fs::create_dir_all(parent).map_err(map_io)?;
    }
    let permissions = fs::metadata(&target)
        .ok()
        .map(|metadata| metadata.permissions());
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let temporary = parent.join(format!(
        ".{file_name}.process-execution-{}.tmp",
        Uuid::new_v4()
    ));
    let write_result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(map_io)?;
        file.write_all(&data).map_err(map_io)?;
        file.sync_all().map_err(map_io)?;
        if let Some(permissions) = permissions {
            fs::set_permissions(&temporary, permissions).map_err(map_io)?;
        }
        // Narrow the race with writers outside this runtime. Mutations within a
        // runtime are serialized by the receipt registry lock.
        if let WriteFileMode::Conditional(precondition) = &mode {
            check_precondition(existing_hash(&target, read_limit)?.as_deref(), precondition)?;
        }
        atomic_replace(&temporary, &target).map_err(map_io)?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result?;
    Ok(WriteFileReceipt {
        mutation_id,
        path,
        sha256: desired_sha256,
        bytes_written: data.len(),
        disposition: MutationDisposition::Applied,
    })
}

// Resolve the final component, including a dangling symlink. Parent components
// continue to use the operating system's normal path resolution.
fn follow_final_symlinks(path: &Path) -> Result<PathBuf> {
    let mut target = path.to_path_buf();
    for _ in 0..40 {
        let metadata = match fs::symlink_metadata(&target) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(target),
            Err(error) => return Err(map_io(error)),
        };
        if !metadata.file_type().is_symlink() {
            return if metadata.is_file() {
                Ok(target)
            } else {
                Err(Error::invalid("path is not a regular file"))
            };
        }
        let link = fs::read_link(&target).map_err(map_io)?;
        target = if link.is_absolute() {
            link
        } else {
            target
                .parent()
                .ok_or_else(|| Error::invalid("file path has no parent directory"))?
                .join(link)
        };
    }
    Err(Error::invalid("too many symbolic links"))
}

// Overwrite bounds the new bytes, not the previous file. A large existing file
// cannot be compared for the already-applied fast path, so it is replaced.
fn readable_existing_hash(path: &Path, limit: usize) -> Result<Option<String>> {
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_file() => Err(Error::invalid("path is not a regular file")),
        Ok(metadata) if metadata.len() > limit as u64 => Ok(None),
        Ok(_) => match read_file_at(path.to_path_buf(), limit) {
            Ok(file) => Ok(Some(file.sha256)),
            Err(error) if error.code == ErrorCode::ResourceLimit => Ok(None),
            Err(error) => Err(error),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(map_io(error)),
    }
}

fn remove_file_at(
    mutation_id: String,
    path: PathBuf,
    precondition: FilePrecondition,
    read_limit: usize,
) -> Result<RemoveFileReceipt> {
    let existing = existing_hash(&path, read_limit)?;
    let Some(existing) = existing else {
        return Ok(RemoveFileReceipt {
            mutation_id,
            path,
            disposition: MutationDisposition::AlreadyApplied,
        });
    };
    check_precondition(Some(&existing), &precondition)?;
    fs::remove_file(&path).map_err(map_io)?;
    Ok(RemoveFileReceipt {
        mutation_id,
        path,
        disposition: MutationDisposition::Applied,
    })
}

fn metadata(path: &Path) -> Result<FileMetadata> {
    let link = fs::symlink_metadata(path).map_err(map_io)?;
    let followed = fs::metadata(path).map_err(map_io)?;
    Ok(FileMetadata {
        is_file: followed.is_file(),
        is_directory: followed.is_dir(),
        is_symlink: link.file_type().is_symlink(),
        size: followed.len(),
        modified_at_ms: followed.modified().ok().and_then(system_time_ms),
    })
}

fn read_file_at(path: PathBuf, limit: usize) -> Result<ReadFileResult> {
    let metadata = metadata(&path)?;
    if !metadata.is_file {
        return Err(Error::invalid("path is not a regular file"));
    }
    if metadata.size > limit as u64 {
        return Err(Error::new(
            ErrorCode::ResourceLimit,
            "file exceeds the requested byte limit",
        ));
    }
    let mut file = fs::File::open(&path).map_err(map_io)?;
    let mut data = Vec::with_capacity(metadata.size as usize);
    std::io::Read::take(&mut file, limit as u64 + 1)
        .read_to_end(&mut data)
        .map_err(map_io)?;
    if data.len() > limit {
        return Err(Error::new(
            ErrorCode::ResourceLimit,
            "file grew beyond the requested byte limit while being read",
        ));
    }
    let sha256 = sha256(&data);
    Ok(ReadFileResult {
        path,
        metadata,
        data,
        sha256,
    })
}

fn existing_hash(path: &Path, limit: usize) -> Result<Option<String>> {
    match read_file_at(path.to_path_buf(), limit) {
        Ok(file) => Ok(Some(file.sha256)),
        Err(error) if error.code == ErrorCode::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn check_precondition(existing: Option<&str>, precondition: &FilePrecondition) -> Result<()> {
    let matches = match precondition {
        FilePrecondition::Missing => existing.is_none(),
        FilePrecondition::Sha256 { sha256 } => {
            existing.is_some_and(|existing| existing.eq_ignore_ascii_case(sha256.as_str()))
        }
    };
    if matches {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::PreconditionFailed,
            "file changed after it was read",
        ))
    }
}

fn validate_precondition(precondition: &FilePrecondition) -> Result<()> {
    if let FilePrecondition::Sha256 { sha256 } = precondition
        && (sha256.len() != 64
            || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            || sha256.bytes().any(|byte| byte.is_ascii_uppercase()))
    {
        return Err(Error::invalid(
            "sha256 precondition must contain 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_mutation_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 256 {
        return Err(Error::invalid("mutation_id must contain 1 to 256 bytes"));
    }
    Ok(())
}

fn ensure_receipt_capacity(registry: &FileMutationRegistry, maximum: usize) -> Result<()> {
    if registry.entries.len() >= maximum {
        return Err(Error::new(
            ErrorCode::ResourceLimit,
            "file mutation receipt limit reached",
        ));
    }
    Ok(())
}

fn idempotency_conflict() -> Error {
    Error::new(
        ErrorCode::IdempotencyConflict,
        "mutation_id was used for different arguments",
    )
}

fn sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn system_time_ms(time: SystemTime) -> Option<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).ok(),
        Err(error) => i64::try_from(error.duration().as_millis())
            .ok()
            .and_then(i64::checked_neg),
    }
}

fn map_io(error: std::io::Error) -> Error {
    let code = if error.kind() == std::io::ErrorKind::NotFound {
        ErrorCode::NotFound
    } else {
        ErrorCode::Io
    };
    Error::new(code, error.to_string())
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}
