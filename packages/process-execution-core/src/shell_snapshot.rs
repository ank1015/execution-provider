use crate::{Error, Result, Shell, ShellKind, ShellSnapshotConfig};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{io::AsyncReadExt, sync::OnceCell};

pub(crate) const MAX_SCOPE_ID_BYTES: usize = 256;
const SECTION_MARKER: &[u8] = b"\0PROCESS_EXECUTION_SNAPSHOT_ENV\0";
const ENV_CHUNK_BYTES: usize = 48 * 1024;
const ENV_PREFIX: &str = "__PROCESS_EXECUTION_SNAPSHOT_";
const COMMAND_ENV: &str = "__PROCESS_EXECUTION_SNAPSHOT_COMMAND";

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    scope_id: String,
    shell: PathBuf,
    kind: ShellKind,
    cwd: PathBuf,
}

struct CacheEntry {
    value: OnceCell<Arc<Snapshot>>,
    retry: Mutex<Retry>,
}

#[derive(Default)]
struct Retry {
    after: Option<Instant>,
    last_error: Option<String>,
}

struct Cached {
    entry: Arc<CacheEntry>,
    last_used: u64,
}

#[derive(Default)]
struct Cache {
    entries: HashMap<Key, Cached>,
    clock: u64,
}

pub(crate) struct ShellSnapshotCache {
    config: ShellSnapshotConfig,
    entries: Mutex<Cache>,
}

impl ShellSnapshotCache {
    pub(crate) fn new(config: ShellSnapshotConfig) -> Self {
        Self {
            config,
            entries: Mutex::new(Cache::default()),
        }
    }

    pub(crate) async fn get(
        &self,
        scope_id: &str,
        shell: &Shell,
        cwd: &Path,
        base_env: &BTreeMap<String, String>,
    ) -> std::result::Result<Arc<Snapshot>, String> {
        validate_scope_id(scope_id).map_err(|error| error.message)?;
        if !self.config.enabled {
            return Err("shell snapshots are disabled by runtime configuration".into());
        }
        if !supported(shell.kind) {
            return Err(format!(
                "shell snapshots are unsupported for {:?}",
                shell.kind
            ));
        }
        let key = Key {
            scope_id: scope_id.to_owned(),
            shell: shell.executable.clone(),
            kind: shell.kind,
            cwd: cwd.to_owned(),
        };
        let entry = self.entry(key);
        let config = self.config.clone();
        let shell = shell.clone();
        let cwd = cwd.to_owned();
        let env = base_env.clone();
        entry
            .value
            .get_or_try_init(|| async {
                {
                    let retry = entry.retry.lock().unwrap();
                    if let Some(after) = retry.after
                        && after > Instant::now()
                    {
                        return Err(retry
                            .last_error
                            .clone()
                            .unwrap_or_else(|| "shell snapshot capture is backing off".into()));
                    }
                }
                match capture(&config, &shell, &cwd, &env).await {
                    Ok(snapshot) => {
                        let mut retry = entry.retry.lock().unwrap();
                        *retry = Retry::default();
                        Ok(Arc::new(snapshot))
                    }
                    Err(error) => {
                        let mut retry = entry.retry.lock().unwrap();
                        retry.after = Some(Instant::now() + config.retry_backoff);
                        retry.last_error = Some(error.clone());
                        Err(error)
                    }
                }
            })
            .await
            .cloned()
    }

    fn entry(&self, key: Key) -> Arc<CacheEntry> {
        let mut cache = self.entries.lock().unwrap();
        cache.clock = cache.clock.wrapping_add(1);
        let now = cache.clock;
        if let Some(cached) = cache.entries.get_mut(&key) {
            cached.last_used = now;
            return cached.entry.clone();
        }
        if cache.entries.len() >= self.config.max_cached_scopes
            && let Some(oldest) = cache
                .entries
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(key, _)| key.clone())
        {
            cache.entries.remove(&oldest);
        }
        let entry = Arc::new(CacheEntry {
            value: OnceCell::new(),
            retry: Mutex::new(Retry::default()),
        });
        cache.entries.insert(
            key,
            Cached {
                entry: entry.clone(),
                last_used: now,
            },
        );
        entry
    }
}

pub(crate) fn validate_scope_id(scope_id: &str) -> Result<()> {
    if scope_id.is_empty() {
        return Err(Error::invalid("shell snapshot scope_id is empty"));
    }
    if scope_id.len() > MAX_SCOPE_ID_BYTES {
        return Err(Error::invalid("shell snapshot scope_id is too long"));
    }
    if scope_id.contains('\0') {
        return Err(Error::invalid("shell snapshot scope_id contains NUL"));
    }
    Ok(())
}

pub(crate) fn supported(kind: ShellKind) -> bool {
    cfg!(unix) && matches!(kind, ShellKind::Sh | ShellKind::Bash | ShellKind::Zsh)
}

/// Places state in bounded environment chunks and returns a wrapper that consumes it.
/// Snapshot contents never appear in the process argument list.
pub(crate) fn install_state(
    env: &mut BTreeMap<String, String>,
    state: &str,
    script: &str,
) -> String {
    env.retain(|key, _| !key.starts_with(ENV_PREFIX));
    env.insert(COMMAND_ENV.into(), script.into());
    let mut names = Vec::new();
    for (index, chunk) in utf8_chunks(state, ENV_CHUNK_BYTES).enumerate() {
        let name = format!("{ENV_PREFIX}{index}");
        env.insert(name.clone(), chunk.to_owned());
        names.push(name);
    }
    let expansion = names
        .iter()
        .map(|name| format!("${{{name}}}"))
        .collect::<String>();
    let unset = names.join(" ");
    format!(
        "__process_execution_snapshot_command=\"${{{COMMAND_ENV}}}\"\nunset {COMMAND_ENV}\n__process_execution_snapshot_state=\"{expansion}\"\nunset {unset}\neval \"$__process_execution_snapshot_state\"\nunset __process_execution_snapshot_state\neval \"$__process_execution_snapshot_command\""
    )
}

fn utf8_chunks(value: &str, max_bytes: usize) -> impl Iterator<Item = &str> {
    let mut remaining = value;
    std::iter::from_fn(move || {
        if remaining.is_empty() {
            return None;
        }
        let mut end = remaining.len().min(max_bytes);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        let (chunk, rest) = remaining.split_at(end);
        remaining = rest;
        Some(chunk)
    })
}

async fn capture(
    config: &ShellSnapshotConfig,
    shell: &Shell,
    cwd: &Path,
    env: &BTreeMap<String, String>,
) -> std::result::Result<Snapshot, String> {
    let script = capture_script(shell.kind)
        .ok_or_else(|| format!("unsupported snapshot shell {:?}", shell.kind))?;
    let mut command = tokio::process::Command::new(&shell.executable);
    command
        .arg("-lc")
        .arg(script)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to start shell snapshot capture: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "snapshot capture stdout was unavailable".to_owned())?;
    let max = config.max_capture_bytes;
    let collect = async move {
        let mut bytes = Vec::new();
        stdout
            .take((max + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| format!("failed reading shell snapshot: {error}"))?;
        let status = child
            .wait()
            .await
            .map_err(|error| format!("failed waiting for shell snapshot: {error}"))?;
        if !status.success() {
            return Err(format!("shell snapshot exited with {status}"));
        }
        if bytes.len() > max {
            return Err(format!("shell snapshot exceeded {max} bytes"));
        }
        Ok(bytes)
    };
    let bytes = tokio::time::timeout(config.capture_timeout, collect)
        .await
        .map_err(|_| "shell snapshot capture timed out".to_owned())??;
    parse(config, &bytes)
}

fn parse(config: &ShellSnapshotConfig, bytes: &[u8]) -> std::result::Result<Snapshot, String> {
    let marker = bytes
        .windows(SECTION_MARKER.len())
        .position(|part| part == SECTION_MARKER)
        .ok_or_else(|| "shell snapshot response was malformed".to_owned())?;
    let state = std::str::from_utf8(&bytes[..marker])
        .map_err(|_| "shell snapshot state was not UTF-8".to_owned())?;
    if state.len() > config.max_state_bytes {
        return Err(format!(
            "shell snapshot state exceeded {} bytes",
            config.max_state_bytes
        ));
    }
    let environment = &bytes[marker + SECTION_MARKER.len()..];
    let mut env = BTreeMap::new();
    for record in environment.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Some(separator) = record.iter().position(|byte| *byte == b'=') else {
            return Err("shell snapshot contained a malformed environment record".into());
        };
        let key = std::str::from_utf8(&record[..separator])
            .map_err(|_| "shell snapshot environment key was not UTF-8".to_owned())?;
        let value = std::str::from_utf8(&record[separator + 1..])
            .map_err(|_| format!("shell snapshot environment value for {key:?} was not UTF-8"))?;
        if !key.is_empty() && !matches!(key, "PWD" | "OLDPWD" | "SHLVL" | "_") {
            env.insert(key.to_owned(), value.to_owned());
        }
    }
    Ok(Snapshot {
        env,
        state: state.to_owned(),
    })
}

fn capture_script(kind: ShellKind) -> Option<&'static str> {
    match kind {
        ShellKind::Zsh => Some(
            r#"if [[ -n "${ZDOTDIR-}" ]]; then __pe_rc="$ZDOTDIR/.zshrc"; else __pe_rc="${HOME-}/.zshrc"; fi
if [[ -n "$__pe_rc" && -r "$__pe_rc" ]]; then . "$__pe_rc" >/dev/null 2>/dev/null; fi
unset __pe_rc
print -r -- '# process-execution shell snapshot'
print -r -- 'unalias -a 2>/dev/null || true'
functions
setopt | while IFS= read -r __pe_option; do print -r -- "setopt $__pe_option"; done
unset __pe_option
alias -L
printf '\0PROCESS_EXECUTION_SNAPSHOT_ENV\0'
command env -0"#,
        ),
        ShellKind::Bash => Some(
            r#"if [ -z "${BASH_ENV-}" ] && [ -n "${HOME-}" ] && [ -r "$HOME/.bashrc" ]; then . "$HOME/.bashrc" >/dev/null 2>/dev/null; fi
echo '# process-execution shell snapshot'
echo 'unalias -a 2>/dev/null || true'
shopt -p || true
set +o
echo 'shopt -s expand_aliases'
declare -f
alias -p
printf '\0PROCESS_EXECUTION_SNAPSHOT_ENV\0'
command env -0"#,
        ),
        ShellKind::Sh => Some(
            r#"if [ -n "${ENV-}" ]; then case "$ENV" in /*) __pe_rc="$ENV" ;; *) __pe_rc="./$ENV" ;; esac; if [ -r "$__pe_rc" ]; then . "$__pe_rc" >/dev/null 2>/dev/null; fi; unset __pe_rc; fi
echo '# process-execution shell snapshot'
echo 'unalias -a 2>/dev/null || true'
if [ -n "${BASH_VERSION-}" ]; then shopt -p || true; fi
set +o 2>/dev/null || true
if [ -n "${BASH_VERSION-}" ]; then echo 'shopt -s expand_aliases'; fi
if command -v typeset >/dev/null 2>&1; then typeset -f; elif command -v declare >/dev/null 2>&1; then declare -f; fi
alias 2>/dev/null || true
printf '\0PROCESS_EXECUTION_SNAPSHOT_ENV\0'
command env -0"#,
        ),
        ShellKind::PowerShell | ShellKind::Cmd => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_is_chunked_without_breaking_utf8() {
        let value = "a".repeat(ENV_CHUNK_BYTES - 1) + "🦀tail";
        let mut env = BTreeMap::new();
        let wrapper = install_state(&mut env, &value, "echo done");
        assert_eq!(env.len(), 3);
        assert!(wrapper.ends_with("eval \"$__process_execution_snapshot_command\""));
        assert_eq!(env.get(COMMAND_ENV).unwrap(), "echo done");
        assert_eq!(
            env.iter()
                .filter(|(key, _)| key.as_str() != COMMAND_ENV)
                .map(|(_, value)| value.as_str())
                .collect::<String>(),
            value
        );
    }
}
