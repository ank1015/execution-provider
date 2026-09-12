use crate::{Command, Error, Result, Shell, ShellKind};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

pub(crate) fn resolve(shell: &Shell) -> Result<Shell> {
    let executable = which::which(&shell.executable)
        .map_err(|e| Error::invalid(format!("shell {:?} is unavailable: {e}", shell.executable)))?;
    Ok(Shell {
        executable,
        kind: shell.kind,
    })
}

fn kind(path: &Path) -> Option<ShellKind> {
    match path.file_stem()?.to_str()?.to_ascii_lowercase().as_str() {
        "sh" => Some(ShellKind::Sh),
        "bash" => Some(ShellKind::Bash),
        "zsh" => Some(ShellKind::Zsh),
        "pwsh" | "powershell" => Some(ShellKind::PowerShell),
        "cmd" => Some(ShellKind::Cmd),
        _ => None,
    }
}

pub(crate) fn discover() -> Result<Shell> {
    let mut candidates = Vec::new();
    #[cfg(unix)]
    if let Some(path) = user_shell() {
        candidates.push(path);
    }
    let names: &[&str] = if cfg!(windows) {
        &["pwsh", "powershell", "cmd.exe"]
    } else if cfg!(target_os = "macos") {
        &["zsh", "bash", "/bin/sh"]
    } else {
        &["bash", "zsh", "/bin/sh"]
    };
    candidates.extend(names.iter().map(PathBuf::from));
    for path in candidates {
        if let Some(kind) = kind(&path)
            && let Ok(shell) = resolve(&Shell {
                executable: path,
                kind,
            })
        {
            return Ok(shell);
        }
    }
    Err(Error::invalid(
        "no supported shell found; configure default_shell",
    ))
}

#[cfg(unix)]
fn user_shell() -> Option<PathBuf> {
    // getpwuid_r uses caller-owned storage, so concurrent runtime creation is safe.
    let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut buffer = vec![0u8; 1024];
    loop {
        let mut result = std::ptr::null_mut();
        let code = unsafe {
            libc::getpwuid_r(
                libc::getuid(),
                entry.as_mut_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &mut result,
            )
        };
        if code == libc::ERANGE && buffer.len() < 1024 * 1024 {
            buffer.resize(buffer.len() * 2, 0);
            continue;
        }
        if code != 0 || result.is_null() {
            return None;
        }
        let entry = unsafe { entry.assume_init() };
        if entry.pw_shell.is_null() {
            return None;
        }
        use std::os::unix::ffi::OsStrExt;
        let bytes = unsafe { std::ffi::CStr::from_ptr(entry.pw_shell) }.to_bytes();
        return Some(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)));
    }
}

pub(crate) fn prepare(
    command: &Command,
    default: &Shell,
) -> Result<(PathBuf, Vec<OsString>, Option<Shell>)> {
    match command {
        Command::Program { executable, args } => {
            if executable.as_os_str().is_empty() {
                return Err(Error::invalid("executable is empty"));
            }
            Ok((
                executable.clone(),
                args.iter().map(OsString::from).collect(),
                None,
            ))
        }
        Command::Shell {
            script,
            shell,
            login,
        } => {
            let selected = match shell {
                Some(s) => resolve(s)?,
                None => default.clone(),
            };
            let args = match selected.kind {
                ShellKind::Sh | ShellKind::Bash | ShellKind::Zsh => {
                    vec![if *login { "-lc" } else { "-c" }, script]
                }
                ShellKind::PowerShell if *login => vec!["-Command", script],
                ShellKind::PowerShell => vec!["-NoProfile", "-Command", script],
                ShellKind::Cmd => vec!["/d", "/s", "/c", script],
            };
            Ok((
                selected.executable.clone(),
                args.into_iter().map(OsString::from).collect(),
                Some(selected),
            ))
        }
    }
}
