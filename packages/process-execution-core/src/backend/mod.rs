use crate::{IoMode, OutputStream, Result};
use std::{
    collections::BTreeMap, ffi::OsString, future::Future, path::PathBuf, pin::Pin, sync::Arc,
};
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub(crate) type Reader = Pin<Box<dyn AsyncRead + Send>>;
pub(crate) type Writer = Pin<Box<dyn AsyncWrite + Send>>;

pub(crate) struct Launch {
    pub executable: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub io: IoMode,
}

pub(crate) struct Exit {
    pub code: Option<u32>,
    pub signal: Option<String>,
}

pub(crate) trait Control: Send + Sync {
    fn interrupt(&self) -> Result<()>;
    /// False means this backend has no graceful termination mechanism.
    fn terminate(&self) -> Result<bool>;
    fn kill(&self) -> Result<()>;
    fn resize(&self, rows: u16, cols: u16) -> Result<()>;
}

pub(crate) struct Process {
    pub control: Arc<dyn Control>,
    pub readers: Vec<(OutputStream, Reader)>,
    pub writer: Option<Writer>,
    pub wait: Pin<Box<dyn Future<Output = std::io::Result<Exit>> + Send>>,
}

pub(crate) fn spawn(launch: Launch) -> Result<Process> {
    #[cfg(unix)]
    {
        unix::spawn(launch)
    }
    #[cfg(windows)]
    {
        windows::spawn(launch)
    }
}

#[cfg(unix)]
fn pty_command(launch: &Launch) -> portable_pty::CommandBuilder {
    let mut command = portable_pty::CommandBuilder::new(&launch.executable);
    command.args(&launch.args);
    command.cwd(&launch.cwd);
    command.env_clear();
    for (key, value) in &launch.env {
        command.env(key, value);
    }
    command
}

#[cfg(unix)]
fn pty_error(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::new(crate::ErrorCode::Io, error.to_string())
}
