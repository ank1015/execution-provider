use super::{Control, Exit, Launch, Process, pty_command, pty_error};
use crate::{Error, ErrorCode, IoMode, OutputStream, Result};
use std::{
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    pin::Pin,
    process::Stdio,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf, unix::AsyncFd},
    process::Command,
};

struct UnixControl {
    pid: i32,
    terminal: Option<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
}

impl UnixControl {
    fn signal(&self, signal: i32) -> Result<()> {
        if unsafe { libc::kill(-self.pid, signal) } == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
        Ok(())
    }
}

impl Control for UnixControl {
    fn interrupt(&self) -> Result<()> {
        let foreground = self.terminal.as_ref().and_then(|terminal| {
            let terminal = terminal.lock().unwrap();
            let fd = terminal.as_raw_fd()?;
            let group = unsafe { libc::tcgetpgrp(fd) };
            (group > 0).then_some(group)
        });
        if let Some(group) = foreground {
            if unsafe { libc::kill(-group, libc::SIGINT) } == -1 {
                return Err(io::Error::last_os_error().into());
            }
            Ok(())
        } else {
            self.signal(libc::SIGINT)
        }
    }
    fn terminate(&self) -> Result<bool> {
        self.signal(libc::SIGTERM)?;
        Ok(true)
    }
    fn kill(&self) -> Result<()> {
        self.signal(libc::SIGKILL)
    }
    fn resize(&self, rows: u16, cols: u16) -> Result<()> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            Error::new(ErrorCode::UnsupportedOperation, "execution has no terminal")
        })?;
        terminal
            .lock()
            .unwrap()
            .resize(portable_pty::PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(pty_error)
    }
}

pub(super) fn spawn(launch: Launch) -> Result<Process> {
    match launch.io {
        IoMode::Pipes { stdin } => {
            let mut command = Command::new(&launch.executable);
            command
                .args(&launch.args)
                .current_dir(&launch.cwd)
                .env_clear()
                .envs(&launch.env)
                .process_group(0)
                .kill_on_drop(true)
                .stdin(if stdin { Stdio::piped() } else { Stdio::null() })
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut child = command.spawn()?;
            let control = Arc::new(UnixControl {
                pid: child.id().unwrap() as i32,
                terminal: None,
            });
            let writer = child.stdin.take().map(|w| Box::pin(w) as super::Writer);
            let readers = vec![
                (
                    OutputStream::Stdout,
                    Box::pin(child.stdout.take().unwrap()) as super::Reader,
                ),
                (
                    OutputStream::Stderr,
                    Box::pin(child.stderr.take().unwrap()) as super::Reader,
                ),
            ];
            let wait = Box::pin(async move {
                use std::os::unix::process::ExitStatusExt;
                let status = child.wait().await?;
                Ok(Exit {
                    code: status.code().map(|c| c as u32),
                    signal: status.signal().map(|s| s.to_string()),
                })
            });
            Ok(Process {
                control,
                writer,
                readers,
                wait,
            })
        }
        IoMode::Pty { rows, cols } => {
            let pair = portable_pty::native_pty_system()
                .openpty(portable_pty::PtySize {
                    rows,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .map_err(pty_error)?;
            let fd = pair
                .master
                .as_raw_fd()
                .ok_or_else(|| pty_error("PTY has no file descriptor"))?;
            let reader = TerminalIo::duplicate(fd)?;
            let writer = TerminalIo::duplicate(fd)?;
            let mut child = pair
                .slave
                .spawn_command(pty_command(&launch))
                .map_err(pty_error)?;
            drop(pair.slave);
            let control = Arc::new(UnixControl {
                pid: child.process_id().unwrap() as i32,
                terminal: Some(Mutex::new(pair.master)),
            });
            let wait = Box::pin(async move {
                let status = tokio::task::spawn_blocking(move || child.wait())
                    .await
                    .map_err(io::Error::other)??;
                Ok(Exit {
                    code: status.signal().is_none().then_some(status.exit_code()),
                    signal: status.signal().map(str::to_owned),
                })
            });
            Ok(Process {
                control,
                readers: vec![(OutputStream::Terminal, Box::pin(reader))],
                writer: Some(Box::pin(writer)),
                wait,
            })
        }
    }
}

/// Nonblocking PTY I/O allows an output-drain timeout to actually release the reader.
struct TerminalIo(AsyncFd<OwnedFd>);

impl TerminalIo {
    fn duplicate(fd: i32) -> io::Result<Self> {
        let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(io::Error::last_os_error());
        }
        let owned = unsafe { OwnedFd::from_raw_fd(duplicate) };
        let flags = unsafe { libc::fcntl(duplicate, libc::F_GETFL) };
        if flags < 0
            || unsafe { libc::fcntl(duplicate, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(AsyncFd::new(owned)?))
    }
}

impl AsyncRead for TerminalIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut ready = std::task::ready!(self.0.poll_read_ready(cx))?;
            let result = ready.try_io(|fd| {
                let dest = buffer.initialize_unfilled();
                let count = unsafe {
                    libc::read(
                        fd.get_ref().as_raw_fd(),
                        dest.as_mut_ptr().cast(),
                        dest.len(),
                    )
                };
                if count < 0 {
                    let error = io::Error::last_os_error();
                    // Linux reports PTY slave closure as EIO; macOS reports EOF.
                    if error.raw_os_error() == Some(libc::EIO) {
                        return Ok(0);
                    }
                    Err(error)
                } else {
                    Ok(count as usize)
                }
            });
            match result {
                Ok(Ok(count)) => {
                    buffer.advance(count);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Ok(Err(error)) => return Poll::Ready(Err(error)),
                Err(_) => continue,
            }
        }
    }
}

impl AsyncWrite for TerminalIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut ready = std::task::ready!(self.0.poll_write_ready(cx))?;
            match ready.try_io(|fd| {
                let count = unsafe {
                    libc::write(
                        fd.get_ref().as_raw_fd(),
                        buffer.as_ptr().cast(),
                        buffer.len(),
                    )
                };
                if count < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(count as usize)
                }
            }) {
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
