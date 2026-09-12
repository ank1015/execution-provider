use super::{Exit, Launch, Process};
use crate::Result;
use std::{
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::{
        IO::CancelSynchronousIo,
        Threading::{GetExitCodeProcess, INFINITE, WaitForSingleObject},
    },
};

mod native;

pub(super) fn spawn(launch: Launch) -> Result<Process> {
    let native::NativeProcess {
        control,
        child,
        readers,
        writer,
    } = native::spawn(&launch)?;
    let readers = readers
        .into_iter()
        .map(|(stream, reader)| {
            (
                stream,
                Box::pin(Bridge::reader(Box::new(reader))) as super::Reader,
            )
        })
        .collect();
    let writer = writer.map(|writer| Box::pin(Bridge::writer(Box::new(writer))) as super::Writer);
    let wait = Box::pin(async move {
        tokio::task::spawn_blocking(move || {
            if unsafe { WaitForSingleObject(child.as_raw_handle(), INFINITE) } != WAIT_OBJECT_0 {
                return Err(io::Error::last_os_error());
            }
            let mut code = 0;
            if unsafe { GetExitCodeProcess(child.as_raw_handle(), &mut code) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Exit {
                code: Some(code),
                signal: None,
            })
        })
        .await
        .map_err(io::Error::other)?
    });
    Ok(Process {
        control,
        readers,
        writer,
        wait,
    })
}

/// Windows synchronous handles are bridged using bounded buffers. Input workers
/// are cancellable; output workers keep draining during ConPTY closure.
struct Bridge {
    stream: Option<tokio::io::DuplexStream>,
    worker: Option<OwnedHandle>,
    error: Arc<Mutex<Option<io::Error>>>,
    cancel_io: bool,
}

impl Bridge {
    fn new(
        cancel_io: bool,
        work: impl FnOnce(&mut tokio_util::io::SyncIoBridge<tokio::io::DuplexStream>) -> io::Result<()>
        + Send
        + 'static,
    ) -> Self {
        let (stream, peer) = tokio::io::duplex(16 * 1024);
        let error = Arc::new(Mutex::new(None));
        let result = error.clone();
        let mut peer = tokio_util::io::SyncIoBridge::new(peer);
        let worker = std::thread::spawn(move || {
            if let Err(error) = work(&mut peer) {
                *result.lock().unwrap() = Some(error);
            }
        });
        let worker = unsafe { OwnedHandle::from_raw_handle(worker.into_raw_handle()) };
        Self {
            stream: Some(stream),
            worker: Some(worker),
            error,
            cancel_io,
        }
    }

    fn reader(mut reader: Box<dyn io::Read + Send>) -> Self {
        Self::new(false, move |peer| {
            let mut buffer = [0u8; 16 * 1024];
            let mut forwarding = true;
            loop {
                let count = match reader.read(&mut buffer) {
                    Ok(count) => count,
                    Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return Ok(()),
                    Err(e) => return Err(e),
                };
                if count == 0 {
                    return Ok(());
                }
                if forwarding && std::io::Write::write_all(peer, &buffer[..count]).is_err() {
                    // The output-drain deadline ended. Discard remaining terminal
                    // output so ClosePseudoConsole can finish without a consumer.
                    forwarding = false;
                }
            }
        })
    }

    fn writer(mut writer: Box<dyn io::Write + Send>) -> Self {
        Self::new(true, move |peer| {
            let mut buffer = [0u8; 16 * 1024];
            loop {
                let count = std::io::Read::read(peer, &mut buffer)?;
                if count == 0 {
                    return Ok(());
                }
                writer.write_all(&buffer[..count])?;
                writer.flush()?;
            }
        })
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        drop(self.stream.take());
        if let Some(worker) = self.worker.take()
            && self.cancel_io
        {
            std::thread::spawn(move || {
                // Repeating handles the race between cancellation and entering ReadFile.
                while unsafe { WaitForSingleObject(worker.as_raw_handle(), 10) } == WAIT_TIMEOUT {
                    unsafe {
                        CancelSynchronousIo(worker.as_raw_handle());
                    }
                }
            });
        }
    }
}

impl AsyncRead for Bridge {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(self.stream.as_mut().unwrap()).poll_read(cx, buffer);
        if let Poll::Ready(Ok(())) = &result
            && buffer.filled().len() == before
            && let Some(error) = self.error.lock().unwrap().take()
        {
            return Poll::Ready(Err(error));
        }
        result
    }
}

impl AsyncWrite for Bridge {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(error) = self.error.lock().unwrap().take() {
            return Poll::Ready(Err(error));
        }
        Pin::new(self.stream.as_mut().unwrap()).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_shutdown(cx)
    }
}
