use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};
use tokio::net::{UnixListener, UnixStream};

pub struct Listener {
    listener: UnixListener,
    _endpoint: EndpointGuard,
}

struct EndpointGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
    _lock: File,
}

impl Listener {
    pub async fn bind(endpoint: &OsStr) -> io::Result<Self> {
        let path = Path::new(endpoint);
        let mut lock_path = endpoint.to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(lock_path)?;
        lock.try_lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("endpoint is already owned by a supervisor: {e}"),
            )
        })?;

        match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                if !metadata.file_type().is_socket() || metadata.uid() != unsafe { libc::geteuid() }
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "endpoint exists and is not an owned socket",
                    ));
                }
                match UnixStream::connect(path).await {
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            "endpoint is already serving connections",
                        ));
                    }
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                        std::fs::remove_file(path)?
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
        let listener = UnixListener::bind(path)?;
        let metadata = std::fs::symlink_metadata(path)?;
        let guard = EndpointGuard {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
            _lock: lock,
        };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        Ok(Self {
            listener,
            _endpoint: guard,
        })
    }

    pub async fn accept(&mut self) -> io::Result<super::Stream> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            if stream.peer_cred()?.uid() == unsafe { libc::geteuid() } {
                return Ok(Box::new(stream));
            }
        }
    }
}

impl Drop for EndpointGuard {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
        // Keep the lock file's inode stable across supervisors. Closing the file releases the lock.
    }
}

pub async fn connect(endpoint: &OsStr) -> io::Result<super::Stream> {
    Ok(Box::new(UnixStream::connect(Path::new(endpoint)).await?))
}
