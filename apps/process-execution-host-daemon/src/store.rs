use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub gateway_url: String,
    pub host_id: Uuid,
    pub token: String,
}

pub struct Store {
    directory: PathBuf,
    _lock: File,
    pub installation_id: Uuid,
}

impl Store {
    pub fn open(directory: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&directory)?;
        private_directory(&directory)?;
        let lock = lock_file(&directory)?;
        lock.try_lock()
            .map_err(|_| "another daemon or configuration operation owns this state directory")?;
        let identity = directory.join("identity.json");
        let installation_id = match std::fs::read(&identity) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let id = Uuid::new_v4();
                write_json(&identity, &id)?;
                id
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            directory,
            _lock: lock,
            installation_id,
        })
    }

    pub fn credential(&self) -> Result<Credential> {
        let file = File::open(self.directory.join("credential.json"))
            .map_err(|_| "machine is not configured; run register first")?;
        let credential: Credential = serde_json::from_reader(file)?;
        validate_token(&credential.token)?;
        Ok(credential)
    }

    pub fn run_output_directory(&self) -> PathBuf {
        self.directory.join("run-output")
    }

    pub fn configure(&self, credential: &Credential) -> Result<()> {
        write_json(&self.directory.join("credential.json"), credential)
    }

    pub fn status(
        &self,
        state: &str,
        host_id: Uuid,
        generation_id: Uuid,
        detail: &str,
    ) -> Result<()> {
        write_json(
            &self.directory.join("status.json"),
            &json!({
                "state": state, "host_id": host_id, "installation_id": self.installation_id,
                "generation_id": generation_id, "pid": std::process::id(), "detail": detail,
                "updated_at": std::time::SystemTime::now()
            }),
        )
    }
}

pub fn validate_token(token: &str) -> Result<()> {
    if token.is_empty() || token.len() > 8192 || !token.bytes().all(|c| (33..=126).contains(&c)) {
        return Err("credential must be 1 to 8192 printable ASCII bytes with no whitespace".into());
    }
    Ok(())
}

pub fn inspect(directory: &Path) -> Result<Value> {
    if !directory.exists() {
        return Ok(json!({"configured": false, "running": false}));
    }
    let lock = lock_file(directory)?;
    let running = match lock.try_lock() {
        Ok(()) => false,
        Err(std::fs::TryLockError::WouldBlock) => true,
        Err(error) => return Err(error.into()),
    };
    let status = match std::fs::read(directory.join("status.json")) {
        Ok(bytes) => serde_json::from_slice::<Value>(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(error) => return Err(error.into()),
    };
    Ok(
        json!({"configured": directory.join("credential.json").is_file(), "running": running, "last_status": status}),
    )
}

fn lock_file(directory: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("daemon.lock"))?)
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        replace(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(unix)]
fn replace(from: &Path, to: &Path) -> Result<()> {
    Ok(std::fs::rename(from, to)?)
}

#[cfg(windows)]
use crate::windows::{private_directory, replace};
