use crate::{Result, service::Service};
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Cursor, Read, Write},
    path::Path,
    time::{Duration, Instant},
};
use url::Url;

pub const DEFAULT_MANIFEST_URL: &str = "https://downloads.acentric.dev/latest/manifest.json";
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Deserialize)]
struct Manifest {
    commit: String,
    artifacts: Vec<Artifact>,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Artifact {
    name: String,
    sha256: String,
    size_bytes: u64,
    url: String,
}

pub async fn apply(manifest_url: &str, state_dir: &Path) -> Result<serde_json::Value> {
    let manifest_url = secure_url(manifest_url)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()?;
    let manifest_response = client.get(manifest_url).send().await?;
    require_success(&manifest_response, "release manifest")?;
    let manifest_bytes = manifest_response.bytes().await?;
    if manifest_bytes.len() > MAX_MANIFEST_BYTES {
        return Err("release manifest is too large".into());
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    let artifact = select_artifact(&manifest.artifacts)?.clone();
    if artifact.size_bytes > MAX_ARTIFACT_BYTES {
        return Err("release artifact is too large".into());
    }
    validate_sha256(&artifact.sha256)?;
    let artifact_url = secure_url(&artifact.url)?;
    let response = client.get(artifact_url).send().await?;
    require_success(&response, "release artifact")?;
    if response
        .content_length()
        .is_some_and(|size| size != artifact.size_bytes)
    {
        return Err("release artifact Content-Length differs from the manifest".into());
    }
    let archive = response.bytes().await?;
    if archive.len() as u64 != artifact.size_bytes {
        return Err("release artifact size differs from the manifest".into());
    }
    let digest = format!("{:x}", Sha256::digest(&archive));
    if !digest.eq_ignore_ascii_case(&artifact.sha256) {
        return Err("release artifact checksum verification failed".into());
    }

    let current = std::env::current_exe()?.canonicalize()?;
    let parent = current
        .parent()
        .ok_or("installed executable has no parent directory")?;
    let temporary = tempfile::Builder::new()
        .prefix(".process-execution-update-")
        .tempdir_in(parent)?;
    let replacement = temporary.path().join(executable_name());
    extract_binary(&archive, &artifact.name, &replacement)?;

    let was_running = crate::store::inspect(state_dir)?["running"]
        .as_bool()
        .unwrap_or(false);
    let service = Service::new(state_dir.to_path_buf(), None)?;
    if was_running {
        service.stop()?;
        wait_until_stopped(state_dir).await?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o755))?;
        fs::rename(&replacement, &current)?;
        if was_running {
            service.restart()?;
        }
        Ok(serde_json::json!({
            "updated": true,
            "commit": manifest.commit,
            "artifact": artifact.name,
            "restarted": was_running
        }))
    }

    #[cfg(windows)]
    {
        let kept = temporary.keep();
        let replacement = kept.join(executable_name());
        let helper = kept.join("process-execution-update-helper.exe");
        fs::copy(&current, &helper)?;
        let mut command = std::process::Command::new(&helper);
        command
            .arg("--state-dir")
            .arg(state_dir)
            .arg("__apply-update")
            .arg("--parent-pid")
            .arg(std::process::id().to_string())
            .arg("--target")
            .arg(&current)
            .arg("--replacement")
            .arg(&replacement);
        if was_running {
            command.arg("--restart");
        }
        command.spawn()?;
        Ok(serde_json::json!({
            "updated": "scheduled",
            "commit": manifest.commit,
            "artifact": artifact.name,
            "restarted": was_running
        }))
    }
}

#[cfg(windows)]
pub fn apply_windows(
    parent_pid: u32,
    target: &Path,
    replacement: &Path,
    state_dir: &Path,
    restart: bool,
) -> Result<()> {
    crate::windows::wait_for_process(parent_pid)?;
    crate::windows::replace(replacement, target)?;
    if restart {
        Service::from_executable(target.to_path_buf(), state_dir.to_path_buf()).restart()?;
    }
    if let Some(directory) = replacement.parent() {
        crate::windows::delete_on_reboot(std::env::current_exe()?)?;
        let _ = fs::remove_dir(directory);
    }
    Ok(())
}

async fn wait_until_stopped(state_dir: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let running = crate::store::inspect(state_dir)?["running"]
            .as_bool()
            .unwrap_or(false);
        if !running {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("daemon did not stop within 15 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn secure_url(value: &str) -> Result<Url> {
    let url = Url::parse(value)?;
    if url.scheme() != "https" {
        return Err("update URLs must use HTTPS".into());
    }
    Ok(url)
}

fn require_success(response: &reqwest::Response, description: &str) -> Result<()> {
    if !response.status().is_success() {
        return Err(format!("{description} request returned HTTP {}", response.status()).into());
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("release manifest contains an invalid SHA-256 digest".into());
    }
    Ok(())
}

fn select_artifact(artifacts: &[Artifact]) -> Result<&Artifact> {
    let expected = expected_artifact()?;
    let matches = artifacts
        .iter()
        .filter(|artifact| artifact.name == expected)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [artifact] => Ok(artifact),
        [] => Err(format!("release manifest does not contain {expected}").into()),
        _ => Err(format!("release manifest contains duplicate {expected} entries").into()),
    }
}

fn expected_artifact() -> Result<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Ok("process-execution-host-daemon-linux-x86_64.tar.gz"),
        ("macos", "x86_64" | "aarch64") => {
            Ok("process-execution-host-daemon-macos-universal.tar.gz")
        }
        ("windows", "x86_64") => Ok("process-execution-host-daemon-windows-x86_64.zip"),
        (os, arch) => Err(format!("automatic updates are not published for {os}/{arch}").into()),
    }
}

fn executable_name() -> &'static str {
    if cfg!(windows) {
        "process-execution-host-daemon.exe"
    } else {
        "process-execution-host-daemon"
    }
}

fn extract_binary(archive: &[u8], artifact_name: &str, destination: &Path) -> Result<()> {
    let expected = executable_name();
    if artifact_name.ends_with(".zip") {
        let mut archive = zip::ZipArchive::new(Cursor::new(archive))?;
        let matches = (0..archive.len())
            .filter(|index| {
                archive
                    .by_index(*index)
                    .ok()
                    .and_then(|file| file.enclosed_name())
                    .and_then(|path| path.file_name().map(|name| name == expected))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err("release archive must contain exactly one daemon executable".into());
        }
        let mut source = archive.by_index(matches[0])?;
        write_replacement(&mut source, destination)?;
    } else {
        let decoder = GzDecoder::new(Cursor::new(archive));
        let mut archive = tar::Archive::new(decoder);
        let mut found = false;
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?;
            if path.file_name().is_some_and(|name| name == expected) {
                if found {
                    return Err("release archive contains duplicate daemon executables".into());
                }
                write_replacement(&mut entry, destination)?;
                found = true;
            }
        }
        if !found {
            return Err("release archive does not contain the daemon executable".into());
        }
    }
    Ok(())
}

fn write_replacement(source: &mut impl Read, destination: &Path) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(destination)?;
    std::io::copy(source, &mut file)?;
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};

    #[test]
    fn sha256_validation_is_strict() {
        assert!(validate_sha256(&"a".repeat(64)).is_ok());
        assert!(validate_sha256(&"g".repeat(64)).is_err());
        assert!(validate_sha256(&"a".repeat(63)).is_err());
    }

    #[test]
    fn artifact_selection_rejects_duplicates() {
        let name = expected_artifact().unwrap().to_string();
        let artifact = Artifact {
            name,
            sha256: "a".repeat(64),
            size_bytes: 1,
            url: "https://example.com/release".into(),
        };
        assert!(select_artifact(std::slice::from_ref(&artifact)).is_ok());
        assert!(select_artifact(&[artifact.clone(), artifact]).is_err());
    }

    #[test]
    fn insecure_update_url_is_rejected() {
        assert!(secure_url("http://downloads.example.com/manifest.json").is_err());
        assert!(secure_url(DEFAULT_MANIFEST_URL).is_ok());
    }

    #[test]
    fn tar_archive_extracts_only_the_daemon() {
        let mut archive = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut tar = tar::Builder::new(&mut archive);
            let contents = b"new daemon";
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            tar.append_data(&mut header, executable_name(), contents.as_slice())
                .unwrap();
            tar.finish().unwrap();
        }
        let archive = archive.finish().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join(executable_name());
        extract_binary(&archive, "release.tar.gz", &destination).unwrap();
        assert_eq!(fs::read(destination).unwrap(), b"new daemon");
    }

    #[test]
    fn zip_archive_extracts_only_the_daemon() {
        let mut archive = Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut archive);
            zip.start_file(executable_name(), zip::write::SimpleFileOptions::default())
                .unwrap();
            zip.write_all(b"new daemon").unwrap();
            zip.finish().unwrap();
        }
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join(executable_name());
        extract_binary(&archive.into_inner(), "release.zip", &destination).unwrap();
        assert_eq!(fs::read(destination).unwrap(), b"new daemon");
    }
}
