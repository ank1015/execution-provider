use crate::Result;
use std::{
    ffi::OsString,
    fs,
    path::PathBuf,
    process::{Command, ExitStatus},
};

pub struct Service {
    executable: PathBuf,
    state_dir: PathBuf,
    config: Option<PathBuf>,
}

impl Service {
    pub fn new(state_dir: PathBuf, config: Option<PathBuf>) -> Result<Self> {
        Ok(Self {
            executable: std::env::current_exe()?.canonicalize()?,
            state_dir,
            config,
        })
    }

    pub fn connect(&self) -> Result<()> {
        platform::connect(self)
    }

    pub fn disconnect(&self) -> Result<()> {
        platform::disconnect(self)
    }

    pub fn stop(&self) -> Result<()> {
        platform::stop(self)
    }

    pub fn restart(&self) -> Result<()> {
        platform::restart(self)
    }

    #[cfg(windows)]
    pub fn from_executable(executable: PathBuf, state_dir: PathBuf) -> Self {
        Self {
            executable,
            state_dir,
            config: None,
        }
    }

    fn arguments(&self) -> Vec<OsString> {
        let mut arguments = vec![
            OsString::from("--state-dir"),
            self.state_dir.as_os_str().to_owned(),
            OsString::from("run"),
        ];
        if let Some(config) = &self.config {
            arguments.push(OsString::from("--config"));
            arguments.push(config.as_os_str().to_owned());
        }
        arguments
    }
}

fn run(mut command: Command, description: &str) -> Result<()> {
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{description} failed with {status}").into())
    }
}

fn run_ignoring_failure(mut command: Command) -> Result<ExitStatus> {
    Ok(command.status()?)
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    const LABEL: &str = "dev.acentric.process-execution-host-daemon";

    pub fn connect(service: &Service) -> Result<()> {
        let path = plist_path()?;
        let parent = path.parent().ok_or("LaunchAgents path has no parent")?;
        fs::create_dir_all(parent)?;
        let log_dir = service.state_dir.join("logs");
        fs::create_dir_all(&log_dir)?;
        let content = plist(service, &log_dir);
        fs::write(&path, content)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;

        let domain = domain();
        let mut bootout = Command::new("launchctl");
        bootout.args(["bootout", &format!("{domain}/{LABEL}")]);
        let _ = run_ignoring_failure(bootout)?;
        let mut bootstrap = Command::new("launchctl");
        bootstrap.arg("bootstrap").arg(&domain).arg(&path);
        run(bootstrap, "launchctl bootstrap")
    }

    pub fn disconnect(_service: &Service) -> Result<()> {
        let mut command = Command::new("launchctl");
        command.args(["bootout", &format!("{}/{LABEL}", domain())]);
        let _ = run_ignoring_failure(command)?;
        Ok(())
    }

    pub fn stop(_service: &Service) -> Result<()> {
        let mut command = Command::new("launchctl");
        command.args(["kill", "SIGTERM", &format!("{}/{LABEL}", domain())]);
        run(command, "launchctl stop")
    }

    pub fn restart(_service: &Service) -> Result<()> {
        let mut command = Command::new("launchctl");
        command.args(["kickstart", &format!("{}/{LABEL}", domain())]);
        run(command, "launchctl restart")
    }

    fn domain() -> String {
        let output = Command::new("id")
            .arg("-u")
            .output()
            .expect("macOS provides id(1)");
        format!("gui/{}", String::from_utf8_lossy(&output.stdout).trim())
    }

    fn plist_path() -> Result<PathBuf> {
        Ok(directories::BaseDirs::new()
            .ok_or("cannot locate the user's home directory")?
            .home_dir()
            .join("Library/LaunchAgents")
            .join(format!("{LABEL}.plist")))
    }

    fn plist(service: &Service, log_dir: &std::path::Path) -> String {
        let mut arguments = vec![
            service
                .executable
                .as_os_str()
                .to_string_lossy()
                .into_owned(),
        ];
        arguments.extend(
            service
                .arguments()
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned()),
        );
        let arguments = arguments
            .iter()
            .map(|value| format!("    <string>{}</string>", xml(value)))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
{arguments}
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><false/>
  <key>StandardErrorPath</key><string>{}</string>
</dict>
</plist>
"#,
            xml(&log_dir.join("daemon.log").to_string_lossy())
        )
    }

    fn xml(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn plist_escapes_paths_and_preserves_arguments() {
            let service = Service {
                executable: PathBuf::from("/tmp/a & b/daemon"),
                state_dir: PathBuf::from("/tmp/state dir"),
                config: Some(PathBuf::from("/tmp/<host>.json")),
            };
            let value = plist(&service, std::path::Path::new("/tmp/logs"));
            assert!(value.contains("/tmp/a &amp; b/daemon"));
            assert!(value.contains("<string>--state-dir</string>"));
            assert!(value.contains("/tmp/&lt;host&gt;.json"));
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    const SERVICE_NAME: &str = "process-execution-host";

    pub fn connect(service: &Service) -> Result<()> {
        let path = unit_path()?;
        fs::create_dir_all(path.parent().ok_or("systemd user path has no parent")?)?;
        fs::write(&path, unit(service))?;
        systemctl(&["daemon-reload"], "systemctl daemon-reload")?;
        systemctl(
            &["enable", "--now", &format!("{SERVICE_NAME}.service")],
            "systemctl enable",
        )
    }

    pub fn disconnect(_service: &Service) -> Result<()> {
        let mut command = Command::new("systemctl");
        command.args([
            "--user",
            "disable",
            "--now",
            &format!("{SERVICE_NAME}.service"),
        ]);
        let _ = run_ignoring_failure(command)?;
        Ok(())
    }

    pub fn stop(_service: &Service) -> Result<()> {
        systemctl(
            &["stop", &format!("{SERVICE_NAME}.service")],
            "systemctl stop",
        )
    }

    pub fn restart(_service: &Service) -> Result<()> {
        systemctl(
            &["restart", &format!("{SERVICE_NAME}.service")],
            "systemctl restart",
        )
    }

    fn systemctl(arguments: &[&str], description: &str) -> Result<()> {
        let mut command = Command::new("systemctl");
        command.arg("--user").args(arguments);
        run(command, description)
    }

    fn unit_path() -> Result<PathBuf> {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| directories::BaseDirs::new().map(|base| base.home_dir().join(".config")))
            .ok_or("cannot locate the user's configuration directory")?;
        Ok(base
            .join("systemd/user")
            .join(format!("{SERVICE_NAME}.service")))
    }

    fn unit(service: &Service) -> String {
        let mut command = vec![systemd_quote(&service.executable)];
        command.extend(service.arguments().iter().map(|value| systemd_quote(value)));
        format!(
            "[Unit]\nDescription=Acentric process execution host\n\n[Service]\nExecStart={}\nRestart=on-failure\nRestartSec=5\nRestartPreventExitStatus=2\n\n[Install]\nWantedBy=default.target\n",
            command.join(" ")
        )
    }

    fn systemd_quote(value: impl AsRef<std::ffi::OsStr>) -> String {
        let value = value.as_ref().to_string_lossy();
        format!(
            "\"{}\"",
            value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('$', "$$")
                .replace('%', "%%")
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn unit_quotes_paths_and_has_safe_restart_policy() {
            let service = Service {
                executable: PathBuf::from("/tmp/a b/daemon"),
                state_dir: PathBuf::from("/tmp/state%dir"),
                config: None,
            };
            let value = unit(&service);
            assert!(value.contains("\"/tmp/a b/daemon\""));
            assert!(value.contains("\"/tmp/state%%dir\""));
            assert!(value.contains("RestartPreventExitStatus=2"));
        }
    }
}

#[cfg(windows)]
mod platform {
    use super::*;

    const TASK_NAME: &str = "Acentric Process Execution Host";

    pub fn connect(service: &Service) -> Result<()> {
        let task_command = windows_command(service);
        let mut create = Command::new("schtasks.exe");
        create.args([
            "/Create",
            "/TN",
            TASK_NAME,
            "/TR",
            &task_command,
            "/SC",
            "ONLOGON",
            "/RL",
            "LIMITED",
            "/F",
        ]);
        run(create, "Task Scheduler registration")?;
        let mut start = Command::new("schtasks.exe");
        start.args(["/Run", "/TN", TASK_NAME]);
        run(start, "Task Scheduler start")
    }

    pub fn disconnect(_service: &Service) -> Result<()> {
        let mut end = Command::new("schtasks.exe");
        end.args(["/End", "/TN", TASK_NAME]);
        let _ = run_ignoring_failure(end)?;
        let mut disable = Command::new("schtasks.exe");
        disable.args(["/Change", "/TN", TASK_NAME, "/Disable"]);
        let _ = run_ignoring_failure(disable)?;
        Ok(())
    }

    pub fn stop(_service: &Service) -> Result<()> {
        let mut command = Command::new("schtasks.exe");
        command.args(["/End", "/TN", TASK_NAME]);
        run(command, "Task Scheduler stop")
    }

    pub fn restart(_service: &Service) -> Result<()> {
        let mut command = Command::new("schtasks.exe");
        command.args(["/Run", "/TN", TASK_NAME]);
        run(command, "Task Scheduler restart")
    }

    fn windows_command(service: &Service) -> String {
        let mut command = vec![windows_quote(service.executable.as_os_str())];
        command.extend(service.arguments().iter().map(windows_quote));
        command.join(" ")
    }

    fn windows_quote(value: impl AsRef<std::ffi::OsStr>) -> String {
        let value = value.as_ref().to_string_lossy();
        let mut quoted = String::from("\"");
        let mut backslashes = 0;
        for character in value.chars() {
            match character {
                '\\' => backslashes += 1,
                '"' => {
                    quoted.push_str(&"\\".repeat(backslashes * 2 + 1));
                    quoted.push('"');
                    backslashes = 0;
                }
                character => {
                    quoted.push_str(&"\\".repeat(backslashes));
                    quoted.push(character);
                    backslashes = 0;
                }
            }
        }
        quoted.push_str(&"\\".repeat(backslashes * 2));
        quoted.push('"');
        quoted
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn task_command_quotes_every_argument() {
            let service = Service {
                executable: PathBuf::from(r"C:\Program Files\Acentric\daemon.exe"),
                state_dir: PathBuf::from(r"C:\Users\Test User\state"),
                config: None,
            };
            let value = windows_command(&service);
            assert!(value.starts_with(r#""C:\Program Files\Acentric\daemon.exe""#));
            assert!(value.contains(r#""C:\Users\Test User\state""#));
        }

        #[test]
        fn windows_quoting_handles_trailing_slashes_and_quotes() {
            assert_eq!(windows_quote(r#"C:\state\"#), r#""C:\state\\""#);
            assert_eq!(windows_quote(r#"a"b"#), r#""a\"b""#);
        }
    }
}
