mod config;
mod connection;
mod receipts;
mod registration;
mod service;
mod store;
mod update;
#[cfg(windows)]
mod windows;

use clap::{Parser, Subcommand};
use process_execution_core::ProcessExecutionCore;
use process_execution_protocol as protocol;
use std::{
    io::{self, BufRead, Read},
    path::PathBuf,
    process::ExitCode,
};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    version,
    about = "Connect this user's execution runtime to a registered gateway"
)]
struct Cli {
    /// Private application state directory. Defaults to the user's local application data.
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Exchange a single-use registration token from stdin for a machine credential.
    Register {
        #[arg(long, default_value = config::DEFAULT_GATEWAY_URL)]
        gateway_url: String,
        #[arg(long)]
        machine_id: Uuid,
        #[arg(long)]
        allow_insecure_loopback: bool,
    },
    /// Store a gateway-issued host credential read from one line of stdin.
    Configure {
        #[arg(long, default_value = config::DEFAULT_GATEWAY_URL)]
        gateway_url: String,
        #[arg(long = "machine-id", alias = "host-id")]
        host_id: Uuid,
        #[arg(long)]
        allow_insecure_loopback: bool,
    },
    /// Run the persistent outbound connection in the foreground.
    Run {
        #[arg(long)]
        gateway_url: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        allow_insecure_loopback: bool,
    },
    /// Install and start the daemon for this user's login session.
    Connect {
        /// Optional runtime configuration file to use on every start.
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Stop and disable the daemon without removing its registration.
    Disconnect,
    /// Install the latest checksum-verified daemon release and restart if connected.
    Update {
        #[arg(long, default_value = update::DEFAULT_MANIFEST_URL)]
        manifest_url: String,
    },
    /// Print local configuration and last connection status without credentials.
    Status,
    /// Print binary and protocol versions as JSON.
    Version,
    #[cfg(windows)]
    #[command(name = "__apply-update", hide = true)]
    ApplyUpdate {
        #[arg(long)]
        parent_pid: u32,
        #[arg(long)]
        target: PathBuf,
        #[arg(long)]
        replacement: PathBuf,
        #[arg(long)]
        restart: bool,
    },
}

fn version() -> serde_json::Value {
    protocol::version_info("process-execution-host-daemon", env!("CARGO_PKG_VERSION"))
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("process-execution-host-daemon: {error}");
            ExitCode::from(2)
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode> {
    if matches!(cli.command, Command::Version) {
        println!("{}", version());
        return Ok(ExitCode::SUCCESS);
    }
    let directory = state_directory(cli.state_dir)?;
    if matches!(cli.command, Command::Status) {
        println!("{}", store::inspect(&directory)?);
        return Ok(ExitCode::SUCCESS);
    }
    #[cfg(windows)]
    if let Command::ApplyUpdate {
        parent_pid,
        target,
        replacement,
        restart,
    } = &cli.command
    {
        update::apply_windows(*parent_pid, target, replacement, &directory, *restart)?;
        return Ok(ExitCode::SUCCESS);
    }
    match &cli.command {
        Command::Connect { config } => {
            if !store::inspect(&directory)?["configured"]
                .as_bool()
                .unwrap_or(false)
            {
                return Err("machine is not configured; run register first".into());
            }
            let config = config
                .as_ref()
                .map(|path| path.canonicalize())
                .transpose()?;
            service::Service::new(directory.clone(), config)?.connect()?;
            wait_until_running(&directory).await?;
            println!("{}", serde_json::json!({"connected": true}));
            return Ok(ExitCode::SUCCESS);
        }
        Command::Disconnect => {
            service::Service::new(directory.clone(), None)?.disconnect()?;
            println!("{}", serde_json::json!({"connected": false}));
            return Ok(ExitCode::SUCCESS);
        }
        Command::Update { manifest_url } => {
            println!("{}", update::apply(manifest_url, &directory).await?);
            return Ok(ExitCode::SUCCESS);
        }
        _ => {}
    }
    let store = store::Store::open(directory)?;
    match cli.command {
        Command::Register {
            gateway_url,
            machine_id,
            allow_insecure_loopback,
        } => {
            let gateway = config::gateway_url(&gateway_url, allow_insecure_loopback)?;
            let token = read_token()?;
            registration::register(&store, &gateway, machine_id, &token).await?;
            println!(
                "{}",
                serde_json::json!({"configured": true, "machineId": machine_id, "gatewayUrl": gateway.as_str()})
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Configure {
            gateway_url,
            host_id,
            allow_insecure_loopback,
        } => {
            let gateway = config::gateway_url(&gateway_url, allow_insecure_loopback)?;
            let token = read_token()?;
            store.configure(&store::Credential {
                gateway_url: gateway.to_string(),
                host_id,
                token,
            })?;
            println!(
                "{}",
                serde_json::json!({"configured": true, "host_id": host_id, "gateway_url": gateway.as_str()})
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Run {
            gateway_url,
            config: config_path,
            allow_insecure_loopback,
        } => {
            let config = config::Config::load(config_path.as_deref())?;
            let credential = store.credential()?;
            let allow_insecure = allow_insecure_loopback || config.allow_insecure_loopback;
            let registered = config::gateway_url(&credential.gateway_url, allow_insecure)?;
            let selected = gateway_url.or(config.gateway_url);
            let gateway = match selected {
                Some(value) if !value.is_empty() => config::gateway_url(&value, allow_insecure)?,
                _ => registered.clone(),
            };
            if gateway != registered {
                return Err("gateway URL differs from the registered gateway; configure a credential for that gateway first".into());
            }
            let core = ProcessExecutionCore::new(config.execution.into_core(None)?)?;
            let generation = core.runtime_info().generation_id;
            let mut runner = connection::Runner::new(core, version());
            let outcome = tokio::select! {
                result = runner.run(&store, &credential, &gateway) => result.map(|()| false),
                result = shutdown_signal() => result.map(|()| true).map_err(Into::into),
            };
            runner.shutdown().await?;
            if outcome? {
                store.status(
                    "stopped",
                    credential.host_id,
                    generation,
                    "local shutdown complete",
                )?;
                Ok(ExitCode::SUCCESS)
            } else {
                Ok(ExitCode::from(2))
            }
        }
        Command::Status
        | Command::Version
        | Command::Connect { .. }
        | Command::Disconnect
        | Command::Update { .. } => unreachable!(),
        #[cfg(windows)]
        Command::ApplyUpdate { .. } => unreachable!(),
    }
}

fn state_directory(path: Option<PathBuf>) -> Result<PathBuf> {
    Ok(match path {
        Some(path) => path,
        None => directories::BaseDirs::new()
            .ok_or("cannot locate the user's application data directory")?
            .data_local_dir()
            .join("process-execution-host-daemon"),
    })
}

async fn wait_until_running(directory: &std::path::Path) -> Result<()> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if store::inspect(directory)?["running"]
            .as_bool()
            .unwrap_or(false)
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("user service did not start within 15 seconds".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(windows)]
    {
        tokio::signal::ctrl_c().await
    }
}

fn read_token() -> Result<String> {
    let mut token = String::new();
    io::stdin().lock().take(8193).read_line(&mut token)?;
    let token = token.trim_end_matches(['\r', '\n']);
    store::validate_token(token)?;
    Ok(token.to_owned())
}
