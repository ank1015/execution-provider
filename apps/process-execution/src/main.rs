mod client;
mod config;
mod framing;
mod protocol;
mod server;
mod transport;

use clap::{Args, Parser, Subcommand};
use std::{ffi::OsString, path::PathBuf, process::ExitCode, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    version,
    about = "Supervise local processes through a private IPC endpoint"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the persistent local process supervisor in the foreground.
    Serve {
        #[command(flatten)]
        endpoint: Endpoint,
        /// JSON runtime configuration. Omitted fields use core defaults.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Override the configured default execution working directory.
        #[arg(long)]
        cwd: Option<PathBuf>,
    },
    /// Read one JSON request from stdin and print one JSON response.
    Rpc {
        #[command(flatten)]
        endpoint: Endpoint,
        /// Timeout for connecting and exchanging the request (not process lifetime).
        #[arg(long, default_value_t = 360_000)]
        timeout_ms: u64,
    },
    /// Query the supervisor's identity, version, and capabilities.
    Health {
        #[command(flatten)]
        endpoint: Endpoint,
        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,
    },
    /// Print machine-readable binary and protocol version information.
    Version,
}

#[derive(Args)]
struct Endpoint {
    /// Unix socket path, or Windows \\.\pipe\NAME. --socket is an alias.
    #[arg(long, alias = "socket")]
    endpoint: OsString,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("process-execution: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<bool> {
    match cli.command {
        Command::Serve {
            endpoint,
            config,
            cwd,
        } => {
            let config = config::load(config.as_deref(), cwd)?;
            server::serve(&endpoint.endpoint, config).await?;
            Ok(true)
        }
        Command::Rpc {
            endpoint,
            timeout_ms,
        } => {
            let request = client::stdin_request()?;
            client::exchange_and_print(
                &endpoint.endpoint,
                &request,
                Duration::from_millis(timeout_ms),
            )
            .await
        }
        Command::Health {
            endpoint,
            timeout_ms,
        } => {
            let request = protocol::Request {
                protocol_version: protocol::VERSION,
                request_id: uuid::Uuid::new_v4().to_string(),
                expected_generation_id: None,
                operation: protocol::Operation::Info,
            };
            client::exchange_and_print(
                &endpoint.endpoint,
                &request,
                Duration::from_millis(timeout_ms),
            )
            .await
        }
        Command::Version => {
            println!("{}", protocol::version_info());
            Ok(true)
        }
    }
}
