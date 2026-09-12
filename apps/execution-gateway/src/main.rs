use clap::{Parser, Subcommand};
use execution_gateway::{AppState, config::Config, db, jobs, router, webhooks};
use sqlx::{Connection, Executor, PgConnection};
use std::{process::ExitCode, time::Duration};

#[derive(Parser)]
#[command(
    version,
    about = "Gateway for user-owned machines and durable execution jobs"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Serve,
    Migrate,
    Version,
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(message) = run(Cli::parse()).await {
        eprintln!("execution-gateway: {message}");
        return ExitCode::from(2);
    }
    ExitCode::SUCCESS
}
async fn run(cli: Cli) -> Result<(), &'static str> {
    if matches!(cli.command, Command::Version) {
        println!(
            "{}",
            process_execution_protocol::version_info(
                "execution-gateway",
                env!("CARGO_PKG_VERSION")
            )
        );
        return Ok(());
    }
    if matches!(cli.command, Command::Migrate) {
        let url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is required")?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await
            .map_err(|_| "database connection failed")?;
        db::MIGRATOR
            .run(&pool)
            .await
            .map_err(|_| "database migration failed")?;
        pool.close().await;
        println!("migrations applied");
        return Ok(());
    }
    let config = Config::from_env()?;
    let pool = db::pool(&config.database_url)
        .await
        .map_err(|_| "database connection failed")?;
    let mut ownership = PgConnection::connect(&config.database_url)
        .await
        .map_err(|_| "database ownership connection failed")?;
    ownership
        .execute("SET statement_timeout='5s'")
        .await
        .map_err(|_| "database ownership initialization failed")?;
    let owns: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(726381004219::bigint)")
        .fetch_one(&mut ownership)
        .await
        .map_err(|_| "database ownership lock failed")?;
    if !owns {
        return Err("another gateway is already serving this database");
    }
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|_| "cannot bind LISTEN_ADDR")?;
    let state = AppState::new(pool, config).map_err(|_| "HTTP client initialization failed")?;
    jobs::recover_inflight(&state)
        .await
        .map_err(|_| "job recovery failed; apply migrations before serving")?;
    eprintln!(
        "execution-gateway listening on {}",
        listener
            .local_addr()
            .map_err(|_| "cannot read listener address")?
    );
    let maintenance = tokio::spawn(jobs::housekeeping(state.clone()));
    let webhook = tokio::spawn(webhooks::run(state.clone()));
    let cancel = state.shutdown.clone();
    let server = axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(async move { cancel.cancelled().await });
    let monitor_state = state.clone();
    let monitor = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut healthy = true;
        loop {
            tokio::select! {
                _ = monitor_state.shutdown.cancelled() => break,
                _ = tick.tick() => {
                    if !matches!(tokio::time::timeout(Duration::from_secs(5), sqlx::query("SELECT 1").execute(&mut ownership)).await, Ok(Ok(_))) {
                        eprintln!("gateway database ownership connection lost");
                        healthy = false;
                        monitor_state.shutdown.cancel();
                        break;
                    }
                }
            }
        }
        (ownership, healthy)
    });
    let server_shutdown = state.shutdown.clone();
    let server = tokio::spawn(async move {
        let result = server.await;
        server_shutdown.cancel();
        result
    });
    tokio::select! {
        _ = shutdown_signal() => state.shutdown.cancel(),
        _ = state.shutdown.cancelled() => {},
    }
    let drained = tokio::time::timeout(Duration::from_secs(15), async {
        let server_ok = matches!(server.await, Ok(Ok(())));
        while !state.connections.is_empty().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let maintenance_ok = maintenance.await.is_ok();
        let webhook_ok = webhook.await.is_ok();
        server_ok && maintenance_ok && webhook_ok
    })
    .await;
    // Hold ownership until admission, sockets and workers have stopped.
    let (ownership, healthy) = monitor.await.map_err(|_| "ownership monitor failed")?;
    if drained.is_err() {
        return Err("shutdown deadline exceeded; pending jobs recover on restart");
    }
    state.pool.close().await;
    drop(ownership);
    if !healthy || !matches!(drained, Ok(true)) {
        return Err("gateway stopped after a service failure");
    }
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(windows)]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
