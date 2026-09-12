use crate::{
    framing,
    protocol::{self, Dispatcher, Request, Response},
    transport,
};
use process_execution_core::{Config, ProcessExecutionCore};
use std::{
    ffi::OsStr,
    io::{self, Write},
    time::Duration,
};
use tokio::{io::BufReader, task::JoinSet};
use tokio_util::sync::CancellationToken;

const FRAME_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONNECTIONS: usize = 128;

pub async fn serve(endpoint: &OsStr, config: Config) -> crate::Result<()> {
    let mut listener = transport::Listener::bind(endpoint).await?;
    let core = ProcessExecutionCore::new(config)?;
    let dispatcher = Dispatcher::local(
        core.clone(),
        protocol::version_info("process-execution", env!("CARGO_PKG_VERSION")),
    );
    let stop = CancellationToken::new();
    let mut handlers = JoinSet::new();
    let _ = writeln!(
        io::stderr(),
        "process-execution serving {:?} (generation {})",
        endpoint,
        core.runtime_info().generation_id
    );

    let signal = shutdown_signal();
    tokio::pin!(signal);
    let result: crate::Result<()> = loop {
        tokio::select! {
            () = stop.cancelled() => break Ok(()),
            result = &mut signal => break result.map_err(Into::into),
            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) if handlers.len() < MAX_CONNECTIONS => {
                        handlers.spawn(handle(stream, dispatcher.clone(), stop.clone()));
                    }
                    Ok(_) => { /* The caller can retry once another connection finishes. */ }
                    Err(error) => break Err(error.into()),
                }
            }
            Some(result) = handlers.join_next() => {
                if let Err(error) = result {
                    let _ = writeln!(io::stderr(), "RPC handler failed: {error}");
                }
            }
        }
    };
    // Finish accepted work even if a client disappeared or the listener failed.
    core.shutdown().await?;
    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
    drop(listener);
    result
}

async fn handle(stream: transport::Stream, dispatcher: Dispatcher, stop: CancellationToken) {
    let mut stream = BufReader::new(stream);
    let generation = dispatcher.generation_id();
    let frame = match tokio::time::timeout(FRAME_TIMEOUT, framing::read(&mut stream)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(error)) => {
            let response =
                Response::new(None, generation, Err(protocol::invalid(error.to_string())));
            let _ = send(&mut stream, &response).await;
            return;
        }
        Err(_) => return,
    };
    let parsed = serde_json::from_slice::<serde_json::Value>(&frame);
    let request_id = parsed
        .as_ref()
        .ok()
        .and_then(|v| v.get("request_id")?.as_str())
        .map(str::to_owned);
    let request: Result<Request, _> = parsed.and_then(serde_json::from_value);
    let (response, wants_shutdown) = match request {
        Ok(request) => {
            let shutdown = request.requests_shutdown();
            (dispatcher.dispatch(request).await, shutdown)
        }
        Err(error) => (
            Response::new(
                request_id,
                generation,
                Err(protocol::invalid(error.to_string())),
            ),
            false,
        ),
    };
    let shutdown = wants_shutdown && response.is_ok();
    let _ = send(&mut stream, &response).await;
    if shutdown {
        stop.cancel();
    }
}

async fn send(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    response: &Response,
) -> crate::Result<()> {
    let frame = protocol::encode_response(response)?;
    tokio::time::timeout(FRAME_TIMEOUT, framing::write(stream, &frame)).await??;
    Ok(())
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
