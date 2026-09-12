use crate::{
    framing,
    protocol::{self, Request, Response},
    transport,
};
use std::{
    ffi::OsStr,
    io::{self, BufRead, Write},
    time::Duration,
};
use tokio::io::BufReader;

pub fn stdin_request() -> crate::Result<Request> {
    let mut frame = Vec::new();
    io::Read::take(io::stdin().lock(), (framing::MAX_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut frame)?;
    framing::validate(&frame)?;
    Ok(serde_json::from_slice(&frame)?)
}

pub async fn exchange_and_print(
    endpoint: &OsStr,
    request: &Request,
    timeout: Duration,
) -> crate::Result<bool> {
    let response = tokio::time::timeout(timeout, async {
        let mut stream = transport::connect(endpoint).await?;
        framing::write(&mut stream, &serde_json::to_vec(request)?).await?;
        let frame = framing::read(&mut BufReader::new(stream)).await?;
        let response: Response = serde_json::from_slice(&frame)?;
        if response.protocol_version != protocol::VERSION
            || response.request_id.as_deref() != Some(&request.request_id)
        {
            return Err("response protocol version or request ID did not match".into());
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response)
    })
    .await
    .map_err(|_| "RPC exchange timed out; an accepted operation may still be running")??;
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &response)?;
    writeln!(stdout)?;
    Ok(response.is_ok())
}
