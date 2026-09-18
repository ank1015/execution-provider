use crate::{
    Result,
    config::{self, CONNECT_TIMEOUT, MAX_CONTROL_REQUESTS, MAX_REQUESTS, WRITE_TIMEOUT},
    receipts::{Admission, Receipts},
    store::{Credential, Store},
};
use futures_util::{SinkExt, StreamExt};
use process_execution_core::ProcessExecutionCore;
use process_execution_protocol::{
    self as protocol, Dispatcher, Response,
    gateway::{DisconnectCode, GATEWAY_PROTOCOL_VERSION, GatewayMessage, HostMessage},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
    time::{MissedTickBehavior, timeout},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
use url::Url;
use uuid::Uuid;

enum End {
    Retry,
    Stop(&'static str),
}

/// Owns request tasks independently from any one network connection.
pub struct Runner {
    runtime: ProcessExecutionCore,
    dispatcher: Dispatcher,
    requests: JoinSet<()>,
    binary: Value,
    receipts: Arc<Mutex<Receipts>>,
}

impl Runner {
    pub fn new(runtime: ProcessExecutionCore, binary: Value) -> Self {
        Self {
            dispatcher: Dispatcher::gateway(runtime.clone(), binary.clone()),
            runtime,
            requests: JoinSet::new(),
            binary,
            receipts: Arc::new(Mutex::new(Receipts::default())),
        }
    }

    pub async fn run(
        &mut self,
        store: &Store,
        credential: &Credential,
        gateway: &Url,
    ) -> Result<()> {
        let mut delay = Duration::from_secs(1);
        loop {
            while self.requests.try_join_next().is_some() {}
            self.status(store, credential, "connecting", "connecting to gateway")?;
            let started = Instant::now();
            let detail = match self.connect(store, credential, gateway).await {
                Ok(End::Stop(reason)) => {
                    self.status(store, credential, "needs_configuration", reason)?;
                    return Ok(());
                }
                Ok(End::Retry) => "gateway connection closed".to_owned(),
                Err(error) => error.to_string(),
            };
            if started.elapsed() >= Duration::from_secs(30) {
                delay = Duration::from_secs(1);
            }
            self.status(store, credential, "reconnecting", &detail)?;
            let random = Uuid::new_v4().as_u128() as u16 as f64 / u16::MAX as f64;
            let pause = delay.mul_f64(0.75 + random * 0.25);
            tokio::time::sleep(pause).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }

    fn status(
        &self,
        store: &Store,
        credential: &Credential,
        state: &str,
        detail: &str,
    ) -> Result<()> {
        eprintln!("process-execution-host-daemon: {state}: {detail}");
        store.status(
            state,
            credential.host_id,
            self.dispatcher.generation_id(),
            detail,
        )
    }

    async fn connect(
        &mut self,
        store: &Store,
        credential: &Credential,
        gateway: &Url,
    ) -> Result<End> {
        let endpoint = config::websocket_url(gateway, credential.host_id);
        let mut request = endpoint.as_str().into_client_request()?;
        let mut authorization = format!("Bearer {}", credential.token)
            .parse::<tungstenite::http::HeaderValue>()
            .map_err(|_| "invalid credential format")?;
        authorization.set_sensitive(true);
        request
            .headers_mut()
            .insert(tungstenite::http::header::AUTHORIZATION, authorization);
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(protocol::MAX_FRAME_BYTES))
            .max_frame_size(Some(protocol::MAX_FRAME_BYTES))
            .max_write_buffer_size(protocol::MAX_FRAME_BYTES * 2);
        let connected = timeout(
            CONNECT_TIMEOUT,
            connect_async_with_config(request, Some(websocket_config), true),
        )
        .await
        .map_err(|_| "gateway connection timed out")?;
        let (mut socket, _) = match connected {
            Ok(value) => value,
            Err(tungstenite::Error::Http(response)) => {
                if matches!(response.status().as_u16(), 401 | 403) {
                    return Ok(End::Stop("gateway rejected host credential"));
                }
                return Err(
                    format!("gateway HTTP handshake returned {}", response.status()).into(),
                );
            }
            Err(error) => return Err(error.into()),
        };
        let hello = HostMessage::Hello {
            protocol_version: GATEWAY_PROTOCOL_VERSION,
            installation_id: store.installation_id,
            host_id: credential.host_id,
            runtime: self.runtime.runtime_info(),
            binary: self.binary.clone(),
            request_recovery: true,
        };
        timeout(
            WRITE_TIMEOUT,
            socket.send(Message::Text(serde_json::to_string(&hello)?.into())),
        )
        .await
        .map_err(|_| "gateway hello write timed out")??;
        let welcome = timeout(CONNECT_TIMEOUT, socket.next())
            .await
            .map_err(|_| "gateway welcome timed out")?
            .ok_or("gateway closed before welcome")??;
        let Message::Text(welcome) = welcome else {
            return Err("expected gateway welcome".into());
        };
        let (interval, recovery) = match serde_json::from_str::<GatewayMessage>(&welcome)
            .map_err(|_| "invalid gateway welcome")?
        {
            GatewayMessage::Welcome {
                protocol_version,
                heartbeat_interval_ms,
                request_recovery,
                ..
            } => {
                if protocol_version != GATEWAY_PROTOCOL_VERSION {
                    return Ok(End::Stop("gateway protocol version is incompatible"));
                }
                if !(50..=60_000).contains(&heartbeat_interval_ms) {
                    return Err("heartbeat interval must be between 50 and 60000 ms".into());
                }
                (
                    Duration::from_millis(heartbeat_interval_ms),
                    request_recovery,
                )
            }
            GatewayMessage::Disconnect { code } => return Ok(disconnect(code)),
            _ => return Err("expected gateway welcome".into()),
        };
        self.status(store, credential, "connected", "gateway session ready")?;
        let (mut sink, mut incoming) = socket.split();
        let (outgoing, mut messages) = mpsc::channel::<Message>(8);
        let mut writer = Writer(tokio::spawn(async move {
            while let Some(message) = messages.recv().await {
                timeout(WRITE_TIMEOUT, sink.send(message))
                    .await
                    .map_err(|_| "gateway write timed out")??;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }));
        let mut heartbeat = tokio::time::interval(interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut liveness = Liveness::new();
        let mut responses = tokio::time::interval(Duration::from_millis(100));
        responses.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut sent = HashSet::new();
        loop {
            tokio::select! {
                result = &mut writer.0 => { result??; return Ok(End::Retry); }
                _ = heartbeat.tick() => {
                    if liveness.expired(interval * 3) {
                        return Err("gateway heartbeat timed out".into());
                    }
                    outgoing.try_send(Message::Ping(Vec::new().into())).map_err(|_| "gateway output queue is full")?;
                }
                _ = responses.tick(), if recovery => {
                    let next = self.receipts.lock().unwrap().next_response(&sent);
                    if let Some((id, text)) = next {
                        outgoing.try_send(Message::Text(text.as_ref().into())).map_err(|_| "gateway output queue is full")?;
                        sent.insert(id);
                    }
                }
                Some(result) = self.requests.join_next() => { result?; }
                message = incoming.next() => {
                    let Some(message) = message else { return Ok(End::Retry); };
                    match message? {
                        Message::Text(text) => {
                            match serde_json::from_str::<GatewayMessage>(&text) {
                                Ok(GatewayMessage::Request { request }) => {
                                    liveness.touch();
                                    if recovery {
                                        self.accept(*request, &outgoing)?;
                                    } else if self.requests.len()
                                        >= MAX_REQUESTS
                                            + if request.terminates_run() {
                                                MAX_CONTROL_REQUESTS
                                            } else {
                                                0
                                            }
                                    {
                                        let response = Response::new(Some(request.request_id), self.dispatcher.generation_id(),
                                            Err(process_execution_core::Error { code: process_execution_core::ErrorCode::ResourceLimit, message: "host request capacity reached".into() }));
                                        outgoing.try_send(response_message(&response)?).map_err(|_| "gateway output queue is full")?;
                                    } else {
                                        let dispatcher = self.dispatcher.clone();
                                        let outgoing = outgoing.clone();
                                        self.requests.spawn(async move {
                                            let response = dispatcher.dispatch(*request).await;
                                            if let Ok(message) = response_message(&response) {
                                                let _ = timeout(WRITE_TIMEOUT, outgoing.send(message)).await;
                                            }
                                        });
                                    }
                                }
                                Ok(GatewayMessage::Recover { request_id, generation_id }) if recovery => {
                                    liveness.touch();
                                    let message = self.receipt_message(&request_id, generation_id)?;
                                    outgoing.try_send(message).map_err(|_| "gateway output queue is full")?;
                                    sent.remove(&request_id);
                                }
                                Ok(GatewayMessage::Acknowledge { request_id, generation_id }) if recovery => {
                                    liveness.touch();
                                    if generation_id == self.dispatcher.generation_id() {
                                        self.receipts.lock().unwrap().acknowledge(&request_id);
                                        sent.remove(&request_id);
                                    }
                                }
                                Ok(GatewayMessage::Recover { .. } | GatewayMessage::Acknowledge { .. }) => return Err("request recovery was not negotiated".into()),
                                Ok(GatewayMessage::Disconnect { code }) => return Ok(disconnect(code)),
                                Ok(GatewayMessage::Welcome { .. }) => return Err("duplicate gateway welcome".into()),
                                Err(_) => {
                                    let request_id = serde_json::from_str::<Value>(&text).ok()
                                        .and_then(|value| value.get("request")?.get("request_id")?.as_str().map(str::to_owned));
                                    let response = Response::new(request_id, self.dispatcher.generation_id(), Err(protocol::invalid("invalid gateway request")));
                                    outgoing.try_send(response_message(&response)?).map_err(|_| "gateway output queue is full")?;
                                }
                            }
                        }
                        Message::Ping(data) => {
                            liveness.touch();
                            outgoing.try_send(Message::Pong(data)).map_err(|_| "gateway output queue is full")?;
                        }
                        Message::Pong(_) => liveness.touch(),
                        Message::Close(_) => return Ok(End::Retry),
                        Message::Binary(_) | Message::Frame(_) => return Err("gateway must send JSON text messages".into()),
                    }
                }
            }
        }
    }

    fn accept(
        &mut self,
        request: protocol::Request,
        outgoing: &mpsc::Sender<Message>,
    ) -> Result<()> {
        let id = request.request_id.clone();
        if id.is_empty() || id.len() > 256 {
            return Err("invalid request identity".into());
        }
        let fingerprint =
            Sha256::digest(serde_json::to_vec(&serde_json::to_value(&request)?)?).into();
        let limit = MAX_REQUESTS
            + if request.terminates_run() {
                MAX_CONTROL_REQUESTS
            } else {
                0
            };
        let admission =
            self.receipts
                .lock()
                .unwrap()
                .accept(&id, fingerprint, self.requests.len() >= limit);
        let message = match admission {
            Admission::New => {
                let dispatcher = self.dispatcher.clone();
                let receipts = self.receipts.clone();
                let task_id = id.clone();
                // The task and its result belong to this runtime, not its socket.
                self.requests.spawn(async move {
                    let response = dispatcher.dispatch(request).await;
                    let text = response_text(&response).expect("protocol response is serializable");
                    receipts.lock().unwrap().complete(&task_id, text);
                });
                encode_host(&HostMessage::Accepted {
                    request_id: id,
                    generation_id: self.dispatcher.generation_id(),
                })?
            }
            Admission::Existing => self.receipt_message(&id, self.dispatcher.generation_id())?,
            Admission::Conflict => {
                return Err("request identity was reused with different input".into());
            }
            Admission::Full => response_message(&Response::new(
                Some(id),
                self.dispatcher.generation_id(),
                Err(process_execution_core::Error {
                    code: process_execution_core::ErrorCode::ResourceLimit,
                    message: "host request or receipt capacity reached".into(),
                }),
            ))?,
        };
        outgoing
            .try_send(message)
            .map_err(|_| "gateway output queue is full")?;
        Ok(())
    }

    fn receipt_message(&self, id: &str, generation: Uuid) -> Result<Message> {
        let receipts = self.receipts.lock().unwrap();
        if generation == self.dispatcher.generation_id() && receipts.contains(id) {
            if let Some(text) = receipts.response(id) {
                return Ok(Message::Text(text.as_ref().into()));
            }
            encode_host(&HostMessage::Accepted {
                request_id: id.to_owned(),
                generation_id: generation,
            })
        } else {
            encode_host(&HostMessage::Missing {
                request_id: id.to_owned(),
                generation_id: self.dispatcher.generation_id(),
            })
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.runtime.shutdown().await?;
        self.requests.abort_all();
        while self.requests.join_next().await.is_some() {}
        Ok(())
    }
}

struct Writer(JoinHandle<Result<()>>);
impl Drop for Writer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Liveness {
    monotonic: Instant,
    wall: SystemTime,
}

impl Liveness {
    fn new() -> Self {
        Self {
            monotonic: Instant::now(),
            wall: SystemTime::now(),
        }
    }
    fn touch(&mut self) {
        *self = Self::new();
    }
    fn expired(&self, deadline: Duration) -> bool {
        // Wall time includes suspension; monotonic time tolerates clock corrections.
        self.monotonic.elapsed() >= deadline || self.wall.elapsed().unwrap_or_default() >= deadline
    }
}

fn disconnect(code: DisconnectCode) -> End {
    match code {
        DisconnectCode::Reconnect => End::Retry,
        DisconnectCode::Revoked => End::Stop("host access was revoked"),
        DisconnectCode::Replaced => End::Stop("host connection was replaced"),
        DisconnectCode::IncompatibleProtocol => {
            End::Stop("gateway protocol version is incompatible")
        }
    }
}

fn encode_host(message: &HostMessage) -> Result<Message> {
    Ok(Message::Text(serde_json::to_string(message)?.into()))
}

fn response_message(response: &Response) -> Result<Message> {
    Ok(Message::Text(response_text(response)?.into()))
}

fn response_text(response: &Response) -> Result<String> {
    let frame = protocol::encode_response(response)?;
    let mut value: Value = serde_json::from_slice(&frame)?;
    let mut text =
        serde_json::to_string(&serde_json::json!({"type": "response", "response": value}))?;
    if text.len() >= protocol::MAX_FRAME_BYTES {
        value = serde_json::to_value(Response::new(
            response.request_id.clone(),
            response.generation_id,
            Err(process_execution_core::Error {
                code: process_execution_core::ErrorCode::ResourceLimit,
                message: "gateway response exceeds 8 MiB".into(),
            }),
        ))?;
        text = serde_json::to_string(&serde_json::json!({"type": "response", "response": value}))?;
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liveness_detects_suspend_and_tolerates_backwards_wall_clock_changes() {
        let deadline = Duration::from_secs(1);
        let suspended = Liveness {
            monotonic: Instant::now(),
            wall: SystemTime::now() - Duration::from_secs(10),
        };
        assert!(suspended.expired(deadline));
        let corrected = Liveness {
            monotonic: Instant::now() - Duration::from_secs(10),
            wall: SystemTime::now() + Duration::from_secs(100),
        };
        assert!(corrected.expired(deadline));
        let mut alive = suspended;
        alive.touch();
        assert!(!alive.expired(deadline));
    }
}
