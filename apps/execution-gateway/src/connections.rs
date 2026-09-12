use crate::ApiPath as Path;
use crate::{
    AppState, bearer, crypto,
    error::{Error, Result},
    jobs,
    machines::Machine,
};
use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::HeaderMap,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use process_execution_protocol::{
    self as protocol,
    gateway::{DisconnectCode, GatewayMessage, HostMessage},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{RwLock, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const HEARTBEAT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Live {
    id: Uuid,
    generation: Uuid,
    ready: Arc<AtomicBool>,
    revoke: CancellationToken,
}

struct Session {
    machine: Uuid,
    version: i32,
    recovery: bool,
    live: Live,
}

#[derive(Default)]
pub struct Connections {
    live: RwLock<HashMap<Uuid, Live>>,
}
impl Connections {
    pub async fn generation(&self, machine: Uuid) -> Option<Uuid> {
        self.live
            .read()
            .await
            .get(&machine)
            .filter(|v| v.ready.load(Ordering::Acquire) && !v.revoke.is_cancelled())
            .map(|v| v.generation)
    }
    pub async fn revoke(&self, machine: Uuid) {
        if let Some(live) = self.live.read().await.get(&machine) {
            live.revoke.cancel();
        }
    }
    pub async fn add_presence(&self, value: &mut Value) -> Result<()> {
        let id: Uuid = serde_json::from_value(value["id"].clone())?;
        let generation = self.generation(id).await;
        value["online"] = json!(generation.is_some());
        value["runtimeGenerationId"] = json!(generation);
        Ok(())
    }
    pub async fn describe(&self, machine: Machine) -> Result<Value> {
        let mut value = serde_json::to_value(machine)?;
        self.add_presence(&mut value).await?;
        Ok(value)
    }
    pub async fn is_empty(&self) -> bool {
        self.live.read().await.is_empty()
    }
}

pub async fn upgrade(
    State(state): State<AppState>,
    Path(machine): Path<Uuid>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response> {
    let hash = crypto::hash(bearer(&headers)?.as_bytes());
    // Temporary administrative disablement blocks dispatch, not device heartbeats.
    let (installation, version): (Uuid, i32) = sqlx::query_as("SELECT installation_id,credential_version FROM machines WHERE id=$1 AND credential_hash=$2 AND deleted_at IS NULL")
        .bind(machine).bind(hash).fetch_optional(&state.pool).await?.ok_or_else(Error::unauthorized)?;
    Ok(ws
        .max_message_size(protocol::MAX_FRAME_BYTES)
        .max_frame_size(protocol::MAX_FRAME_BYTES)
        .on_upgrade(move |socket| async move {
            if let Err(error) = session(state, machine, installation, version, socket).await {
                eprintln!("machine {machine}: {}", error.1);
            }
        }))
}

async fn session(
    state: AppState,
    machine: Uuid,
    installation: Uuid,
    version: i32,
    mut socket: WebSocket,
) -> Result<()> {
    let first = timeout(WRITE_TIMEOUT, socket.recv())
        .await
        .map_err(|_| Error::invalid("hello timeout"))?
        .ok_or_else(Error::unavailable)?
        .map_err(|_| Error::unavailable())?;
    let Message::Text(text) = first else {
        return Err(Error::invalid("expected hello"));
    };
    let HostMessage::Hello {
        protocol_version,
        installation_id,
        host_id,
        runtime,
        binary,
        request_recovery: recovery,
    } = serde_json::from_str(&text)?
    else {
        return Err(Error::invalid("expected hello"));
    };
    if protocol_version != protocol::VERSION {
        let message = encode(&GatewayMessage::Disconnect {
            code: DisconnectCode::IncompatibleProtocol,
        })?;
        let _ = timeout(WRITE_TIMEOUT, socket.send(message)).await;
        return Ok(());
    }
    if installation_id != installation || host_id != machine {
        return Err(Error::unauthorized());
    }
    let generation = runtime.generation_id;
    let id = Uuid::new_v4();
    let revoke = CancellationToken::new();
    let ready = Arc::new(AtomicBool::new(false));
    let connection = Live {
        id,
        generation,
        revoke: revoke.clone(),
        ready,
    };
    {
        let mut live = state.connections.live.write().await;
        if live.contains_key(&machine) {
            return Err(Error::conflict(
                "already_connected",
                "machine already has a connection",
            ));
        }
        live.insert(machine, connection.clone());
    }
    let mut pending = HashSet::new();
    let result = connected(
        &state,
        Session {
            machine,
            version,
            recovery,
            live: connection,
        },
        &mut pending,
        socket,
        serde_json::to_value(runtime)?,
        binary,
    )
    .await;
    // Reserve the slot until disconnect state is saved, before a new socket
    // can reconcile receipts from this runtime.
    if let Some(live) = state.connections.live.read().await.get(&machine) {
        live.ready.store(false, Ordering::Release);
    }
    if recovery && !revoke.is_cancelled() {
        if let Err(error) = jobs::disconnected(&state, machine, generation).await {
            eprintln!("machine {machine}: {}", error.1);
            state.shutdown.cancel();
        }
    } else {
        for job in pending {
            if let Err(error) = jobs::finish(&state, job, "unknown", json!({"code":"connection_lost","message":"operation response was lost; work was not replayed"})).await {
                eprintln!("job {job}: {}", error.1);
                state.shutdown.cancel();
            }
        }
    }
    let mut live = state.connections.live.write().await;
    if live.get(&machine).is_some_and(|v| v.id == id) {
        live.remove(&machine);
    }
    result
}

async fn connected(
    state: &AppState,
    session: Session,
    pending: &mut HashSet<Uuid>,
    socket: WebSocket,
    runtime: Value,
    binary: Value,
) -> Result<()> {
    let Session {
        machine,
        version,
        recovery,
        live:
            Live {
                generation,
                id,
                revoke,
                ready,
            },
    } = session;
    let updated = sqlx::query("UPDATE machines SET last_runtime_info=$3,last_binary_info=$4,last_seen_at=clock_timestamp() WHERE id=$1 AND credential_version=$2 AND deleted_at IS NULL")
        .bind(machine).bind(version).bind(runtime).bind(binary).execute(&state.pool).await?;
    if updated.rows_affected() != 1 {
        return Err(Error::unauthorized());
    }
    let mut recovering: VecDeque<_> = jobs::reconnect(state, machine, generation, recovery)
        .await?
        .into();
    pending.extend(recovering.iter().copied());
    let (mut sink, mut source) = socket.split();
    let (outgoing, mut messages) = mpsc::channel::<Message>(40);
    let mut writer = Writer(tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            timeout(WRITE_TIMEOUT, sink.send(message))
                .await
                .map_err(|_| Error::unavailable())?
                .map_err(|_| Error::unavailable())?;
        }
        Ok(())
    }));
    send(
        &outgoing,
        encode(&GatewayMessage::Welcome {
            protocol_version: protocol::VERSION,
            connection_id: id,
            heartbeat_interval_ms: HEARTBEAT.as_millis() as u64,
            request_recovery: recovery,
        })?,
    )?;
    ready.store(true, Ordering::Release);
    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    let mut dispatch = tokio::time::interval(Duration::from_millis(100));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    dispatch.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_seen = Instant::now();
    let mut last_contact = chrono::Utc::now();
    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                ready.store(false, Ordering::Release);
                let _ = send(&outgoing, Message::Close(None));
                drop(outgoing);
                let _ = timeout(WRITE_TIMEOUT, &mut writer.0).await;
                return Ok(());
            }
            _ = revoke.cancelled() => {
                ready.store(false, Ordering::Release);
                let _ = send(&outgoing, encode(&GatewayMessage::Disconnect { code: DisconnectCode::Revoked })?);
                let _ = send(&outgoing, Message::Close(None));
                drop(outgoing);
                let _ = timeout(WRITE_TIMEOUT, &mut writer.0).await;
                return Ok(());
            }
            result = &mut writer.0 => { return result.map_err(|_| Error::unavailable())?; }
            _ = heartbeat.tick() => {
                if last_seen.elapsed() >= HEARTBEAT * 3 { return Err(Error::unavailable()); }
                let result = sqlx::query("UPDATE machines SET last_seen_at=$3 WHERE id=$1 AND credential_version=$2 AND deleted_at IS NULL")
                    .bind(machine).bind(version).bind(last_contact).execute(&state.pool).await?;
                if result.rows_affected() != 1 { revoke.cancel(); continue; }
                send(&outgoing, Message::Ping(Vec::new().into()))?;
            }
            _ = dispatch.tick() => {
                if let Some(job) = recovering.pop_front() {
                    if pending.contains(&job) {
                        send(&outgoing, encode(&GatewayMessage::Recover { request_id: job.to_string(), generation_id: generation })?)?;
                    }
                } else if pending.len() < 32 && let Some((job, request)) = jobs::dispatch_next(state, machine, generation, version, recovery).await? {
                    pending.insert(job);
                    send(&outgoing, encode(&GatewayMessage::Request { request: Box::new(request) })?)?;
                }
            }
            message = source.next() => {
                let Some(Ok(message)) = message else { return Ok(()); };
                match message {
                    Message::Text(text) => {
                        match serde_json::from_str::<HostMessage>(&text)? {
                            HostMessage::Response { response } => {
                                let job = response.request_id.as_deref().and_then(|s| Uuid::parse_str(s).ok()).ok_or_else(|| Error::invalid("invalid response identity"))?;
                                if response.protocol_version != protocol::VERSION || response.generation_id != generation || (!recovery && !pending.contains(&job)) {
                                    return Err(Error::invalid("unmatched machine response"));
                                }
                                // Commit both the result and its outbox event before releasing the receipt.
                                jobs::response(state, machine, generation, job, *response).await?;
                                pending.remove(&job);
                                if recovery {
                                    send(&outgoing, encode(&GatewayMessage::Acknowledge { request_id: job.to_string(), generation_id: generation })?)?;
                                }
                            }
                            HostMessage::Accepted { request_id, generation_id } if recovery && generation_id == generation => {
                                let job = Uuid::parse_str(&request_id).map_err(|_| Error::invalid("invalid receipt identity"))?;
                                if pending.contains(&job) { jobs::accepted(state, machine, generation, job).await?; }
                            }
                            HostMessage::Missing { request_id, generation_id } if recovery && generation_id == generation => {
                                let job = Uuid::parse_str(&request_id).map_err(|_| Error::invalid("invalid receipt identity"))?;
                                if pending.remove(&job) {
                                    jobs::finish(state, job, "unknown", json!({"code":"receipt_missing","message":"daemon no longer has the request receipt; work was not replayed"})).await?;
                                }
                            }
                            _ => return Err(Error::invalid("unexpected machine message")),
                        }
                        last_seen = Instant::now();
                        last_contact = chrono::Utc::now();
                    }
                    Message::Ping(data) => { send(&outgoing, Message::Pong(data))?; last_seen = Instant::now(); last_contact = chrono::Utc::now(); }
                    Message::Pong(_) => { last_seen = Instant::now(); last_contact = chrono::Utc::now(); }
                    Message::Close(_) => return Ok(()),
                    Message::Binary(_) => return Err(Error::invalid("machine messages must be JSON text")),
                }
            }
        }
    }
}
fn send(sender: &mpsc::Sender<Message>, message: Message) -> Result<()> {
    sender.try_send(message).map_err(|_| Error::unavailable())
}
fn encode(message: &GatewayMessage) -> Result<Message> {
    let text = serde_json::to_string(message)?;
    if text.len() >= protocol::MAX_FRAME_BYTES {
        return Err(Error::invalid("message exceeds transport limit"));
    }
    Ok(Message::Text(text.into()))
}
struct Writer(JoinHandle<Result<()>>);
impl Drop for Writer {
    fn drop(&mut self) {
        self.0.abort();
    }
}
