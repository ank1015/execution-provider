//! Machine registration and negotiated gateway transport contract.
use crate::{Request, Response, VERSION};
use process_execution_core::RuntimeInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const GATEWAY_PROTOCOL_VERSION: u32 = VERSION;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostMessage {
    Hello {
        protocol_version: u32,
        installation_id: Uuid,
        host_id: Uuid,
        runtime: RuntimeInfo,
        binary: Value,
        #[serde(default)]
        request_recovery: bool,
    },
    Accepted {
        request_id: String,
        generation_id: Uuid,
    },
    Missing {
        request_id: String,
        generation_id: Uuid,
    },
    Response {
        response: Box<Response>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayMessage {
    Welcome {
        protocol_version: u32,
        connection_id: Uuid,
        heartbeat_interval_ms: u64,
        #[serde(default)]
        request_recovery: bool,
    },
    Request {
        request: Box<Request>,
    },
    Recover {
        request_id: String,
        generation_id: Uuid,
    },
    Acknowledge {
        request_id: String,
        generation_id: Uuid,
    },
    Disconnect {
        code: DisconnectCode,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisconnectCode {
    Reconnect,
    Revoked,
    Replaced,
    IncompatibleProtocol,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistrationRequest {
    pub installation_id: Uuid,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegistrationResponse {
    pub machine_id: Uuid,
    pub credential: String,
}
