//! Initial gateway handshake contract. Registration remains gateway-specific.
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
    },
    Request {
        request: Box<Request>,
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
