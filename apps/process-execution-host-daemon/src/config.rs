use crate::Result;
use process_execution_protocol::runtime_config::RuntimeConfig;
use serde::Deserialize;
use std::{path::Path, time::Duration};
use url::Url;
use uuid::Uuid;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_REQUESTS: usize = 32;
pub const MAX_CONTROL_REQUESTS: usize = 4;
pub const DEFAULT_GATEWAY_URL: &str = "https://execution.acentric.dev";

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub gateway_url: Option<String>,
    pub allow_insecure_loopback: bool,
    pub execution: RuntimeConfig,
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(path) => Ok(serde_json::from_reader(std::fs::File::open(path)?)?),
            None => Ok(Self::default()),
        }
    }
}

pub fn gateway_url(value: &str, allow_insecure_loopback: bool) -> Result<Url> {
    let mut url = Url::parse(value)?;
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        None => false,
    };
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback && allow_insecure_loopback) {
        return Err("gateway requires HTTPS; HTTP is allowed only for explicitly enabled loopback development".into());
    }
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("gateway URL must have a host and no credentials, query, or fragment".into());
    }
    let path = format!("{}/", url.path().trim_end_matches('/'));
    url.set_path(&path);
    Ok(url)
}

pub fn websocket_url(gateway: &Url, host_id: Uuid) -> Url {
    let mut url = gateway
        .join(&format!("v1/machines/{host_id}/connect"))
        .expect("fixed relative URL");
    url.set_scheme(if gateway.scheme() == "https" {
        "wss"
    } else {
        "ws"
    })
    .expect("WebSocket scheme");
    url
}
