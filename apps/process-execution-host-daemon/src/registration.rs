use crate::{
    Result,
    config::CONNECT_TIMEOUT,
    store::{self, Credential, Store},
};
use process_execution_protocol::gateway::{RegistrationRequest, RegistrationResponse};
use url::Url;
use uuid::Uuid;

pub async fn register(store: &Store, gateway: &Url, machine: Uuid, token: &str) -> Result<()> {
    let endpoint = gateway.join(&format!("v1/machines/{machine}/register"))?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(CONNECT_TIMEOUT)
        .build()?;
    let mut response = client
        .post(endpoint)
        .bearer_auth(token)
        .json(&RegistrationRequest {
            installation_id: store.installation_id,
        })
        .send()
        .await
        .map_err(|_| "registration request failed; obtain a new token if it was consumed")?;
    if !response.status().is_success() {
        return Err(format!("registration returned HTTP {}", response.status().as_u16()).into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "registration response was interrupted")?
    {
        if body.len() + chunk.len() > 16 * 1024 {
            return Err("registration response exceeds 16 KiB".into());
        }
        body.extend_from_slice(&chunk);
    }
    let registered: RegistrationResponse =
        serde_json::from_slice(&body).map_err(|_| "invalid registration response")?;
    if registered.machine_id != machine {
        return Err("registration response belongs to a different machine".into());
    }
    store::validate_token(&registered.credential)?;
    store.configure(&Credential {
        gateway_url: gateway.to_string(),
        host_id: machine,
        token: registered.credential,
    })
}
