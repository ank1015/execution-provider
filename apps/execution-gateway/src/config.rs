use crate::error::{Error, Result};
use std::{collections::HashSet, net::SocketAddr};
use url::Url;

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub admin_key: String,
    pub encryption_key: [u8; 32],
    pub listen: SocketAddr,
    pub webhook_origins: HashSet<String>,
    pub retention_days: i32,
}

impl Config {
    pub fn from_env() -> std::result::Result<Self, &'static str> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is required")?;
        let url = Url::parse(&database_url).map_err(|_| "invalid DATABASE_URL")?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err("invalid DATABASE_URL");
        }
        let admin_key = std::env::var("ADMIN_API_KEY").map_err(|_| "ADMIN_API_KEY is required")?;
        if !(32..=512).contains(&admin_key.len())
            || admin_key.bytes().any(|b| !(33..=126).contains(&b))
        {
            return Err("invalid ADMIN_API_KEY");
        }
        let encryption_key =
            hex::decode(std::env::var("ENCRYPTION_KEY").map_err(|_| "ENCRYPTION_KEY is required")?)
                .ok()
                .and_then(|v| v.try_into().ok())
                .ok_or("ENCRYPTION_KEY must be 64 hexadecimal characters")?;
        let listen = std::env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3000".into())
            .parse()
            .map_err(|_| "invalid LISTEN_ADDR")?;
        let retention_days = std::env::var("REQUEST_RETENTION_DAYS")
            .unwrap_or_else(|_| "7".into())
            .parse()
            .map_err(|_| "invalid REQUEST_RETENTION_DAYS")?;
        if !(1..=365).contains(&retention_days) {
            return Err("invalid REQUEST_RETENTION_DAYS");
        }
        let mut webhook_origins = HashSet::new();
        for origin in std::env::var("WEBHOOK_ALLOWED_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let url = callback_url(origin).map_err(|_| "invalid WEBHOOK_ALLOWED_ORIGINS")?;
            if url.origin().ascii_serialization() != origin {
                return Err("WEBHOOK_ALLOWED_ORIGINS must contain canonical HTTPS origins");
            }
            webhook_origins.insert(origin.to_owned());
        }
        Ok(Self {
            database_url,
            admin_key,
            encryption_key,
            listen,
            webhook_origins,
            retention_days,
        })
    }
}

pub fn callback_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| Error::invalid("invalid callbackUrl"))?;
    if value.len() > 2048
        || url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::invalid(
            "callbackUrl requires HTTPS without credentials or fragment",
        ));
    }
    Ok(url)
}

pub fn name(value: String) -> Result<String> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.chars().count() > 200 || value.contains('\0') {
        return Err(Error::invalid("name must contain 1 to 200 characters"));
    }
    Ok(value)
}
