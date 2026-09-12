use crate::error::{Error, Result};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub fn hash(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}
pub fn token(prefix: &str) -> String {
    let mut bytes = [0; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes))
}
pub fn encrypt(key: &[u8; 32], owner: Uuid, secret: &str) -> Result<Vec<u8>> {
    let mut nonce = [0; 12];
    OsRng.fill_bytes(&mut nonce);
    let aad = format!("user:{owner}:webhook");
    let encrypted = Aes256Gcm::new(key.into())
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: secret.as_bytes(),
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| Error::internal())?;
    Ok([&[1][..], &nonce, &encrypted].concat())
}
pub fn decrypt(key: &[u8; 32], owner: Uuid, bytes: &[u8]) -> Result<String> {
    if bytes.len() < 29 || bytes[0] != 1 {
        return Err(Error::internal());
    }
    let aad = format!("user:{owner}:webhook");
    let clear = Aes256Gcm::new(key.into())
        .decrypt(
            Nonce::from_slice(&bytes[1..13]),
            Payload {
                msg: &bytes[13..],
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| Error::internal())?;
    String::from_utf8(clear).map_err(|_| Error::internal())
}
