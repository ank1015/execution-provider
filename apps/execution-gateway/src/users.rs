use crate::{
    ApiJson, AppState, bearer, config, crypto,
    db::{self, Page},
    error::{Error, Result},
};
use crate::{ApiPath as Path, ApiQuery as Query};
use axum::{
    Extension, Json,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction};
use subtle::ConstantTimeEq;
use uuid::Uuid;

#[derive(Clone, Copy)]
pub struct UserId(pub Uuid);

pub async fn admin_auth(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Result<Response> {
    let provided = crypto::hash(bearer(req.headers())?.as_bytes());
    let expected = crypto::hash(state.config.admin_key.as_bytes());
    if !bool::from(provided.as_bytes().ct_eq(expected.as_bytes())) {
        return Err(Error::unauthorized());
    }
    Ok(next.run(req).await)
}
pub async fn user_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response> {
    let hash = crypto::hash(bearer(req.headers())?.as_bytes());
    let id = sqlx::query_scalar::<_, Uuid>("SELECT k.user_id FROM user_api_keys k JOIN users u ON u.id=k.user_id WHERE k.key_hash=$1 AND k.revoked_at IS NULL AND u.enabled")
        .bind(hash).fetch_optional(&state.pool).await?.ok_or_else(Error::unauthorized)?;
    req.extensions_mut().insert(UserId(id));
    Ok(next.run(req).await)
}

#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub callback_url: String,
    pub webhook_payload_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
const USER_COLUMNS: &str =
    "id,name,enabled,callback_url,webhook_payload_version,created_at,updated_at";

#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Key {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: Option<String>,
    pub key_prefix: String,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}
const KEY_COLUMNS: &str = "id,user_id,name,key_prefix,created_at,revoked_at";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Create {
    name: String,
    callback_url: String,
    webhook_payload_version: Option<i32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Update {
    name: Option<String>,
    callback_url: Option<String>,
    enabled: Option<bool>,
    webhook_payload_version: Option<i32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateMe {
    name: Option<String>,
    callback_url: Option<String>,
    webhook_payload_version: Option<i32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueKey {
    name: Option<String>,
}

async fn read(state: &AppState, id: Uuid) -> Result<User> {
    sqlx::query_as(&format!("SELECT {USER_COLUMNS} FROM users WHERE id=$1"))
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(Error::missing)
}
async fn mint_key(
    tx: &mut Transaction<'_, Postgres>,
    user: Uuid,
    name: Option<String>,
) -> Result<Value> {
    let secret = crypto::token("egw_");
    let key: Key = sqlx::query_as(&format!("INSERT INTO user_api_keys(id,user_id,name,key_hash,key_prefix) VALUES($1,$2,$3,$4,$5) RETURNING {KEY_COLUMNS}"))
        .bind(Uuid::new_v4()).bind(user).bind(name).bind(crypto::hash(secret.as_bytes())).bind(&secret[..12]).fetch_one(&mut **tx).await?;
    let mut value = serde_json::to_value(key)?;
    value["secret"] = json!(secret);
    Ok(value)
}
pub async fn create(
    State(state): State<AppState>,
    ApiJson(input): ApiJson<Create>,
) -> Result<(StatusCode, Json<Value>)> {
    let name = config::name(input.name)?;
    let callback = config::callback_setting(&input.callback_url)?;
    let payload_version = webhook_payload_version(input.webhook_payload_version.unwrap_or(2))?;
    let id = Uuid::new_v4();
    let secret = crypto::token("whsec_");
    let encrypted = crypto::encrypt(&state.config.encryption_key, id, &secret)?;
    let mut tx = state.pool.begin().await?;
    let user: User = sqlx::query_as(&format!("INSERT INTO users(id,name,callback_url,webhook_secret_encrypted,webhook_payload_version) VALUES($1,$2,$3,$4,$5) RETURNING {USER_COLUMNS}"))
        .bind(id).bind(name).bind(callback).bind(encrypted).bind(payload_version).fetch_one(&mut *tx).await?;
    let key = mint_key(&mut tx, id, None).await?;
    tx.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"user": user, "key": key, "webhookSecret": secret})),
    ))
}
pub async fn list(State(state): State<AppState>, Query(page): Query<Page>) -> Result<Json<Value>> {
    Ok(Json(
        db::page::<User>(
            &state.pool,
            QueryBuilder::new(format!("SELECT {USER_COLUMNS} FROM users WHERE true")),
            page,
        )
        .await?,
    ))
}
pub async fn get(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<User>> {
    Ok(Json(read(&state, id).await?))
}
pub async fn me(
    State(state): State<AppState>,
    Extension(UserId(id)): Extension<UserId>,
) -> Result<Json<User>> {
    Ok(Json(read(&state, id).await?))
}

async fn patch_user(state: &AppState, id: Uuid, input: Update) -> Result<User> {
    if input.name.is_none()
        && input.callback_url.is_none()
        && input.enabled.is_none()
        && input.webhook_payload_version.is_none()
    {
        return Err(Error::invalid("patch must contain a setting"));
    }
    let name = input.name.map(config::name).transpose()?;
    let callback = input
        .callback_url
        .map(|v| config::callback_setting(&v))
        .transpose()?;
    let payload_version = input
        .webhook_payload_version
        .map(webhook_payload_version)
        .transpose()?;
    sqlx::query_as(&format!("UPDATE users SET name=COALESCE($2,name), callback_url=COALESCE($3,callback_url), enabled=COALESCE($4,enabled), webhook_payload_version=COALESCE($5,webhook_payload_version), updated_at=clock_timestamp() WHERE id=$1 RETURNING {USER_COLUMNS}"))
        .bind(id).bind(name).bind(callback).bind(input.enabled).bind(payload_version).fetch_optional(&state.pool).await?.ok_or_else(Error::missing)
}

fn webhook_payload_version(version: i32) -> Result<i32> {
    if matches!(version, 1 | 2) {
        Ok(version)
    } else {
        Err(Error::invalid("webhookPayloadVersion must be 1 or 2"))
    }
}
pub async fn update(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    ApiJson(input): ApiJson<Update>,
) -> Result<Json<User>> {
    Ok(Json(patch_user(&state, id, input).await?))
}
pub async fn update_me(
    State(state): State<AppState>,
    Extension(UserId(id)): Extension<UserId>,
    ApiJson(input): ApiJson<UpdateMe>,
) -> Result<Json<User>> {
    Ok(Json(
        patch_user(
            &state,
            id,
            Update {
                name: input.name,
                callback_url: input.callback_url,
                enabled: None,
                webhook_payload_version: input.webhook_payload_version,
            },
        )
        .await?,
    ))
}
pub async fn rotate_secret(
    State(state): State<AppState>,
    Extension(UserId(id)): Extension<UserId>,
) -> Result<Json<Value>> {
    let secret = crypto::token("whsec_");
    let encrypted = crypto::encrypt(&state.config.encryption_key, id, &secret)?;
    sqlx::query(
        "UPDATE users SET webhook_secret_encrypted=$2,updated_at=clock_timestamp() WHERE id=$1",
    )
    .bind(id)
    .bind(encrypted)
    .execute(&state.pool)
    .await?;
    Ok(Json(json!({"webhookSecret": secret})))
}
pub async fn issue_key(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    ApiJson(input): ApiJson<IssueKey>,
) -> Result<(StatusCode, Json<Value>)> {
    read(&state, id).await?;
    let name = input.name.map(config::name).transpose()?;
    let mut tx = state.pool.begin().await?;
    let value = mint_key(&mut tx, id, name).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(value)))
}
pub async fn list_keys(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(page): Query<Page>,
) -> Result<Json<Value>> {
    read(&state, id).await?;
    let mut query = QueryBuilder::new(format!(
        "SELECT {KEY_COLUMNS} FROM user_api_keys WHERE user_id="
    ));
    query.push_bind(id);
    Ok(Json(db::page::<Key>(&state.pool, query, page).await?))
}
pub async fn revoke_key(
    State(state): State<AppState>,
    Path((user, key)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode> {
    let result = sqlx::query("UPDATE user_api_keys SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE id=$1 AND user_id=$2")
        .bind(key).bind(user).execute(&state.pool).await?;
    if result.rows_affected() == 0 {
        return Err(Error::missing());
    }
    Ok(StatusCode::NO_CONTENT)
}
