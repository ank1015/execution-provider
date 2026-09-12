use crate::{
    ApiJson, AppState, bearer, config, crypto,
    db::{self, Page},
    error::{Error, Result},
    users::UserId,
};
use crate::{ApiPath as Path, ApiQuery as Query};
use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction};
use uuid::Uuid;

#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Machine {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub installation_id: Option<Uuid>,
    pub credential_version: i32,
    pub last_runtime_info: Option<Value>,
    pub last_binary_info: Option<Value>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
const COLUMNS: &str = "id,user_id,name,enabled,installation_id,credential_version,last_runtime_info,last_binary_info,last_seen_at,created_at,updated_at";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Update {
    name: Option<String>,
    enabled: Option<bool>,
}
use process_execution_protocol::gateway::{RegistrationRequest, RegistrationResponse};

async fn registration(tx: &mut Transaction<'_, Postgres>, machine: Uuid) -> Result<Value> {
    sqlx::query("UPDATE machine_registration_tokens SET revoked_at=clock_timestamp() WHERE machine_id=$1 AND consumed_at IS NULL AND revoked_at IS NULL")
        .bind(machine).execute(&mut **tx).await?;
    let token = crypto::token("egr_");
    let expires: DateTime<Utc> = sqlx::query_scalar("INSERT INTO machine_registration_tokens(id,machine_id,token_hash,expires_at) VALUES($1,$2,$3,now()+interval '15 minutes') RETURNING expires_at")
        .bind(Uuid::new_v4()).bind(machine).bind(crypto::hash(token.as_bytes())).fetch_one(&mut **tx).await?;
    Ok(json!({"machineId": machine, "registrationToken": token, "expiresAt": expires}))
}

pub async fn create(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    ApiJson(input): ApiJson<Create>,
) -> Result<(StatusCode, Json<Value>)> {
    let name = config::name(input.name)?;
    let id = Uuid::new_v4();
    let mut tx = state.pool.begin().await?;
    let machine: Machine = sqlx::query_as(&format!(
        "INSERT INTO machines(id,user_id,name) VALUES($1,$2,$3) RETURNING {COLUMNS}"
    ))
    .bind(id)
    .bind(user)
    .bind(name)
    .fetch_one(&mut *tx)
    .await?;
    let mut result = registration(&mut tx, id).await?;
    tx.commit().await?;
    result["machine"] = state.connections.describe(machine).await?;
    Ok((StatusCode::CREATED, Json(result)))
}
pub async fn list(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Query(page): Query<Page>,
) -> Result<Json<Value>> {
    let mut query = QueryBuilder::new(format!(
        "SELECT {COLUMNS} FROM machines WHERE deleted_at IS NULL AND user_id="
    ));
    query.push_bind(user);
    let mut result = db::page::<Machine>(&state.pool, query, page).await?;
    if let Some(items) = result["data"].as_array_mut() {
        for item in items {
            state.connections.add_presence(item).await?;
        }
    }
    Ok(Json(result))
}
pub async fn get(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>> {
    let machine: Machine = sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM machines WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL"
    ))
    .bind(id)
    .bind(user)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(Error::missing)?;
    Ok(Json(state.connections.describe(machine).await?))
}
pub async fn update(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
    ApiJson(input): ApiJson<Update>,
) -> Result<Json<Value>> {
    if input.name.is_none() && input.enabled.is_none() {
        return Err(Error::invalid("patch must contain a setting"));
    }
    let name = input.name.map(config::name).transpose()?;
    let machine: Machine = sqlx::query_as(&format!("UPDATE machines SET name=COALESCE($3,name),enabled=COALESCE($4,enabled),updated_at=clock_timestamp() WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL RETURNING {COLUMNS}"))
        .bind(id).bind(user).bind(name).bind(input.enabled).fetch_optional(&state.pool).await?.ok_or_else(Error::missing)?;
    Ok(Json(state.connections.describe(machine).await?))
}
pub async fn issue_registration(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Value>)> {
    let mut tx = state.pool.begin().await?;
    let enabled: bool = sqlx::query_scalar(
        "SELECT enabled FROM machines WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(id)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(Error::missing)?;
    if !enabled {
        return Err(Error::conflict(
            "machine_disabled",
            "enable the machine before registration",
        ));
    }
    let value = registration(&mut tx, id).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(value)))
}
pub async fn register(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    ApiJson(input): ApiJson<RegistrationRequest>,
) -> Result<Json<Value>> {
    let hash = crypto::hash(bearer(&headers)?.as_bytes());
    let mut tx = state.pool.begin().await?;
    let machine: Option<(Uuid, bool)> = sqlx::query_as(
        "SELECT user_id,enabled FROM machines WHERE id=$1 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let (user, enabled) = machine.ok_or_else(Error::unauthorized)?;
    let user_enabled: bool = sqlx::query_scalar("SELECT enabled FROM users WHERE id=$1 FOR SHARE")
        .bind(user)
        .fetch_one(&mut *tx)
        .await?;
    if !enabled || !user_enabled {
        return Err(Error::unauthorized());
    }
    let redeemed = sqlx::query("UPDATE machine_registration_tokens SET consumed_at=clock_timestamp() WHERE machine_id=$1 AND token_hash=$2 AND consumed_at IS NULL AND revoked_at IS NULL AND expires_at>clock_timestamp()")
        .bind(id).bind(hash).execute(&mut *tx).await?;
    if redeemed.rows_affected() != 1 {
        return Err(Error::unauthorized());
    }
    let credential = crypto::token("egm_");
    sqlx::query("UPDATE machines SET installation_id=$2,credential_hash=$3,credential_version=credential_version+1,updated_at=clock_timestamp(),last_runtime_info=NULL,last_binary_info=NULL,last_seen_at=NULL WHERE id=$1")
        .bind(id).bind(input.installation_id).bind(crypto::hash(credential.as_bytes())).execute(&mut *tx).await?;
    tx.commit().await?;
    state.connections.revoke(id).await;
    Ok(Json(serde_json::to_value(RegistrationResponse {
        machine_id: id,
        credential,
    })?))
}
pub async fn remove(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode> {
    let mut tx = state.pool.begin().await?;
    let deleted: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT deleted_at FROM machines WHERE id=$1 AND user_id=$2 FOR UPDATE")
            .bind(id)
            .bind(user)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(Error::missing)?;
    if deleted.is_none() {
        sqlx::query("UPDATE machines SET enabled=false,deleted_at=clock_timestamp(),updated_at=clock_timestamp(),credential_hash=NULL,credential_version=credential_version+1 WHERE id=$1").bind(id).execute(&mut *tx).await?;
        sqlx::query("UPDATE machine_registration_tokens SET revoked_at=clock_timestamp() WHERE machine_id=$1 AND consumed_at IS NULL AND revoked_at IS NULL").bind(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    state.connections.revoke(id).await;
    Ok(StatusCode::NO_CONTENT)
}
