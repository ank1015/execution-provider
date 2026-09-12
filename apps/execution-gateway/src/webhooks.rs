use crate::{ApiPath as Path, ApiQuery as Query};
use crate::{
    AppState, config, crypto,
    db::{self, Page},
    error::{Error, Result},
    users::UserId,
};
use axum::{Extension, Json, extract::State, http::StatusCode};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::{FromRow, QueryBuilder};
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    pub id: Uuid,
    pub job_id: Uuid,
    pub event_type: String,
    pub callback_url: String,
    pub status: String,
    pub next_attempt_at: DateTime<Utc>,
    pub retry_from_attempt: i32,
    pub retry_started_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}
const COLUMNS: &str = "id,job_id,event_type,callback_url,status,next_attempt_at,retry_from_attempt,retry_started_at,created_at,delivered_at";
#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Attempt {
    id: Uuid,
    delivery_id: Uuid,
    attempt_number: i32,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    http_status: Option<i32>,
    error: Option<Value>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Filter {
    job_id: Option<Uuid>,
    status: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttemptPage {
    attempt_limit: Option<i64>,
    attempt_cursor: Option<i32>,
}

pub async fn list(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Query(filter): Query<Filter>,
) -> Result<Json<Value>> {
    let mut query = QueryBuilder::new(format!(
        "SELECT {COLUMNS} FROM webhook_deliveries WHERE user_id="
    ));
    query.push_bind(user);
    if let Some(job) = filter.job_id {
        query.push(" AND job_id=").push_bind(job);
    }
    if let Some(status) = filter.status {
        if !["pending", "delivering", "retry_wait", "delivered", "failed"]
            .contains(&status.as_str())
        {
            return Err(Error::invalid("invalid delivery status"));
        }
        query.push(" AND status=").push_bind(status);
    }
    Ok(Json(
        db::page::<Delivery>(
            &state.pool,
            query,
            Page {
                limit: filter.limit,
                cursor: filter.cursor,
            },
        )
        .await?,
    ))
}
pub async fn get(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
    Query(page): Query<AttemptPage>,
) -> Result<Json<Value>> {
    let limit = page.attempt_limit.unwrap_or(50);
    if !(1..=100).contains(&limit) || page.attempt_cursor.is_some_and(|v| v < 1) {
        return Err(Error::invalid("invalid attempt pagination"));
    }
    let mut tx = state.pool.begin().await?;
    let delivery: Delivery = sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM webhook_deliveries WHERE id=$1 AND user_id=$2 FOR SHARE"
    ))
    .bind(id)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(Error::missing)?;
    let payload: Value = sqlx::query_scalar("SELECT payload FROM webhook_deliveries WHERE id=$1")
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let mut attempts: Vec<Attempt> = sqlx::query_as("SELECT id,delivery_id,attempt_number,started_at,finished_at,http_status,error FROM webhook_delivery_attempts WHERE delivery_id=$1 AND ($2::integer IS NULL OR attempt_number<$2) ORDER BY attempt_number DESC LIMIT $3")
        .bind(id).bind(page.attempt_cursor).bind(limit + 1).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    let more = attempts.len() > limit as usize;
    attempts.truncate(limit as usize);
    let next = if more {
        attempts.last().map(|v| v.attempt_number)
    } else {
        None
    };
    let mut value = serde_json::to_value(delivery)?;
    value["payload"] = payload;
    value["attempts"] = json!({"data": attempts, "nextCursor": next});
    Ok(Json(value))
}
pub async fn redeliver(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Delivery>)> {
    let mut tx = state.pool.begin().await?;
    let status: String = sqlx::query_scalar(
        "SELECT status FROM webhook_deliveries WHERE id=$1 AND user_id=$2 FOR UPDATE",
    )
    .bind(id)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(Error::missing)?;
    if !["delivered", "failed"].contains(&status.as_str()) {
        return Err(Error::conflict(
            "delivery_already_scheduled",
            "delivery already has an active retry cycle",
        ));
    }
    let delivery = sqlx::query_as(&format!("UPDATE webhook_deliveries SET status='pending',next_attempt_at=clock_timestamp(),retry_started_at=clock_timestamp(),retry_from_attempt=(SELECT COALESCE(max(attempt_number),0)+1 FROM webhook_delivery_attempts WHERE delivery_id=$1) WHERE id=$1 RETURNING {COLUMNS}"))
        .bind(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok((StatusCode::ACCEPTED, Json(delivery)))
}

#[derive(FromRow)]
struct Candidate {
    id: Uuid,
    user_id: Uuid,
    callback_url: String,
    payload: Value,
    retry_from_attempt: i32,
    retry_started_at: DateTime<Utc>,
    webhook_secret_encrypted: Vec<u8>,
}
struct Claim {
    row: Candidate,
    lease: Uuid,
    number: i32,
    cycle_number: i32,
}

async fn claim(state: &AppState) -> Result<Option<Claim>> {
    let mut tx = state.pool.begin().await?;
    let row: Option<Candidate> = sqlx::query_as("SELECT d.id,d.user_id,d.callback_url,d.payload,d.retry_from_attempt,d.retry_started_at,u.webhook_secret_encrypted FROM webhook_deliveries d JOIN users u ON u.id=d.user_id WHERE u.enabled AND ((d.status IN ('pending','retry_wait') AND d.next_attempt_at<=clock_timestamp()) OR (d.status='delivering' AND d.lease_expires_at<=clock_timestamp())) ORDER BY d.next_attempt_at,d.id LIMIT 1 FOR UPDATE OF d SKIP LOCKED")
        .fetch_optional(&mut *tx).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    sqlx::query("UPDATE webhook_delivery_attempts SET finished_at=clock_timestamp(),error=$2 WHERE delivery_id=$1 AND finished_at IS NULL")
        .bind(row.id).bind(json!({"code":"lease_expired","message":"previous delivery acknowledgement is unknown"})).execute(&mut *tx).await?;
    let number: i32 = sqlx::query_scalar("SELECT COALESCE(max(attempt_number),0)+1 FROM webhook_delivery_attempts WHERE delivery_id=$1").bind(row.id).fetch_one(&mut *tx).await?;
    let cycle_number = number - row.retry_from_attempt + 1;
    let expired: bool =
        sqlx::query_scalar("SELECT $1::timestamptz+interval '24 hours'<=clock_timestamp()")
            .bind(row.retry_started_at)
            .fetch_one(&mut *tx)
            .await?;
    if cycle_number > 8 || expired {
        sqlx::query("UPDATE webhook_deliveries SET status='failed',lease_token=NULL,lease_expires_at=NULL WHERE id=$1").bind(row.id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(None);
    }
    let lease = Uuid::new_v4();
    sqlx::query("UPDATE webhook_deliveries SET status='delivering',lease_token=$2,lease_expires_at=clock_timestamp()+interval '60 seconds' WHERE id=$1")
        .bind(row.id).bind(lease).execute(&mut *tx).await?;
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts(id,delivery_id,attempt_number) VALUES($1,$2,$3)",
    )
    .bind(Uuid::new_v4())
    .bind(row.id)
    .bind(number)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(Claim {
        row,
        lease,
        number,
        cycle_number,
    }))
}

pub fn signature(secret: &str, timestamp: &str, event: Uuid, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(format!("{timestamp}.{event}.").as_bytes());
    mac.update(body);
    format!("v1={}", hex::encode(mac.finalize().into_bytes()))
}

struct Outcome {
    http_status: Option<i32>,
    error: Option<Value>,
    retry: bool,
    retry_after: Option<DateTime<Utc>>,
}

async fn deliver(state: &AppState, claim: &Claim) -> Outcome {
    match prepare_and_send(state, claim).await {
        Ok(outcome) => outcome,
        Err(error) => Outcome {
            http_status: None,
            error: Some(error.value()),
            retry: false,
            retry_after: None,
        },
    }
}
async fn prepare_and_send(state: &AppState, claim: &Claim) -> Result<Outcome> {
    let url = config::callback_url(&claim.row.callback_url)?;
    if !state
        .config
        .webhook_origins
        .contains(&url.origin().ascii_serialization())
    {
        return Err(Error::conflict(
            "destination_not_allowed",
            "callback origin is not allowed",
        ));
    }
    let secret = crypto::decrypt(
        &state.config.encryption_key,
        claim.row.user_id,
        &claim.row.webhook_secret_encrypted,
    )?;
    let body = serde_json::to_vec(&claim.row.payload)?;
    let timestamp = Utc::now().timestamp().to_string();
    let signed = signature(&secret, &timestamp, claim.row.id, &body);
    let response = state
        .http
        .post(url)
        .header("Content-Type", "application/json")
        .header("X-Execution-Gateway-Event-Id", claim.row.id.to_string())
        .header("X-Execution-Gateway-Timestamp", timestamp)
        .header("X-Execution-Gateway-Signature", signed)
        .body(body)
        .send()
        .await;
    match response {
        Ok(response) => {
            let status = response.status().as_u16();
            let retry = matches!(status, 408 | 429 | 500 | 502 | 503 | 504);
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(retry_after);
            Ok(Outcome {
                http_status: Some(status.into()),
                error: if (200..300).contains(&status) {
                    None
                } else {
                    Some(
                        json!({"code":"callback_http_error","message":"callback returned an unsuccessful status"}),
                    )
                },
                retry,
                retry_after,
            })
        }
        Err(_) => Ok(Outcome {
            http_status: None,
            error: Some(
                json!({"code":"callback_network_error","message":"callback request did not complete"}),
            ),
            retry: true,
            retry_after: None,
        }),
    }
}
fn retry_after(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(seconds) = value.parse::<i64>() {
        if seconds < 0 {
            return None;
        }
        return chrono::Duration::try_seconds(seconds)
            .and_then(|d| Utc::now().checked_add_signed(d));
    }
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|v| v.with_timezone(&Utc))
}

async fn finalize(state: &AppState, claim: Claim, outcome: Outcome) -> Result<()> {
    let mut tx = state.pool.begin().await?;
    let current: Option<Uuid> = sqlx::query_scalar("SELECT id FROM webhook_deliveries WHERE id=$1 AND lease_token=$2 AND lease_expires_at>clock_timestamp() FOR UPDATE")
        .bind(claim.row.id).bind(claim.lease).fetch_optional(&mut *tx).await?;
    if current.is_none() {
        return Ok(());
    }
    sqlx::query("UPDATE webhook_delivery_attempts SET finished_at=clock_timestamp(),http_status=$3,error=$4 WHERE delivery_id=$1 AND attempt_number=$2")
        .bind(claim.row.id).bind(claim.number).bind(outcome.http_status).bind(&outcome.error).execute(&mut *tx).await?;
    let success = outcome.error.is_none();
    let backoff = (30_i64 * 2_i64.pow((claim.cycle_number - 1) as u32)).min(3600);
    let millis = (backoff as f64 * rand::thread_rng().gen_range(0.5..1.0) * 1000.0) as i64;
    let next = (Utc::now() + chrono::Duration::milliseconds(millis))
        .max(outcome.retry_after.unwrap_or(DateTime::<Utc>::MIN_UTC));
    let retry = !success
        && outcome.retry
        && claim.cycle_number < 8
        && next < claim.row.retry_started_at + chrono::Duration::hours(24);
    let status = if success {
        "delivered"
    } else if retry {
        "retry_wait"
    } else {
        "failed"
    };
    sqlx::query("UPDATE webhook_deliveries SET status=$3,next_attempt_at=$4,lease_token=NULL,lease_expires_at=NULL,delivered_at=CASE WHEN $3='delivered' THEN clock_timestamp() ELSE delivered_at END WHERE id=$1 AND lease_token=$2")
        .bind(claim.row.id).bind(claim.lease).bind(status).bind(next).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn run(state: AppState) {
    loop {
        let step = async {
            match claim(&state).await? {
                Some(claim) => {
                    let outcome = deliver(&state, &claim).await;
                    finalize(&state, claim, outcome).await?;
                }
                None => tokio::time::sleep(Duration::from_millis(250)).await,
            }
            Ok::<_, Error>(())
        };
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            result = step => if let Err(error) = result {
                eprintln!("webhook delivery: {}", error.1);
                tokio::select! { _ = state.shutdown.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        }
    }
}

/// One bounded delivery cycle, also used by database integration tests.
pub async fn run_once(state: &AppState) -> Result<bool> {
    let Some(claim) = claim(state).await? else {
        return Ok(false);
    };
    let outcome = deliver(state, &claim).await;
    finalize(state, claim, outcome).await?;
    Ok(true)
}
