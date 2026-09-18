use crate::{
    ApiJson, AppState, crypto,
    db::{self, Page},
    error::{Error, Result},
    users::UserId,
};
use crate::{ApiPath as Path, ApiQuery as Query};
use axum::{Extension, Json, extract::State, http::StatusCode};
use chrono::{DateTime, Utc};
use process_execution_protocol::{self as protocol, Operation, Payload, Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction};
use std::{collections::HashSet, time::Duration};
use uuid::Uuid;

#[derive(Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: Uuid,
    pub user_id: Uuid,
    pub machine_id: Uuid,
    pub idempotency_key: String,
    pub status: String,
    pub runtime_generation_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub dispatched_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
const COLUMNS: &str = "id,user_id,machine_id,idempotency_key,status,runtime_generation_id,created_at,dispatched_at,finished_at,updated_at";
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Submit {
    machine_id: Uuid,
    idempotency_key: String,
    request: Value,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Filter {
    machine_id: Option<Uuid>,
    status: Option<String>,
    idempotency_key: Option<String>,
    limit: Option<i64>,
    cursor: Option<String>,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Wait {
    timeout_ms: Option<u64>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TerminalJobEventV2 {
    schema_version: u8,
    event_id: Uuid,
    r#type: String,
    job_id: Uuid,
    machine_id: Uuid,
    completed_at: DateTime<Utc>,
}

const MAX_WAIT_MS: u64 = 5 * 60 * 1000;
impl Wait {
    fn timeout(&self) -> Result<Duration> {
        let timeout = self.timeout_ms.unwrap_or(MAX_WAIT_MS);
        if !(1..=MAX_WAIT_MS).contains(&timeout) {
            return Err(Error::invalid("timeoutMs must be between 1 and 300000"));
        }
        Ok(Duration::from_millis(timeout))
    }
}
fn terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "unknown")
}

pub fn normalize(value: Value) -> Result<Value> {
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(|| Error::invalid("request must be an object"))?;
    if object.keys().any(|k| {
        ![
            "operation",
            "params",
            "mode",
            "accepted_error_codes",
            "operations",
            "expected_generation_id",
        ]
        .contains(&k.as_str())
    }) {
        return Err(Error::invalid("unexpected request fields"));
    }
    object.insert("protocol_version".into(), json!(protocol::VERSION));
    object.insert("request_id".into(), json!(Uuid::nil().to_string()));
    let request: Request = serde_json::from_value(Value::Object(object))?;
    match &request.payload {
        Payload::Single(operation) => validate_operation(operation)?,
        Payload::Batch(batch) => {
            if batch.operations.is_empty()
                || batch.operations.len() > protocol::MAX_BATCH_OPERATIONS
            {
                return Err(Error::invalid("batches require 1 to 32 operations"));
            }
            let mut ids = HashSet::new();
            for item in &batch.operations {
                if item.request_id.is_empty()
                    || item.request_id.len() > 256
                    || !ids.insert(&item.request_id)
                {
                    return Err(Error::invalid(
                        "batch request IDs must be unique and contain 1 to 256 bytes",
                    ));
                }
                validate_operation(&item.operation)?;
                if matches!(item.operation, Operation::TerminateRun { .. }) {
                    return Err(Error::invalid("execution.terminate_run cannot be batched"));
                }
            }
        }
    }
    let mut value = serde_json::to_value(request)?;
    value
        .as_object_mut()
        .ok_or_else(Error::internal)?
        .remove("protocol_version");
    value
        .as_object_mut()
        .ok_or_else(Error::internal)?
        .remove("request_id");
    // Leave room for generated correlation/generation fields and the WS envelope.
    if serde_json::to_vec(&value)?.len() > protocol::MAX_FRAME_BYTES - 1024 {
        return Err(Error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request exceeds transport limit",
        ));
    }
    Ok(value)
}
fn validate_operation(operation: &Operation) -> Result<()> {
    if matches!(operation, Operation::Shutdown) {
        return Err(Error::invalid("runtime.shutdown is unavailable remotely"));
    }
    Ok(())
}

pub fn wire_request(value: Value, id: Uuid, generation: Uuid) -> Result<Request> {
    let mut object = value.as_object().cloned().ok_or_else(Error::internal)?;
    if let Some(expected) = object
        .get("expected_generation_id")
        .filter(|v| !v.is_null())
        && serde_json::from_value::<Uuid>(expected.clone())? != generation
    {
        return Err(Error::conflict(
            "generation_mismatch",
            "machine runtime generation changed",
        ));
    }
    object.insert("protocol_version".into(), json!(protocol::VERSION));
    object.insert("request_id".into(), json!(id.to_string()));
    object.insert("expected_generation_id".into(), json!(generation));
    Ok(serde_json::from_value(Value::Object(object))?)
}

pub async fn submit(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    ApiJson(input): ApiJson<Submit>,
) -> Result<(StatusCode, Json<Value>)> {
    if input.idempotency_key.is_empty()
        || input.idempotency_key.len() > 200
        || input
            .idempotency_key
            .chars()
            .any(|c| c.is_whitespace() || c == '\0')
    {
        return Err(Error::invalid(
            "idempotencyKey must contain 1 to 200 bytes without whitespace",
        ));
    }
    let request = normalize(input.request)?;
    let fingerprint = crypto::hash(&serde_json::to_vec(
        &json!({"machineId": input.machine_id, "request": request}),
    )?);
    let mut tx = state.pool.begin().await?;
    // Serialize one user's admission and idempotency checks; the unique constraint is authoritative.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(user.to_string())
        .execute(&mut *tx)
        .await?;
    if let Some((id, status, hash)) = sqlx::query_as::<_, (Uuid, String, String)>(
        "SELECT id,status,request_hash FROM jobs WHERE user_id=$1 AND idempotency_key=$2",
    )
    .bind(user)
    .bind(&input.idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    {
        if hash != fingerprint {
            return Err(Error::conflict(
                "idempotency_conflict",
                "idempotency key was used with different input",
            ));
        }
        tx.commit().await?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(json!({"id": id, "status": status})),
        ));
    }
    let enabled: bool = sqlx::query_scalar(
        "SELECT enabled FROM machines WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL FOR SHARE",
    )
    .bind(input.machine_id)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(Error::missing)?;
    let user_enabled: bool = sqlx::query_scalar("SELECT enabled FROM users WHERE id=$1 FOR SHARE")
        .bind(user)
        .fetch_one(&mut *tx)
        .await?;
    if !user_enabled {
        return Err(Error::unauthorized());
    }
    if !enabled {
        return Err(Error::conflict("machine_disabled", "machine is disabled"));
    }
    let generation = state
        .connections
        .generation(input.machine_id)
        .await
        .ok_or_else(|| Error::conflict("machine_offline", "machine is offline"))?;
    let id = Uuid::new_v4();
    wire_request(request.clone(), id, generation)?;
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE user_id=$1 AND status IN ('queued','dispatching','waiting_response')")
        .bind(user).fetch_one(&mut *tx).await?;
    if pending >= 256 {
        return Err(Error(
            StatusCode::TOO_MANY_REQUESTS,
            "resource_limit",
            "user has too many outstanding jobs",
        ));
    }
    sqlx::query("INSERT INTO jobs(id,user_id,machine_id,idempotency_key,request_hash) VALUES($1,$2,$3,$4,$5)")
        .bind(id).bind(user).bind(input.machine_id).bind(input.idempotency_key).bind(fingerprint).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO job_requests(job_id,request) VALUES($1,$2)")
        .bind(id)
        .bind(request)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "status": "queued"})),
    ))
}
pub async fn list(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Query(filter): Query<Filter>,
) -> Result<Json<Value>> {
    let mut query = QueryBuilder::new(format!("SELECT {COLUMNS} FROM jobs WHERE user_id="));
    query.push_bind(user);
    if let Some(machine) = filter.machine_id {
        query.push(" AND machine_id=").push_bind(machine);
    }
    if let Some(status) = filter.status {
        if ![
            "queued",
            "dispatching",
            "waiting_response",
            "succeeded",
            "failed",
            "unknown",
        ]
        .contains(&status.as_str())
        {
            return Err(Error::invalid("invalid job status"));
        }
        query.push(" AND status=").push_bind(status);
    }
    if let Some(key) = filter.idempotency_key {
        query.push(" AND idempotency_key=").push_bind(key);
    }
    Ok(Json(
        db::page::<Job>(
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
) -> Result<Json<Value>> {
    Ok(Json(detail(&state, user, id).await?))
}

pub async fn wait(
    State(state): State<AppState>,
    Extension(UserId(user)): Extension<UserId>,
    Path(id): Path<Uuid>,
    Query(query): Query<Wait>,
) -> Result<Json<Value>> {
    let timeout = query.timeout()?;
    if terminal(&status(&state, user, id).await?) {
        return Ok(Json(detail(&state, user, id).await?));
    }
    let mut subscription = state.job_completions.subscribe(user, id).ok_or(Error(
        StatusCode::TOO_MANY_REQUESTS,
        "resource_limit",
        "too many concurrent job waits",
    ))?;
    // The second read closes the completion race between the first read and subscription.
    if !terminal(&status(&state, user, id).await?) {
        subscription.wait(&state.shutdown, timeout).await;
    }
    Ok(Json(detail(&state, user, id).await?))
}

async fn status(state: &AppState, user: Uuid, id: Uuid) -> Result<String> {
    sqlx::query_scalar("SELECT status FROM jobs WHERE id=$1 AND user_id=$2")
        .bind(id)
        .bind(user)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(Error::missing)
}

async fn detail(state: &AppState, user: Uuid, id: Uuid) -> Result<Value> {
    let mut tx = state.pool.begin().await?;
    let job: Job = sqlx::query_as(&format!(
        "SELECT {COLUMNS} FROM jobs WHERE id=$1 AND user_id=$2 FOR SHARE"
    ))
    .bind(id)
    .bind(user)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(Error::missing)?;
    let (response, error): (Option<Value>, Option<Value>) =
        sqlx::query_as("SELECT response,error FROM jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
    let request: Option<(Option<Value>, Option<DateTime<Utc>>)> = sqlx::query_as("SELECT CASE WHEN expires_at IS NULL OR expires_at>clock_timestamp() THEN request ELSE NULL END,expires_at FROM job_requests WHERE job_id=$1")
        .bind(id).fetch_optional(&mut *tx).await?;
    tx.commit().await?;
    let (input, expires) = request.unwrap_or((None, None));
    let mut value = serde_json::to_value(job)?;
    value["response"] = json!(response);
    value["error"] = json!(error);
    value["requestStatus"] = json!(if input.is_some() {
        "retained"
    } else {
        "expired"
    });
    value["request"] = json!(input);
    value["requestExpiresAt"] = json!(expires);
    Ok(value)
}

pub async fn finish_tx(
    tx: &mut Transaction<'_, Postgres>,
    id: Uuid,
    status: &str,
    response: Option<Value>,
    error: Option<Value>,
    retention: i32,
) -> Result<()> {
    let updated: Option<(Uuid, Uuid, DateTime<Utc>)> = sqlx::query_as("UPDATE jobs SET status=$2,response=$3,error=$4,finished_at=clock_timestamp(),recovery_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND status IN ('queued','dispatching','waiting_response') RETURNING user_id,machine_id,finished_at")
        .bind(id).bind(status).bind(&response).bind(&error).fetch_optional(&mut **tx).await?;
    let Some((user, machine, finished)) = updated else {
        return Ok(());
    };
    let (callback, payload_version): (String, i32) = sqlx::query_as(
        "SELECT callback_url,webhook_payload_version FROM users WHERE id=$1 FOR SHARE",
    )
    .bind(user)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE job_requests SET expires_at=$2 + make_interval(days => $3) WHERE job_id=$1",
    )
    .bind(id)
    .bind(finished)
    .bind(retention)
    .execute(&mut **tx)
    .await?;
    crate::job_completion::publish(tx, id).await?;
    if callback.is_empty() {
        return Ok(());
    }
    let event = Uuid::new_v4();
    let event_type = format!("job.{status}");
    let payload = terminal_event(
        payload_version,
        event,
        event_type.clone(),
        id,
        machine,
        finished,
        &response,
        &error,
    )?;
    sqlx::query("INSERT INTO webhook_deliveries(id,job_id,user_id,event_type,callback_url,payload) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(event).bind(id).bind(user).bind(event_type).bind(callback).bind(payload).execute(&mut **tx).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn terminal_event(
    version: i32,
    event: Uuid,
    event_type: String,
    job: Uuid,
    machine: Uuid,
    completed: DateTime<Utc>,
    response: &Option<Value>,
    error: &Option<Value>,
) -> Result<Value> {
    match version {
        1 => Ok(
            json!({"eventId": event, "type": event_type, "jobId": job, "machineId": machine, "completedAt": completed, "response": response, "error": error}),
        ),
        2 => Ok(serde_json::to_value(TerminalJobEventV2 {
            schema_version: 2,
            event_id: event,
            r#type: event_type,
            job_id: job,
            machine_id: machine,
            completed_at: completed,
        })?),
        _ => Err(Error::internal()),
    }
}
pub async fn finish(state: &AppState, id: Uuid, status: &str, error: Value) -> Result<()> {
    let mut tx = state.pool.begin().await?;
    finish_tx(
        &mut tx,
        id,
        status,
        None,
        Some(error),
        state.config.retention_days,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Claim only work that has never been dispatched. There is no automatic replay.
pub async fn dispatch_next(
    state: &AppState,
    machine: Uuid,
    generation: Uuid,
    version: i32,
    recovery: bool,
    control_only: bool,
) -> Result<Option<(Uuid, Request)>> {
    let mut tx = state.pool.begin().await?;
    let usable: bool = sqlx::query_scalar("SELECT m.enabled AND u.enabled AND m.deleted_at IS NULL AND m.credential_version=$2 FROM machines m JOIN users u ON u.id=m.user_id WHERE m.id=$1 FOR SHARE OF m,u")
        .bind(machine).bind(version).fetch_one(&mut *tx).await?;
    let job: Option<(Uuid, Value)> = sqlx::query_as("SELECT j.id,r.request FROM jobs j JOIN job_requests r ON r.job_id=j.id WHERE j.machine_id=$1 AND j.status='queued' AND (NOT $2 OR r.request->>'operation'='execution.terminate_run') ORDER BY (r.request->>'operation'='execution.terminate_run') DESC,j.created_at,j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED")
        .bind(machine).bind(control_only).fetch_optional(&mut *tx).await?;
    let Some((id, value)) = job else {
        return Ok(None);
    };
    let request = if usable {
        wire_request(value, id, generation)
    } else {
        Err(Error::conflict(
            "machine_disabled",
            "machine or user is unavailable for dispatch",
        ))
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => {
            finish_tx(
                &mut tx,
                id,
                "failed",
                None,
                Some(error.value()),
                state.config.retention_days,
            )
            .await?;
            tx.commit().await?;
            return Ok(None);
        }
    };
    sqlx::query("UPDATE jobs SET status='dispatching',runtime_generation_id=$2,response_recovery=$3,dispatched_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1")
        .bind(id).bind(generation).bind(recovery).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some((id, request)))
}

pub async fn response(
    state: &AppState,
    machine: Uuid,
    generation: Uuid,
    id: Uuid,
    response: Response,
) -> Result<()> {
    let status = if response.succeeded() {
        "succeeded"
    } else {
        "failed"
    };
    let mut tx = state.pool.begin().await?;
    let expired: bool = sqlx::query_scalar("SELECT COALESCE(recovery_expires_at<=clock_timestamp(),false) FROM jobs WHERE id=$1 AND machine_id=$2 AND runtime_generation_id=$3 AND status <> 'queued' FOR UPDATE")
        .bind(id).bind(machine).bind(generation).fetch_optional(&mut *tx).await?
        .ok_or_else(|| Error::invalid("response does not match a dispatched job"))?;
    if expired {
        finish_tx(&mut tx, id, "unknown", None, Some(json!({"code":"recovery_unavailable","message":"response recovery window expired; work was not replayed"})), state.config.retention_days).await?;
        tx.commit().await?;
        return Ok(());
    }
    finish_tx(
        &mut tx,
        id,
        status,
        Some(serde_json::to_value(response)?),
        None,
        state.config.retention_days,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn recover_inflight(state: &AppState) -> Result<()> {
    sqlx::query("UPDATE jobs SET recovery_expires_at=COALESCE(recovery_expires_at,clock_timestamp()+interval '24 hours') WHERE response_recovery AND status IN ('dispatching','waiting_response')")
        .execute(&state.pool).await?;
    loop {
        let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM jobs WHERE NOT response_recovery AND status IN ('dispatching','waiting_response') ORDER BY created_at LIMIT 100").fetch_all(&state.pool).await?;
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            finish(state, id, "unknown", json!({"code":"gateway_restarted","message":"gateway lost the operation response; work was not replayed"})).await?;
        }
    }
}
/// A lost socket never causes an operation to be dispatched again.
pub async fn disconnected(state: &AppState, machine: Uuid, generation: Uuid) -> Result<()> {
    sqlx::query("UPDATE jobs SET recovery_expires_at=COALESCE(recovery_expires_at,clock_timestamp()+interval '24 hours'),updated_at=clock_timestamp() WHERE machine_id=$1 AND runtime_generation_id=$2 AND response_recovery AND status IN ('dispatching','waiting_response')")
        .bind(machine).bind(generation).execute(&state.pool).await?;
    Ok(())
}

pub async fn reconnect(
    state: &AppState,
    machine: Uuid,
    generation: Uuid,
    recovery: bool,
) -> Result<Vec<Uuid>> {
    let jobs: Vec<(Uuid, Uuid, bool, bool)> = sqlx::query_as("SELECT id,runtime_generation_id,response_recovery,COALESCE(recovery_expires_at<=clock_timestamp(),false) FROM jobs WHERE machine_id=$1 AND status IN ('dispatching','waiting_response') ORDER BY created_at,id")
        .bind(machine).fetch_all(&state.pool).await?;
    let mut pending = Vec::new();
    for (id, previous, supported, expired) in jobs {
        if expired {
            finish(state, id, "unknown", json!({"code":"recovery_unavailable","message":"response recovery window expired; work was not replayed"})).await?;
        } else if recovery && supported && previous == generation {
            pending.push(id);
        } else {
            finish(state, id, "unknown", json!({"code":"runtime_replaced","message":"original runtime receipts are unavailable; work was not replayed"})).await?;
        }
    }
    Ok(pending)
}

pub async fn accepted(state: &AppState, machine: Uuid, generation: Uuid, id: Uuid) -> Result<()> {
    // A duplicate acceptance may follow an already committed response.
    sqlx::query("UPDATE jobs SET status='waiting_response',recovery_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND machine_id=$2 AND runtime_generation_id=$3 AND response_recovery AND (recovery_expires_at IS NULL OR recovery_expires_at>clock_timestamp()) AND status IN ('dispatching','waiting_response')")
        .bind(id).bind(machine).bind(generation).execute(&state.pool).await?;
    Ok(())
}

pub async fn housekeeping(state: AppState) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! { _ = state.shutdown.cancelled() => break, _ = tick.tick() => {} }
        if let Err(error) = housekeeping_once(&state).await {
            eprintln!("gateway maintenance: {}", error.1);
        }
    }
}
pub async fn housekeeping_once(state: &AppState) -> Result<()> {
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM jobs WHERE status='queued' AND created_at<clock_timestamp()-interval '30 seconds' ORDER BY created_at LIMIT 100").fetch_all(&state.pool).await?;
    for id in ids {
        let mut tx = state.pool.begin().await?;
        let queued: bool =
            sqlx::query_scalar("SELECT status='queued' FROM jobs WHERE id=$1 FOR UPDATE")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        if queued {
            finish_tx(&mut tx, id, "failed", None, Some(json!({"code":"dispatch_timeout","message":"job was not dispatched within 30 seconds"})), state.config.retention_days).await?;
        }
        tx.commit().await?;
    }
    let expired: Vec<Uuid> = sqlx::query_scalar("SELECT j.id FROM jobs j JOIN machines m ON m.id=j.machine_id WHERE j.status IN ('dispatching','waiting_response') AND (j.recovery_expires_at<=clock_timestamp() OR m.deleted_at IS NOT NULL) ORDER BY j.created_at LIMIT 100")
        .fetch_all(&state.pool).await?;
    for id in expired {
        let mut tx = state.pool.begin().await?;
        let expired: bool = sqlx::query_scalar("SELECT j.status IN ('dispatching','waiting_response') AND (COALESCE(j.recovery_expires_at<=clock_timestamp(),false) OR m.deleted_at IS NOT NULL) FROM jobs j JOIN machines m ON m.id=j.machine_id WHERE j.id=$1 FOR UPDATE OF j")
            .bind(id).fetch_one(&mut *tx).await?;
        if expired {
            finish_tx(&mut tx, id, "unknown", None, Some(json!({"code":"recovery_unavailable","message":"machine was removed or its 24-hour response recovery window expired; work was not replayed"})), state.config.retention_days).await?;
        }
        tx.commit().await?;
    }
    sqlx::query("DELETE FROM job_requests WHERE job_id IN (SELECT r.job_id FROM job_requests r JOIN jobs j ON j.id=r.job_id WHERE r.expires_at<=clock_timestamp() AND j.status IN ('succeeded','failed','unknown') LIMIT 100)")
        .execute(&state.pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_WAIT_MS, Wait, terminal_event};
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn job_waits_default_to_five_minutes_and_are_bounded() {
        assert_eq!(
            Wait { timeout_ms: None }.timeout().unwrap().as_millis(),
            u128::from(MAX_WAIT_MS)
        );
        assert!(
            Wait {
                timeout_ms: Some(1)
            }
            .timeout()
            .is_ok()
        );
        assert!(
            Wait {
                timeout_ms: Some(MAX_WAIT_MS)
            }
            .timeout()
            .is_ok()
        );
        assert!(
            Wait {
                timeout_ms: Some(0)
            }
            .timeout()
            .is_err()
        );
        assert!(
            Wait {
                timeout_ms: Some(MAX_WAIT_MS + 1)
            }
            .timeout()
            .is_err()
        );
    }

    #[test]
    fn terminal_events_are_versioned_without_breaking_legacy_consumers() {
        let event = Uuid::new_v4();
        let job = Uuid::new_v4();
        let machine = Uuid::new_v4();
        let completed = Utc::now();
        let expected_response = json!({"status":"ok"});
        let response = Some(expected_response.clone());
        let legacy = terminal_event(
            1,
            event,
            "job.succeeded".into(),
            job,
            machine,
            completed,
            &response,
            &None,
        )
        .unwrap();
        assert_eq!(legacy["response"], expected_response);
        assert!(legacy.get("schemaVersion").is_none());

        let payload = terminal_event(
            2,
            event,
            "job.succeeded".into(),
            job,
            machine,
            completed,
            &None,
            &None,
        )
        .unwrap();
        assert_eq!(payload["eventId"], event.to_string());
        assert_eq!(payload["jobId"], job.to_string());
        assert_eq!(payload["machineId"], machine.to_string());
        assert_eq!(payload["type"], "job.succeeded");
        assert_eq!(payload["schemaVersion"], 2);
        assert_eq!(payload.as_object().unwrap().len(), 6);
    }
}
