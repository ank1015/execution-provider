pub mod config;
pub mod connections;
pub mod crypto;
pub mod db;
pub mod error;
pub mod job_completion;
pub mod jobs;
pub mod machines;
pub mod users;
pub mod webhooks;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, request::Parts},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use config::Config;
use error::{Error, Result};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub connections: Arc<connections::Connections>,
    pub job_completions: Arc<job_completion::JobCompletions>,
    pub http: reqwest::Client,
    pub shutdown: CancellationToken,
}

impl AppState {
    pub fn new(pool: PgPool, config: Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .no_proxy()
            .build()
            .map_err(|_| Error::internal())?;
        Ok(Self {
            pool,
            config: Arc::new(config),
            connections: Arc::new(connections::Connections::default()),
            job_completions: Arc::new(job_completion::JobCompletions::default()),
            http,
            shutdown: CancellationToken::new(),
        })
    }
}

/// Normalize extractor failures without returning request contents in errors.
pub struct ApiJson<T>(pub T);
impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Error;
    async fn from_request(req: Request, state: &S) -> Result<Self> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(v)| Self(v))
            .map_err(|e| match e.status() {
                StatusCode::PAYLOAD_TOO_LARGE => Error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "payload_too_large",
                    "request body exceeds limit",
                ),
                StatusCode::UNSUPPORTED_MEDIA_TYPE => Error(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    "unsupported_media_type",
                    "application/json is required",
                ),
                _ => Error::invalid("invalid request JSON or fields"),
            })
    }
}

pub struct ApiPath<T>(pub T);
impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self> {
        axum::extract::Path::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Path(v)| Self(v))
            .map_err(|_| Error::invalid("invalid resource identifier"))
    }
}
pub struct ApiQuery<T>(pub T);
impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Error;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self> {
        axum::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(v)| Self(v))
            .map_err(|_| Error::invalid("invalid query parameters"))
    }
}

pub fn bearer(headers: &HeaderMap) -> Result<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty() && v.len() <= 8192 && v.bytes().all(|b| (33..=126).contains(&b)))
        .ok_or_else(Error::unauthorized)
}

async fn no_store(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let mut response = if state.shutdown.is_cancelled() {
        Error::unavailable().into_response()
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    response
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok"}))
}
async fn ready(State(state): State<AppState>) -> Result<Json<Value>> {
    sqlx::query("SELECT id FROM jobs LIMIT 0")
        .execute(&state.pool)
        .await
        .map_err(|_| Error::unavailable())?;
    Ok(Json(json!({"status": "ready"})))
}

pub fn router(state: AppState) -> Router {
    let admin = Router::new()
        .route("/v1/admin/users", post(users::create).get(users::list))
        .route(
            "/v1/admin/users/{user}",
            get(users::get).patch(users::update),
        )
        .route(
            "/v1/admin/users/{user}/keys",
            post(users::issue_key).get(users::list_keys),
        )
        .route(
            "/v1/admin/users/{user}/keys/{key}",
            axum::routing::delete(users::revoke_key),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            users::admin_auth,
        ));
    let management = Router::new()
        .route("/v1/me", get(users::me).patch(users::update_me))
        .route("/v1/me/webhook-secret/rotate", post(users::rotate_secret))
        .route("/v1/machines", post(machines::create).get(machines::list))
        .route(
            "/v1/machines/{machine}",
            get(machines::get)
                .patch(machines::update)
                .delete(machines::remove),
        )
        .route(
            "/v1/machines/{machine}/registration",
            post(machines::issue_registration),
        )
        .route("/v1/webhook-deliveries", get(webhooks::list))
        .route("/v1/webhook-deliveries/{delivery}", get(webhooks::get))
        .route(
            "/v1/webhook-deliveries/{delivery}/redeliver",
            post(webhooks::redeliver),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            users::user_auth,
        ));
    let jobs = Router::new()
        .route("/v1/jobs", post(jobs::submit).get(jobs::list))
        .route("/v1/jobs/{job}", get(jobs::get))
        .route("/v1/jobs/{job}/wait", get(jobs::wait))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            users::user_auth,
        ))
        .layer(DefaultBodyLimit::max(
            process_execution_protocol::MAX_FRAME_BYTES,
        ));
    let devices = Router::new()
        .route("/v1/machines/{machine}/register", post(machines::register))
        .route("/v1/machines/{machine}/connect", get(connections::upgrade))
        .route("/v1/hosts/{machine}/connect", get(connections::upgrade));
    Router::new()
        .merge(
            admin
                .merge(management)
                .merge(devices)
                .layer(DefaultBodyLimit::max(16 * 1024)),
        )
        .merge(jobs)
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .fallback(|| async { Error::missing() })
        .layer(middleware::from_fn_with_state(state.clone(), no_store))
        .with_state(state)
}
