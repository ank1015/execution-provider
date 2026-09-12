use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Error(pub StatusCode, pub &'static str, pub &'static str);

impl Error {
    pub fn invalid(message: &'static str) -> Self {
        Self(StatusCode::BAD_REQUEST, "invalid_argument", message)
    }
    pub fn unauthorized() -> Self {
        Self(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid or inactive credential",
        )
    }
    pub fn missing() -> Self {
        Self(StatusCode::NOT_FOUND, "not_found", "resource not found")
    }
    pub fn conflict(code: &'static str, message: &'static str) -> Self {
        Self(StatusCode::CONFLICT, code, message)
    }
    pub fn internal() -> Self {
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "request could not be completed",
        )
    }
    pub fn unavailable() -> Self {
        Self(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "service temporarily unavailable",
        )
    }
    pub fn value(&self) -> serde_json::Value {
        json!({"code": self.1, "message": self.2})
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.value()}))).into_response()
    }
}
impl From<sqlx::Error> for Error {
    fn from(_: sqlx::Error) -> Self {
        Self::internal()
    }
}
impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::invalid("invalid request JSON")
    }
}
