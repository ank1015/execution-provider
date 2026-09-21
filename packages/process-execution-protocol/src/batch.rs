use super::*;
use futures_util::{StreamExt, stream};
use serde::{Deserializer, de::Error as _};
use std::collections::HashSet;

pub const MAX_BATCH_OPERATIONS: usize = 32;
pub const MAX_PARALLEL_OPERATIONS: usize = 8;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Payload {
    Single(Operation),
    Batch(Batch),
}

impl<'de> Deserialize<'de> for Payload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| D::Error::custom("request must be an object"))?;
        let batch = object.contains_key("operations");
        let allowed = if batch {
            &["mode", "accepted_error_codes", "operations"][..]
        } else {
            &["operation", "params"][..]
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(D::Error::custom("unexpected or mixed request fields"));
        }
        if batch {
            serde_json::from_value(value)
                .map(Self::Batch)
                .map_err(D::Error::custom)
        } else {
            serde_json::from_value(value)
                .map(Self::Single)
                .map_err(D::Error::custom)
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    #[default]
    Sequential,
    Parallel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Batch {
    #[serde(default)]
    pub mode: BatchMode,
    /// Per-item operation errors that are expected by the caller. They remain
    /// encoded as error outcomes, but do not fail the batch or stop sequential
    /// dispatch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepted_error_codes: Vec<core::ErrorCode>,
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug, Serialize)]
pub struct BatchOperation {
    pub request_id: String,
    #[serde(flatten)]
    pub operation: Operation,
}

impl<'de> Deserialize<'de> for BatchOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let mut value = Value::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| D::Error::custom("batch operation must be an object"))?;
        let request_id = object
            .remove("request_id")
            .ok_or_else(|| D::Error::custom("batch operation requires request_id"))?;
        let request_id = serde_json::from_value(request_id).map_err(D::Error::custom)?;
        if object
            .keys()
            .any(|key| !["operation", "params"].contains(&key.as_str()))
        {
            return Err(D::Error::custom("unexpected batch operation fields"));
        }
        let operation = serde_json::from_value(value).map_err(D::Error::custom)?;
        Ok(Self {
            request_id,
            operation,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperationResponse {
    pub request_id: String,
    #[serde(flatten)]
    pub outcome: OperationOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OperationOutcome {
    Ok { result: Value },
    Error { error: core::Error },
    Skipped { reason: String },
}

impl Request {
    pub fn requests_shutdown(&self) -> bool {
        matches!(self.payload, Payload::Single(Operation::Shutdown))
    }

    pub fn terminates_run(&self) -> bool {
        matches!(
            self.payload,
            Payload::Single(Operation::TerminateRun { .. })
        )
    }
}

impl Response {
    /// A valid batch envelope can contain failed or skipped operations.
    pub fn succeeded(&self) -> bool {
        matches!(&self.outcome, Outcome::Ok { result }
            if result.get("succeeded") != Some(&Value::Bool(false))
            && !matches!(result.get("status").and_then(Value::as_str), Some("rejected" | "partial")))
    }
}

#[derive(Clone)]
pub struct Dispatcher {
    runtime: core::ProcessExecutionCore,
    binary: Value,
    allow_shutdown: bool,
}

impl Dispatcher {
    pub fn local(runtime: core::ProcessExecutionCore, binary: Value) -> Self {
        Self {
            runtime,
            binary,
            allow_shutdown: true,
        }
    }

    pub fn gateway(runtime: core::ProcessExecutionCore, binary: Value) -> Self {
        Self {
            runtime,
            binary,
            allow_shutdown: false,
        }
    }

    pub fn generation_id(&self) -> Uuid {
        self.runtime.runtime_info().generation_id
    }

    pub async fn dispatch(&self, request: Request) -> Response {
        let result = match self.validate(&request) {
            Ok(()) => match request.payload {
                Payload::Single(operation) => self.operation(operation).await,
                Payload::Batch(batch) => self.batch(batch).await,
            },
            Err(error) => Err(error),
        };
        Response::new(Some(request.request_id), self.generation_id(), result)
    }

    fn validate(&self, request: &Request) -> core::Result<()> {
        if request.protocol_version != VERSION {
            return Err(invalid(format!(
                "unsupported protocol version {}; expected {VERSION}",
                request.protocol_version
            )));
        }
        validate_id(&request.request_id)?;
        if request
            .expected_generation_id
            .is_some_and(|id| id != self.generation_id())
        {
            return Err(core::Error {
                code: core::ErrorCode::GenerationMismatch,
                message: "supervisor generation changed".into(),
            });
        }
        match &request.payload {
            Payload::Single(Operation::Shutdown) if !self.allow_shutdown => Err(invalid(
                "runtime.shutdown is a local administration operation",
            )),
            Payload::Batch(batch) => {
                if batch.operations.is_empty() || batch.operations.len() > MAX_BATCH_OPERATIONS {
                    return Err(invalid(format!(
                        "batches require 1 to {MAX_BATCH_OPERATIONS} operations"
                    )));
                }
                let mut ids = HashSet::new();
                for operation in &batch.operations {
                    validate_id(&operation.request_id)?;
                    if !ids.insert(&operation.request_id) {
                        return Err(invalid("batch request IDs must be unique"));
                    }
                    if matches!(operation.operation, Operation::Shutdown) {
                        return Err(invalid("runtime.shutdown cannot be batched"));
                    }
                    if matches!(operation.operation, Operation::TerminateRun { .. }) {
                        return Err(invalid("execution.terminate_run cannot be batched"));
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn operation(&self, operation: Operation) -> core::Result<Value> {
        dispatch_operation(&self.runtime, operation, &self.binary).await
    }

    async fn item(&self, item: BatchOperation) -> OperationResponse {
        let outcome = match self.operation(item.operation).await {
            Ok(result) => OperationOutcome::Ok { result },
            Err(error) => OperationOutcome::Error { error },
        };
        OperationResponse {
            request_id: item.request_id,
            outcome,
        }
    }

    async fn batch(&self, batch: Batch) -> core::Result<Value> {
        let accepted_error_codes = batch.accepted_error_codes;
        let results: Vec<OperationResponse> = match batch.mode {
            BatchMode::Parallel => {
                stream::iter(batch.operations)
                    .map(|item| self.item(item))
                    .buffered(MAX_PARALLEL_OPERATIONS)
                    .collect()
                    .await
            }
            BatchMode::Sequential => {
                let mut results = Vec::new();
                let mut failed = false;
                for item in batch.operations {
                    let response = if failed {
                        OperationResponse {
                            request_id: item.request_id,
                            outcome: OperationOutcome::Skipped {
                                reason: "a previous operation failed".into(),
                            },
                        }
                    } else {
                        self.item(item).await
                    };
                    failed |= !outcome_succeeded(&response.outcome, &accepted_error_codes);
                    results.push(response);
                }
                results
            }
        };
        let succeeded = results
            .iter()
            .all(|item| outcome_succeeded(&item.outcome, &accepted_error_codes));
        Ok(json!({"succeeded": succeeded, "results": results}))
    }
}

fn outcome_succeeded(outcome: &OperationOutcome, accepted_error_codes: &[core::ErrorCode]) -> bool {
    match outcome {
        OperationOutcome::Ok { result } => !matches!(
            result.get("status").and_then(Value::as_str),
            Some("rejected" | "partial")
        ),
        OperationOutcome::Error { error } => accepted_error_codes.contains(&error.code),
        OperationOutcome::Skipped { .. } => false,
    }
}

fn validate_id(id: &str) -> core::Result<()> {
    if id.is_empty() || id.len() > 256 {
        return Err(invalid("request_id must contain 1 to 256 bytes"));
    }
    Ok(())
}

/// Shared response size limit for local IPC and gateway transports.
pub fn encode_response(response: &Response) -> Result<Vec<u8>> {
    let frame = serde_json::to_vec(response)?;
    if frame.len() < MAX_FRAME_BYTES {
        return Ok(frame);
    }
    Ok(serde_json::to_vec(&Response::new(
        response.request_id.clone(),
        response.generation_id,
        Err(core::Error {
            code: core::ErrorCode::ResourceLimit,
            message: "response exceeds 8 MiB; reduce output, batch, or page size".into(),
        }),
    ))?)
}
