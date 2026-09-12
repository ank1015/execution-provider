use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use process_execution_core as core;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use uuid::Uuid;

pub const VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub protocol_version: u32,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation_id: Option<Uuid>,
    #[serde(flatten)]
    pub operation: Operation,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", content = "params")]
pub enum Operation {
    #[serde(rename = "runtime.info")]
    Info,
    #[serde(rename = "runtime.shutdown")]
    Shutdown,
    #[serde(rename = "execution.start")]
    Start(StartParams),
    #[serde(rename = "execution.get")]
    Get { handle: core::ExecutionHandle },
    #[serde(rename = "execution.observe")]
    Observe(ObserveParams),
    #[serde(rename = "execution.write_input")]
    WriteInput {
        handle: core::ExecutionHandle,
        input_id: String,
        data_base64: String,
    },
    #[serde(rename = "execution.close_input")]
    CloseInput { handle: core::ExecutionHandle },
    #[serde(rename = "execution.interrupt")]
    Interrupt {
        handle: core::ExecutionHandle,
        operation_id: String,
    },
    #[serde(rename = "execution.terminate")]
    Terminate {
        handle: core::ExecutionHandle,
        grace_period_ms: Option<u64>,
    },
    #[serde(rename = "execution.resize_terminal")]
    Resize {
        handle: core::ExecutionHandle,
        rows: u16,
        cols: u16,
    },
    #[serde(rename = "execution.list")]
    List(ListParams),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartParams {
    pub start_id: String,
    pub command: core::Command,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub io: core::IoMode,
    #[serde(default)]
    pub wait_ms: u64,
    pub max_output_bytes: Option<usize>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObserveParams {
    pub handle: core::ExecutionHandle,
    pub after_cursor: Option<String>,
    #[serde(default)]
    pub wait_ms: u64,
    #[serde(default)]
    pub return_when: WaitMode,
    pub max_output_bytes: Option<usize>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    #[default]
    Activity,
    FinishedOrTimeout,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateFilter {
    #[default]
    Active,
    Finished,
    All,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListParams {
    #[serde(default)]
    pub state: StateFilter,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default = "page_size")]
    pub limit: usize,
    pub page_cursor: Option<String>,
}
fn page_size() -> usize {
    50
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub protocol_version: u32,
    pub request_id: Option<String>,
    pub generation_id: Uuid,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Outcome {
    Ok { result: Value },
    Error { error: core::Error },
}

impl Response {
    pub fn new(
        request_id: Option<String>,
        generation_id: Uuid,
        result: core::Result<Value>,
    ) -> Self {
        Self {
            protocol_version: VERSION,
            request_id,
            generation_id,
            outcome: match result {
                Ok(result) => Outcome::Ok { result },
                Err(error) => Outcome::Error { error },
            },
        }
    }
    pub fn is_ok(&self) -> bool {
        matches!(self.outcome, Outcome::Ok { .. })
    }
}

pub fn invalid(message: impl Into<String>) -> core::Error {
    core::Error {
        code: core::ErrorCode::InvalidArgument,
        message: message.into(),
    }
}

pub fn version_info() -> Value {
    json!({"binary": "process-execution", "version": env!("CARGO_PKG_VERSION"),
        "protocol_version": VERSION, "platform": std::env::consts::OS, "architecture": std::env::consts::ARCH})
}

pub async fn dispatch(
    runtime: &core::ProcessExecutionCore,
    request: Request,
) -> core::Result<Value> {
    if request.protocol_version != VERSION {
        return Err(invalid(format!(
            "unsupported protocol version {}; expected {VERSION}",
            request.protocol_version
        )));
    }
    if request.request_id.is_empty() {
        return Err(invalid("request_id is empty"));
    }
    let info = runtime.runtime_info();
    if request
        .expected_generation_id
        .is_some_and(|id| id != info.generation_id)
    {
        return Err(core::Error {
            code: core::ErrorCode::GenerationMismatch,
            message: "supervisor generation changed".into(),
        });
    }
    match request.operation {
        Operation::Info => Ok(json!({"runtime": info, "binary": version_info()})),
        Operation::Shutdown => {
            runtime.shutdown().await?;
            Ok(json!({"shutdown": true}))
        }
        Operation::Start(params) => {
            let result = runtime
                .start_execution(core::StartRequest {
                    start_id: params.start_id,
                    command: params.command,
                    cwd: params.cwd,
                    env: params.env,
                    io: params.io,
                    wait_ms: params.wait_ms,
                    max_output_bytes: params.max_output_bytes,
                    labels: params.labels,
                })
                .await?;
            observation(result)
        }
        Operation::Get { handle } => value(runtime.get_execution(handle).await?),
        Operation::Observe(params) => observation(
            runtime
                .observe_execution(core::ObserveRequest {
                    handle: params.handle,
                    after_cursor: params
                        .after_cursor
                        .as_deref()
                        .map(decode_cursor)
                        .transpose()?,
                    wait_ms: params.wait_ms,
                    return_when: match params.return_when {
                        WaitMode::Activity => core::WaitMode::Activity,
                        WaitMode::FinishedOrTimeout => core::WaitMode::FinishedOrTimeout,
                    },
                    max_output_bytes: params.max_output_bytes,
                })
                .await?,
        ),
        Operation::WriteInput {
            handle,
            input_id,
            data_base64,
        } => {
            let data = STANDARD
                .decode(data_base64)
                .map_err(|e| invalid(format!("invalid base64 input: {e}")))?;
            value(runtime.write_input(handle, input_id, data).await?)
        }
        Operation::CloseInput { handle } => {
            let state = match runtime.close_input(handle).await? {
                core::StdinState::Open => "open",
                core::StdinState::Closing => "closing",
                core::StdinState::Closed => "closed",
            };
            Ok(json!({"stdin_state": state}))
        }
        Operation::Interrupt {
            handle,
            operation_id,
        } => value(runtime.interrupt_execution(handle, operation_id).await?),
        Operation::Terminate {
            handle,
            grace_period_ms,
        } => value(
            runtime
                .terminate_execution(handle, grace_period_ms.map(Duration::from_millis))
                .await?,
        ),
        Operation::Resize { handle, rows, cols } => {
            value(runtime.resize_terminal(handle, rows, cols).await?)
        }
        Operation::List(params) => {
            let result = runtime
                .list_executions(core::ListRequest {
                    state: match params.state {
                        StateFilter::Active => core::StateFilter::Active,
                        StateFilter::Finished => core::StateFilter::Finished,
                        StateFilter::All => core::StateFilter::All,
                    },
                    labels: params.labels,
                    limit: params.limit,
                    page_cursor: params
                        .page_cursor
                        .as_deref()
                        .map(decode_cursor)
                        .transpose()?,
                })
                .await?;
            Ok(
                json!({"executions": result.executions, "next_page_cursor": result.next_page_cursor.as_ref().map(encode_cursor).transpose()?}),
            )
        }
    }
}

fn value(value: impl Serialize) -> core::Result<Value> {
    serde_json::to_value(value).map_err(|e| core::Error {
        code: core::ErrorCode::Io,
        message: e.to_string(),
    })
}

fn observation(result: core::Observation) -> core::Result<Value> {
    let output: Vec<_> = result
        .output
        .into_iter()
        .map(|chunk| json!({"stream": chunk.stream, "data_base64": STANDARD.encode(chunk.data)}))
        .collect();
    Ok(json!({"execution": result.execution, "output": output,
        "next_cursor": encode_cursor(&result.next_cursor)?, "has_more": result.has_more,
        "output_gap": result.output_gap, "return_reason": result.return_reason}))
}

fn encode_cursor(cursor: &impl Serialize) -> core::Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor).map_err(|e| invalid(e.to_string()))?))
}

fn decode_cursor<T: DeserializeOwned>(cursor: &str) -> core::Result<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid("invalid cursor encoding"))?;
    serde_json::from_slice(&bytes).map_err(|_| invalid("invalid cursor contents"))
}
