use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use process_execution_core as core;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use uuid::Uuid;

mod batch;
pub mod gateway;
pub mod runtime_config;
pub use batch::*;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub const VERSION: u32 = 4;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub protocol_version: u32,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation_id: Option<Uuid>,
    #[serde(flatten)]
    pub payload: Payload,
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
    #[serde(rename = "execution.run")]
    Run(RunParams),
    #[serde(rename = "execution.terminate_run")]
    TerminateRun {
        run_id: String,
        grace_period_ms: Option<u64>,
    },
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
    #[serde(rename = "filesystem.get_metadata")]
    GetFileMetadata(FilePathParams),
    #[serde(rename = "filesystem.read_file")]
    ReadFile(ReadFileParams),
    #[serde(rename = "filesystem.write_file")]
    WriteFile(WriteFileParams),
    #[serde(rename = "filesystem.remove_file")]
    RemoveFile(RemoveFileParams),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilePathParams {
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadFileParams {
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteFileParams {
    pub mutation_id: String,
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub data_base64: String,
    #[serde(default)]
    pub create_parent_directories: bool,
    #[serde(default, skip_serializing_if = "is_conditional_write_mode")]
    pub mode: WriteMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precondition: Option<core::FilePrecondition>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Conditional,
    Overwrite,
}

fn is_conditional_write_mode(mode: &WriteMode) -> bool {
    *mode == WriteMode::Conditional
}

impl WriteFileParams {
    pub fn core_mode(&self) -> core::Result<core::WriteFileMode> {
        match (self.mode, &self.precondition) {
            (WriteMode::Conditional, Some(precondition)) => {
                Ok(core::WriteFileMode::Conditional(precondition.clone()))
            }
            (WriteMode::Conditional, None) => {
                Err(invalid("conditional file write requires a precondition"))
            }
            (WriteMode::Overwrite, None) => Ok(core::WriteFileMode::Overwrite),
            (WriteMode::Overwrite, Some(_)) => Err(invalid(
                "overwrite file write must not include a precondition",
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveFileParams {
    pub mutation_id: String,
    pub cwd: Option<PathBuf>,
    pub path: PathBuf,
    pub precondition: core::FilePrecondition,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartParams {
    pub start_id: String,
    pub command: core::Command,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_snapshot: Option<core::ShellSnapshotRequest>,
    #[serde(default)]
    pub io: core::IoMode,
    #[serde(default)]
    pub wait_ms: u64,
    pub max_output_bytes: Option<usize>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunParams {
    pub run_id: String,
    pub command: core::Command,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_snapshot: Option<core::ShellSnapshotRequest>,
    pub timeout_ms: Option<u64>,
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
            request_id: request_id.filter(|id| id.len() <= 256),
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

pub fn version_info(name: &str, version: &str) -> Value {
    json!({"binary": name, "version": version,
        "protocol_version": VERSION, "platform": std::env::consts::OS, "architecture": std::env::consts::ARCH})
}

async fn dispatch_operation(
    runtime: &core::ProcessExecutionCore,
    operation: Operation,
    binary: &Value,
) -> core::Result<Value> {
    let info = runtime.runtime_info();
    match operation {
        Operation::Info => Ok(json!({"runtime": info, "binary": binary})),
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
                    shell_snapshot: params.shell_snapshot,
                    io: params.io,
                    wait_ms: params.wait_ms,
                    max_output_bytes: params.max_output_bytes,
                    labels: params.labels,
                })
                .await?;
            observation(result)
        }
        Operation::Run(params) => {
            let result = runtime
                .run_execution(core::RunRequest {
                    run_id: params.run_id,
                    command: params.command,
                    cwd: params.cwd,
                    env: params.env,
                    shell_snapshot: params.shell_snapshot,
                    timeout_ms: params.timeout_ms,
                    max_output_bytes: params.max_output_bytes,
                    labels: params.labels,
                })
                .await?;
            run_result(result)
        }
        Operation::TerminateRun {
            run_id,
            grace_period_ms,
        } => value(
            runtime
                .terminate_run(run_id, grace_period_ms.map(Duration::from_millis))
                .await?,
        ),
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
        Operation::GetFileMetadata(params) => value(
            runtime
                .get_file_metadata(core::FilePathRequest {
                    cwd: params.cwd,
                    path: params.path,
                })
                .await?,
        ),
        Operation::ReadFile(params) => {
            let result = runtime
                .read_file(core::ReadFileRequest {
                    cwd: params.cwd,
                    path: params.path,
                    max_bytes: params.max_bytes,
                })
                .await?;
            Ok(json!({
                "path": result.path,
                "metadata": result.metadata,
                "data_base64": STANDARD.encode(result.data),
                "sha256": result.sha256,
            }))
        }
        Operation::WriteFile(params) => {
            let mode = params.core_mode()?;
            let data = STANDARD
                .decode(params.data_base64)
                .map_err(|error| invalid(format!("invalid base64 file data: {error}")))?;
            value(
                runtime
                    .write_file(core::WriteFileRequest {
                        mutation_id: params.mutation_id,
                        cwd: params.cwd,
                        path: params.path,
                        data,
                        create_parent_directories: params.create_parent_directories,
                        mode,
                    })
                    .await?,
            )
        }
        Operation::RemoveFile(params) => value(
            runtime
                .remove_file(core::RemoveFileRequest {
                    mutation_id: params.mutation_id,
                    cwd: params.cwd,
                    path: params.path,
                    precondition: params.precondition,
                })
                .await?,
        ),
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

fn run_result(result: core::RunResult) -> core::Result<Value> {
    let output: Vec<_> = result
        .output
        .into_iter()
        .map(|chunk| json!({"stream": chunk.stream, "data_base64": STANDARD.encode(chunk.data)}))
        .collect();
    Ok(json!({
        "run_id": result.run_id,
        "execution": result.execution,
        "output_file": result.output_file,
        "output": output,
        "output_truncated": result.output_truncated,
    }))
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
