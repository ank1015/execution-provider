use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionHandle {
    pub id: Uuid,
    pub generation_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShellKind {
    Sh,
    Bash,
    Zsh,
    PowerShell,
    Cmd,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shell {
    pub executable: PathBuf,
    pub kind: ShellKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Program {
        executable: PathBuf,
        #[serde(default)]
        args: Vec<String>,
    },
    Shell {
        script: String,
        shell: Option<Shell>,
        #[serde(default)]
        login: bool,
    },
}

impl Command {
    pub fn program(
        executable: impl Into<PathBuf>,
        args: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::Program {
            executable: executable.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    pub fn shell(script: impl Into<String>) -> Self {
        Self::Shell {
            script: script.into(),
            shell: None,
            login: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IoMode {
    Pipes { stdin: bool },
    Pty { rows: u16, cols: u16 },
}

impl Default for IoMode {
    fn default() -> Self {
        Self::Pipes { stdin: false }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Starting,
    Running,
    Stopping,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum ExecutionResult {
    Exited {
        exit_code: Option<u32>,
        signal: Option<String>,
    },
    Terminated {
        exit_code: Option<u32>,
        signal: Option<String>,
    },
    StartFailed {
        message: String,
    },
    Lost {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub handle: ExecutionHandle,
    pub command: Command,
    pub resolved_shell: Option<Shell>,
    pub cwd: PathBuf,
    pub io: IoMode,
    pub labels: BTreeMap<String, String>,
    pub state: ExecutionState,
    pub result: Option<ExecutionResult>,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    /// True when a drain deadline or an I/O failure prevented complete capture.
    pub output_incomplete: bool,
    pub input_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartRequest {
    pub start_id: String,
    pub command: Command,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub io: IoMode,
    pub wait_ms: u64,
    pub max_output_bytes: Option<usize>,
    pub labels: BTreeMap<String, String>,
}

impl StartRequest {
    pub fn new(start_id: impl Into<String>, command: Command) -> Self {
        Self {
            start_id: start_id.into(),
            command,
            cwd: None,
            env: BTreeMap::new(),
            io: IoMode::default(),
            wait_ms: 0,
            max_output_bytes: None,
            labels: BTreeMap::new(),
        }
    }
}

/// An opaque position within one execution's retained output and lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub(crate) handle: ExecutionHandle,
    pub(crate) offset: u64,
    pub(crate) revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputChunk {
    pub stream: OutputStream,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WaitMode {
    #[default]
    Activity,
    FinishedOrTimeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserveRequest {
    pub handle: ExecutionHandle,
    pub after_cursor: Option<Cursor>,
    pub wait_ms: u64,
    pub return_when: WaitMode,
    pub max_output_bytes: Option<usize>,
}

impl ObserveRequest {
    pub fn new(handle: ExecutionHandle) -> Self {
        Self {
            handle,
            after_cursor: None,
            wait_ms: 0,
            return_when: WaitMode::Activity,
            max_output_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnReason {
    Activity,
    Finished,
    WaitElapsed,
    OutputLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub execution: Execution,
    pub output: Vec<OutputChunk>,
    pub next_cursor: Cursor,
    pub has_more: bool,
    pub output_gap: bool,
    pub return_reason: ReturnReason,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputReceipt {
    pub input_id: String,
    pub accepted_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StdinState {
    Open,
    Closing,
    Closed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StateFilter {
    #[default]
    Active,
    Finished,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageCursor {
    pub(crate) generation_id: Uuid,
    pub(crate) after: u64,
    pub(crate) through: u64,
    pub(crate) state: StateFilter,
    pub(crate) labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRequest {
    pub state: StateFilter,
    pub labels: BTreeMap<String, String>,
    pub limit: usize,
    pub page_cursor: Option<PageCursor>,
}

impl Default for ListRequest {
    fn default() -> Self {
        Self {
            state: StateFilter::Active,
            labels: BTreeMap::new(),
            limit: 50,
            page_cursor: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPage {
    pub executions: Vec<Execution>,
    pub next_page_cursor: Option<PageCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub generation_id: Uuid,
    pub default_shell: Shell,
    pub pty: bool,
    pub pipe_interrupt: bool,
    pub terminal_interrupt: bool,
}
