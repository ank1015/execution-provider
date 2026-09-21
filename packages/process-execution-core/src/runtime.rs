use crate::{
    backend,
    session::{self, OutputSpool, Session, StopReason},
    shell, shell_snapshot, *,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Cloneable access to one runtime generation. Dropping a request never cancels an accepted start.
#[derive(Clone)]
pub struct ProcessExecutionCore(pub(crate) Arc<Inner>);

pub(crate) struct Inner {
    pub(crate) config: Config,
    info: RuntimeInfo,
    registry: Mutex<Registry>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) file_mutations: tokio::sync::Mutex<FileMutationRegistry>,
    shell_snapshots: shell_snapshot::ShellSnapshotCache,
    configured_env: BTreeMap<String, String>,
}

struct Registry {
    entries: BTreeMap<u64, Entry>,
    handles: HashMap<Uuid, u64>,
    starts: HashMap<String, u64>,
    runs: HashMap<String, u64>,
    pending_run_terminations: HashMap<String, PendingRunTermination>,
    next_sequence: u64,
    shutting_down: bool,
}

struct Entry {
    start_request: Option<StartRequest>,
    run_request: Option<RunRequest>,
    session: Arc<Session>,
}

struct PendingRunTermination {
    created_at: Instant,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl ProcessExecutionCore {
    /// Must be called inside a Tokio runtime. Environment defaults are captured here.
    pub fn new(mut config: Config) -> Result<Self> {
        config.limits.validate()?;
        config.shell_snapshot.validate()?;
        if !config.cwd.is_absolute() {
            config.cwd = std::env::current_dir()?.join(&config.cwd);
        }
        if !config.cwd.is_dir() {
            return Err(Error::invalid("configured cwd is not a directory"));
        }
        let output_directory = config
            .run_output_directory
            .take()
            .unwrap_or_else(|| config.cwd.join(".process-execution-runs"));
        config.run_output_directory = Some(if output_directory.is_absolute() {
            output_directory
        } else {
            config.cwd.join(output_directory)
        });
        let default_shell = match &config.default_shell {
            Some(shell) => shell::resolve(shell)?,
            None => shell::discover()?,
        };
        validate_env(&config.env)?;
        let configured_env = config.env.clone();
        let mut env: BTreeMap<String, String> = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        merge_env(&mut env, config.env);
        config.env = env;
        let info = RuntimeInfo {
            generation_id: Uuid::new_v4(),
            default_shell,
            pty: true,
            pipe_interrupt: cfg!(unix),
            terminal_interrupt: true,
            filesystem: FileSystemCapabilities {
                max_read_bytes: config.limits.max_file_read_bytes,
                max_write_bytes: config.limits.max_file_write_bytes,
                conditional_mutations: true,
                atomic_replace: true,
                overwrite: true,
            },
            shell_snapshot: ShellSnapshotCapabilities {
                enabled: config.shell_snapshot.enabled,
                supported: cfg!(unix),
                max_scope_id_bytes: shell_snapshot::MAX_SCOPE_ID_BYTES,
            },
            execution_run: RunCapabilities {
                max_preview_bytes: config.limits.max_output_bytes_per_response,
                output_retention_ms: config.limits.run_output_retention.as_millis() as u64,
                terminate_by_run_id: true,
            },
        };
        let shell_snapshots =
            shell_snapshot::ShellSnapshotCache::new(config.shell_snapshot.clone());
        let core = Self(Arc::new(Inner {
            config,
            info,
            registry: Mutex::new(Registry {
                entries: BTreeMap::new(),
                handles: HashMap::new(),
                starts: HashMap::new(),
                runs: HashMap::new(),
                pending_run_terminations: HashMap::new(),
                next_sequence: 1,
                shutting_down: false,
            }),
            shutdown: CancellationToken::new(),
            file_mutations: tokio::sync::Mutex::new(FileMutationRegistry::default()),
            shell_snapshots,
            configured_env,
        }));
        let weak = Arc::downgrade(&core.0);
        let period = (core.0.config.limits.finished_retention / 2)
            .clamp(Duration::from_millis(10), Duration::from_secs(60));
        let shutdown = core.0.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! { () = tokio::time::sleep(period) => (), () = shutdown.cancelled() => break }
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                inner.registry.lock().unwrap().cleanup(
                    inner.config.limits.finished_retention,
                    inner.config.limits.run_output_retention,
                );
            }
        });
        Ok(core)
    }

    pub fn runtime_info(&self) -> RuntimeInfo {
        self.0.info.clone()
    }

    pub async fn start_execution(&self, request: StartRequest) -> Result<Observation> {
        self.output_limit(request.max_output_bytes)?;
        self.wait_limit(request.wait_ms)?;
        if request.start_id.is_empty() {
            return Err(Error::invalid("start_id is empty"));
        }
        if let IoMode::Pty { rows, cols } = request.io {
            validate_size(rows, cols)?;
        }
        validate_env(&request.env)?;
        if let Some(snapshot) = &request.shell_snapshot {
            shell_snapshot::validate_scope_id(&snapshot.scope_id)?;
        }
        let (mut executable, mut args, resolved_shell) =
            shell::prepare(&request.command, &self.0.info.default_shell)?;
        let cwd = match &request.cwd {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => self.0.config.cwd.join(path),
            None => self.0.config.cwd.clone(),
        };
        let mut env = self.0.config.env.clone();
        let selected_shell = resolved_shell
            .as_ref()
            .unwrap_or(&self.0.info.default_shell);
        let snapshot = if let Some(snapshot_request) = &request.shell_snapshot {
            self.0
                .shell_snapshots
                .get(
                    &snapshot_request.scope_id,
                    selected_shell,
                    &cwd,
                    &self.0.config.env,
                )
                .await
                .ok()
        } else {
            None
        };
        if let Some(snapshot) = &snapshot {
            merge_env(&mut env, snapshot.env.clone());
        }
        // Explicit runtime configuration and per-start values always beat profile values.
        merge_env(&mut env, self.0.configured_env.clone());
        merge_env(&mut env, request.env.clone());
        if let (Command::Shell { script, .. }, Some(snapshot)) = (&request.command, &snapshot) {
            let wrapped = shell_snapshot::install_state(&mut env, &snapshot.state, script);
            let selected = resolved_shell
                .as_ref()
                .expect("shell commands always resolve a shell");
            let prepared = shell::prepare_resolved(selected, &wrapped, false);
            executable = prepared.0;
            args = prepared.1;
        }
        let launch = backend::Launch {
            executable,
            args,
            cwd: cwd.clone(),
            env,
            io: request.io,
        };
        let (session, retry) = {
            let mut registry = self.0.registry.lock().unwrap();
            registry.cleanup(
                self.0.config.limits.finished_retention,
                self.0.config.limits.run_output_retention,
            );
            if let Some(sequence) = registry.starts.get(&request.start_id) {
                let entry = &registry.entries[sequence];
                if entry.start_request.as_ref() != Some(&request) {
                    return Err(Error::new(
                        ErrorCode::IdempotencyConflict,
                        "start_id was used for different arguments",
                    ));
                }
                (entry.session.clone(), true)
            } else {
                if registry.shutting_down {
                    return Err(Error::new(
                        ErrorCode::Unavailable,
                        "runtime is shutting down",
                    ));
                }
                let active = registry
                    .entries
                    .values()
                    .filter(|e| e.session.snapshot().state != ExecutionState::Finished)
                    .count();
                if active >= self.0.config.limits.max_active_executions
                    || registry.entries.len() >= self.0.config.limits.max_retained_executions
                {
                    return Err(Error::new(
                        ErrorCode::ResourceLimit,
                        "execution capacity reached",
                    ));
                }
                let execution = Execution {
                    handle: ExecutionHandle {
                        id: Uuid::new_v4(),
                        generation_id: self.0.info.generation_id,
                    },
                    command: request.command.clone(),
                    resolved_shell,
                    cwd,
                    io: request.io,
                    labels: request.labels.clone(),
                    state: ExecutionState::Starting,
                    result: None,
                    created_at: SystemTime::now(),
                    started_at: None,
                    finished_at: None,
                    output_incomplete: false,
                    input_error: None,
                };
                let session = Session::new(execution, self.0.config.limits.clone(), None);
                let sequence = registry.next_sequence;
                registry.next_sequence += 1;
                registry
                    .handles
                    .insert(session.snapshot().handle.id, sequence);
                registry.starts.insert(request.start_id.clone(), sequence);
                registry.entries.insert(
                    sequence,
                    Entry {
                        start_request: Some(request.clone()),
                        run_request: None,
                        session: session.clone(),
                    },
                );
                // No await between recording acceptance and handing ownership to the runtime.
                tokio::spawn(session::run(
                    session.clone(),
                    launch,
                    self.0.shutdown.clone(),
                ));
                (session, false)
            }
        };
        session.wait_launched().await;
        self.observe_session(
            session.clone(),
            ObserveRequest {
                handle: session.snapshot().handle,
                after_cursor: None,
                wait_ms: if retry { 0 } else { request.wait_ms },
                return_when: WaitMode::FinishedOrTimeout,
                max_output_bytes: request.max_output_bytes,
            },
        )
        .await
    }

    /// Run a non-interactive command through completion while spooling all captured output.
    pub async fn run_execution(&self, request: RunRequest) -> Result<RunResult> {
        let output_limit = self.output_limit(request.max_output_bytes)?;
        if request.run_id.is_empty() || request.run_id.len() > 256 {
            return Err(Error::invalid("run_id must contain 1 to 256 bytes"));
        }
        if request.timeout_ms == Some(0) {
            return Err(Error::invalid("timeout_ms must be positive when present"));
        }
        validate_env(&request.env)?;
        if let Some(snapshot) = &request.shell_snapshot {
            shell_snapshot::validate_scope_id(&snapshot.scope_id)?;
        }
        let (mut executable, mut args, resolved_shell) =
            shell::prepare(&request.command, &self.0.info.default_shell)?;
        let cwd = match &request.cwd {
            Some(path) if path.is_absolute() => path.clone(),
            Some(path) => self.0.config.cwd.join(path),
            None => self.0.config.cwd.clone(),
        };
        let mut env = self.0.config.env.clone();
        let selected_shell = resolved_shell
            .as_ref()
            .unwrap_or(&self.0.info.default_shell);
        let snapshot = if let Some(snapshot_request) = &request.shell_snapshot {
            self.0
                .shell_snapshots
                .get(
                    &snapshot_request.scope_id,
                    selected_shell,
                    &cwd,
                    &self.0.config.env,
                )
                .await
                .ok()
        } else {
            None
        };
        if let Some(snapshot) = &snapshot {
            merge_env(&mut env, snapshot.env.clone());
        }
        merge_env(&mut env, self.0.configured_env.clone());
        merge_env(&mut env, request.env.clone());
        if let (Command::Shell { script, .. }, Some(snapshot)) = (&request.command, &snapshot) {
            let wrapped = shell_snapshot::install_state(&mut env, &snapshot.state, script);
            let selected = resolved_shell
                .as_ref()
                .expect("shell commands always resolve a shell");
            let prepared = shell::prepare_resolved(selected, &wrapped, false);
            executable = prepared.0;
            args = prepared.1;
        }
        let launch = backend::Launch {
            executable,
            args,
            cwd: cwd.clone(),
            env,
            io: IoMode::Pipes { stdin: false },
        };
        let (session, retry, cancelled) = {
            let mut registry = self.0.registry.lock().unwrap();
            registry.cleanup(
                self.0.config.limits.finished_retention,
                self.0.config.limits.run_output_retention,
            );
            if let Some(sequence) = registry.runs.get(&request.run_id) {
                let entry = &registry.entries[sequence];
                if entry.run_request.as_ref() != Some(&request) {
                    return Err(Error::new(
                        ErrorCode::IdempotencyConflict,
                        "run_id was used for different arguments",
                    ));
                }
                (entry.session.clone(), true, None)
            } else {
                if registry.shutting_down {
                    return Err(Error::new(
                        ErrorCode::Unavailable,
                        "runtime is shutting down",
                    ));
                }
                let active = registry
                    .entries
                    .values()
                    .filter(|entry| entry.session.snapshot().state != ExecutionState::Finished)
                    .count();
                if active >= self.0.config.limits.max_active_executions
                    || registry.entries.len() >= self.0.config.limits.max_retained_executions
                {
                    return Err(Error::new(
                        ErrorCode::ResourceLimit,
                        "execution capacity reached",
                    ));
                }
                let spool = OutputSpool::create(
                    self.0
                        .config
                        .run_output_directory
                        .as_deref()
                        .expect("run output directory is resolved"),
                    self.0.config.limits.max_output_bytes_per_response,
                )?;
                let execution = Execution {
                    handle: ExecutionHandle {
                        id: Uuid::new_v4(),
                        generation_id: self.0.info.generation_id,
                    },
                    command: request.command.clone(),
                    resolved_shell,
                    cwd,
                    io: IoMode::Pipes { stdin: false },
                    labels: request.labels.clone(),
                    state: ExecutionState::Starting,
                    result: None,
                    created_at: SystemTime::now(),
                    started_at: None,
                    finished_at: None,
                    output_incomplete: false,
                    input_error: None,
                };
                let session = Session::new(execution, self.0.config.limits.clone(), Some(spool));
                let sequence = registry.next_sequence;
                registry.next_sequence += 1;
                registry
                    .handles
                    .insert(session.snapshot().handle.id, sequence);
                registry.runs.insert(request.run_id.clone(), sequence);
                let cancelled = registry.pending_run_terminations.remove(&request.run_id);
                registry.entries.insert(
                    sequence,
                    Entry {
                        start_request: None,
                        run_request: Some(request.clone()),
                        session: session.clone(),
                    },
                );
                (session, false, cancelled)
            }
        };
        if !retry {
            if cancelled.is_some() {
                session.finish(ExecutionResult::Terminated {
                    exit_code: None,
                    signal: None,
                });
            } else {
                tokio::spawn(session::run(
                    session.clone(),
                    launch,
                    self.0.shutdown.clone(),
                ));
                if let Some(timeout_ms) = request.timeout_ms {
                    let timed = session.clone();
                    let grace = self.0.config.limits.termination_grace;
                    tokio::spawn(async move {
                        timed.wait_launched().await;
                        if timed.snapshot().state == ExecutionState::Finished {
                            return;
                        }
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                                timed.terminate(grace, StopReason::Timeout);
                            }
                            () = timed.wait_finished() => {}
                        }
                    });
                }
            }
        }
        session.wait_finished().await;
        let finished_at = session
            .snapshot()
            .finished_at
            .unwrap_or_else(SystemTime::now);
        session.run_result(
            request.run_id,
            output_limit,
            finished_at + self.0.config.limits.run_output_retention,
        )
    }

    /// Terminate a run by stable identity. An early cancellation is retained for a racing run.
    pub async fn terminate_run(
        &self,
        run_id: impl Into<String>,
        grace: Option<Duration>,
    ) -> Result<TerminateRunReceipt> {
        let run_id = run_id.into();
        if run_id.is_empty() || run_id.len() > 256 {
            return Err(Error::invalid("run_id must contain 1 to 256 bytes"));
        }
        let grace = grace.unwrap_or(self.0.config.limits.termination_grace);
        if grace > self.0.config.limits.max_termination_grace {
            return Err(Error::invalid("termination grace exceeds maximum"));
        }
        let session = {
            let mut registry = self.0.registry.lock().unwrap();
            registry.cleanup(
                self.0.config.limits.finished_retention,
                self.0.config.limits.run_output_retention,
            );
            registry
                .runs
                .get(&run_id)
                .map(|sequence| registry.entries[sequence].session.clone())
        };
        let Some(session) = session else {
            let mut registry = self.0.registry.lock().unwrap();
            if registry.pending_run_terminations.len()
                >= self.0.config.limits.max_retained_executions
            {
                return Err(Error::new(
                    ErrorCode::ResourceLimit,
                    "pending run termination capacity reached",
                ));
            }
            registry
                .pending_run_terminations
                .entry(run_id.clone())
                .or_insert(PendingRunTermination {
                    created_at: Instant::now(),
                });
            return Ok(TerminateRunReceipt {
                run_id,
                state: TerminateRunState::Pending,
                execution: None,
            });
        };
        let execution = session.snapshot();
        if execution.state == ExecutionState::Finished {
            return Ok(TerminateRunReceipt {
                run_id,
                state: TerminateRunState::Finished,
                execution: Some(execution),
            });
        }
        let execution = session.terminate(grace, StopReason::ExplicitTermination);
        Ok(TerminateRunReceipt {
            run_id,
            state: TerminateRunState::Terminating,
            execution: Some(execution),
        })
    }

    pub async fn get_execution(&self, handle: ExecutionHandle) -> Result<Execution> {
        Ok(self.session(handle)?.snapshot())
    }

    pub async fn observe_execution(&self, request: ObserveRequest) -> Result<Observation> {
        self.observe_session(self.session(request.handle)?, request)
            .await
    }

    async fn observe_session(
        &self,
        session: Arc<Session>,
        request: ObserveRequest,
    ) -> Result<Observation> {
        let limit = self.output_limit(request.max_output_bytes)?;
        self.wait_limit(request.wait_ms)?;
        let deadline = Instant::now() + Duration::from_millis(request.wait_ms);
        loop {
            let changed = session.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut observation = {
                let data = session.data.lock().unwrap();
                data.journal.read(&data.execution, &request, limit)?
            };
            let immediate = matches!(
                observation.return_reason,
                ReturnReason::Finished | ReturnReason::OutputLimit
            ) || (request.return_when == WaitMode::Activity
                && observation.return_reason == ReturnReason::Activity);
            if immediate {
                return Ok(observation);
            }
            if Instant::now() >= deadline {
                observation.return_reason = ReturnReason::WaitElapsed;
                return Ok(observation);
            }
            let _ = tokio::time::timeout_at(deadline, changed).await;
        }
    }

    pub async fn write_input(
        &self,
        handle: ExecutionHandle,
        input_id: impl Into<String>,
        data: Vec<u8>,
    ) -> Result<InputReceipt> {
        self.session(handle)?.write(input_id.into(), data)
    }

    pub async fn close_input(&self, handle: ExecutionHandle) -> Result<StdinState> {
        self.session(handle)?.close_input()
    }

    pub async fn interrupt_execution(
        &self,
        handle: ExecutionHandle,
        operation_id: impl Into<String>,
    ) -> Result<Execution> {
        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(Error::invalid("operation_id is empty"));
        }
        let session = self.session(handle)?;
        let mut data = session.data.lock().unwrap();
        if let Some(result) = data.interrupt_receipts.get(&operation_id) {
            return result.clone();
        }
        if data.execution.state == ExecutionState::Finished {
            return Ok(data.execution.clone());
        }
        if data.execution.state != ExecutionState::Running {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "execution is not running",
            ));
        }
        if data.interrupt_receipts.len() >= session.limits.max_interrupt_receipts {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "interrupt receipt limit reached",
            ));
        }
        let result = if cfg!(windows) && matches!(data.execution.io, IoMode::Pty { .. }) {
            session.enqueue(&data, vec![3])
        } else {
            data.control.as_ref().unwrap().interrupt()
        }
        .map(|()| data.execution.clone());
        data.interrupt_receipts.insert(operation_id, result.clone());
        result
    }

    /// Repeated calls preserve the first stop deadline; the returned snapshot is an acknowledgement.
    pub async fn terminate_execution(
        &self,
        handle: ExecutionHandle,
        grace: Option<Duration>,
    ) -> Result<Execution> {
        let grace = grace.unwrap_or(self.0.config.limits.termination_grace);
        if grace > self.0.config.limits.max_termination_grace {
            return Err(Error::invalid("termination grace exceeds maximum"));
        }
        Ok(self
            .session(handle)?
            .terminate(grace, StopReason::ExplicitTermination))
    }

    pub async fn resize_terminal(
        &self,
        handle: ExecutionHandle,
        rows: u16,
        cols: u16,
    ) -> Result<Execution> {
        validate_size(rows, cols)?;
        let session = self.session(handle)?;
        let mut data = session.data.lock().unwrap();
        if data.execution.state != ExecutionState::Running {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "execution is not running",
            ));
        }
        data.control.as_ref().unwrap().resize(rows, cols)?;
        data.execution.io = IoMode::Pty { rows, cols };
        data.journal.revision += 1;
        let execution = data.execution.clone();
        drop(data);
        session.changed.notify_waiters();
        Ok(execution)
    }

    pub async fn list_executions(&self, request: ListRequest) -> Result<ExecutionPage> {
        if request.limit == 0 {
            return Err(Error::invalid("list limit must be positive"));
        }
        let mut registry = self.0.registry.lock().unwrap();
        registry.cleanup(
            self.0.config.limits.finished_retention,
            self.0.config.limits.run_output_retention,
        );
        let (after, through) = if let Some(cursor) = &request.page_cursor {
            self.generation(cursor.generation_id)?;
            if cursor.state != request.state || cursor.labels != request.labels {
                return Err(Error::invalid("list filters changed between pages"));
            }
            (cursor.after, cursor.through)
        } else {
            (0, registry.next_sequence - 1)
        };
        if after > through || through >= registry.next_sequence {
            return Err(Error::invalid(
                "page cursor is ahead of this runtime's execution list",
            ));
        }
        let mut executions = Vec::new();
        let mut last = after;
        let mut more = false;
        for (sequence, entry) in registry.entries.range((
            std::ops::Bound::Excluded(after),
            std::ops::Bound::Included(through),
        )) {
            let execution = entry.session.snapshot();
            let finished = execution.state == ExecutionState::Finished;
            if matches!(request.state, StateFilter::Active) && finished
                || matches!(request.state, StateFilter::Finished) && !finished
            {
                continue;
            }
            if !request
                .labels
                .iter()
                .all(|(key, value)| execution.labels.get(key) == Some(value))
            {
                continue;
            }
            if executions.len() == request.limit {
                more = true;
                break;
            }
            last = *sequence;
            executions.push(execution);
        }
        let next_page_cursor = more.then(|| PageCursor {
            generation_id: self.0.info.generation_id,
            after: last,
            through,
            state: request.state,
            labels: request.labels,
        });
        Ok(ExecutionPage {
            executions,
            next_page_cursor,
        })
    }

    /// Reject new starts, terminate all accepted work, and wait for its output to finalize.
    pub async fn shutdown(&self) -> Result<()> {
        let sessions: Vec<_> = {
            let mut registry = self.0.registry.lock().unwrap();
            registry.shutting_down = true;
            registry
                .entries
                .values()
                .map(|e| e.session.clone())
                .collect()
        };
        self.0.shutdown.cancel();
        for session in &sessions {
            session.terminate(
                self.0.config.limits.termination_grace,
                StopReason::RuntimeShutdown,
            );
        }
        for session in sessions {
            session.wait_finished().await;
        }
        Ok(())
    }

    fn session(&self, handle: ExecutionHandle) -> Result<Arc<Session>> {
        self.generation(handle.generation_id)?;
        let mut registry = self.0.registry.lock().unwrap();
        registry.cleanup(
            self.0.config.limits.finished_retention,
            self.0.config.limits.run_output_retention,
        );
        let sequence = registry.handles.get(&handle.id).ok_or_else(|| {
            Error::new(
                ErrorCode::NotFound,
                "execution was not found or its retention ended",
            )
        })?;
        Ok(registry.entries[sequence].session.clone())
    }

    fn generation(&self, generation: Uuid) -> Result<()> {
        if generation != self.0.info.generation_id {
            return Err(Error::new(
                ErrorCode::GenerationMismatch,
                "execution belongs to another runtime generation",
            ));
        }
        Ok(())
    }

    fn wait_limit(&self, wait_ms: u64) -> Result<()> {
        if Duration::from_millis(wait_ms) > self.0.config.limits.max_wait {
            return Err(Error::invalid("wait exceeds configured maximum"));
        }
        Ok(())
    }

    fn output_limit(&self, requested: Option<usize>) -> Result<usize> {
        let max = self.0.config.limits.max_output_bytes_per_response;
        let limit = requested.unwrap_or(max);
        if limit == 0 || limit > max {
            return Err(Error::invalid(
                "output limit must be positive and within the configured maximum",
            ));
        }
        Ok(limit)
    }
}

impl Registry {
    fn cleanup(&mut self, retention: Duration, run_retention: Duration) {
        self.pending_run_terminations
            .retain(|_, value| value.created_at.elapsed() < run_retention);
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(sequence, entry)| {
                let retention = if entry.run_request.is_some() {
                    run_retention
                } else {
                    retention
                };
                entry
                    .session
                    .data
                    .lock()
                    .unwrap()
                    .finished_at
                    .filter(|at| at.elapsed() >= retention)
                    .map(|_| *sequence)
            })
            .collect();
        for sequence in expired {
            let entry = self.entries.remove(&sequence).unwrap();
            self.handles.remove(&entry.session.snapshot().handle.id);
            if let Some(request) = entry.start_request {
                self.starts.remove(&request.start_id);
            }
            if let Some(request) = entry.run_request {
                self.runs.remove(&request.run_id);
                entry.session.remove_output_file();
            }
        }
    }
}

fn validate_env(env: &BTreeMap<String, String>) -> Result<()> {
    for (key, value) in env {
        if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
            return Err(Error::invalid("invalid environment key or value"));
        }
    }
    Ok(())
}

fn merge_env(target: &mut BTreeMap<String, String>, overrides: BTreeMap<String, String>) {
    for (key, value) in overrides {
        if cfg!(windows) {
            target.retain(|existing, _| !existing.eq_ignore_ascii_case(&key));
        }
        target.insert(key, value);
    }
}

fn validate_size(rows: u16, cols: u16) -> Result<()> {
    if rows == 0 || cols == 0 || rows > i16::MAX as u16 || cols > i16::MAX as u16 {
        return Err(Error::invalid(
            "terminal dimensions must be between 1 and 32767",
        ));
    }
    Ok(())
}
