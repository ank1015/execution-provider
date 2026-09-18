use crate::{backend, journal::Journal, *};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinSet,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Session {
    pub data: Mutex<Data>,
    pub changed: Notify,
    pub limits: Limits,
    pub input_capacity: Arc<Semaphore>,
    pub stop: Notify,
}

pub(crate) struct Data {
    pub execution: Execution,
    pub journal: Journal,
    pub launched: bool,
    pub root_exited: bool,
    pub finished_at: Option<Instant>,
    pub stop_deadline: Option<Instant>,
    pub stop_reason: Option<StopReason>,
    pub control: Option<Arc<dyn backend::Control>>,
    pub stdin: StdinState,
    pub input_tx: Option<mpsc::UnboundedSender<Input>>,
    pub input_receipts: HashMap<String, ([u8; 32], InputReceipt)>,
    pub interrupt_receipts: HashMap<String, Result<Execution>>,
    output_spool: Option<OutputSpool>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StopReason {
    ExplicitTermination,
    Timeout,
    RuntimeShutdown,
}

pub(crate) struct OutputSpool {
    artifact_id: uuid::Uuid,
    path: PathBuf,
    file: Option<File>,
    hasher: Sha256,
    sha256: Option<String>,
    size_bytes: u64,
    captured_bytes: u64,
    tail: VecDeque<OutputChunk>,
    tail_bytes: usize,
    tail_capacity: usize,
    error: Option<String>,
}

impl OutputSpool {
    pub fn create(directory: &Path, tail_capacity: usize) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700).create(directory)?;
        }
        #[cfg(windows)]
        std::fs::create_dir_all(directory)?;
        let artifact_id = uuid::Uuid::new_v4();
        let path = directory.join(format!("{artifact_id}.log"));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&path)?;
        Ok(Self {
            artifact_id,
            path,
            file: Some(file),
            hasher: Sha256::new(),
            sha256: None,
            size_bytes: 0,
            captured_bytes: 0,
            tail: VecDeque::new(),
            tail_bytes: 0,
            tail_capacity,
            error: None,
        })
    }

    fn append(&mut self, stream: OutputStream, data: &[u8]) {
        self.captured_bytes = self.captured_bytes.saturating_add(data.len() as u64);
        self.append_tail(stream, data);
        let Some(file) = self.file.as_mut() else {
            return;
        };
        let mut remaining = data;
        while !remaining.is_empty() {
            match file.write(remaining) {
                Ok(0) => {
                    self.fail("output file write returned zero bytes".into());
                    break;
                }
                Ok(count) => {
                    self.hasher.update(&remaining[..count]);
                    self.size_bytes += count as u64;
                    remaining = &remaining[count..];
                }
                Err(error) => {
                    self.fail(error.to_string());
                    break;
                }
            }
        }
    }

    fn append_tail(&mut self, stream: OutputStream, data: &[u8]) {
        if self.tail_capacity == 0 {
            return;
        }
        self.tail.push_back(OutputChunk {
            stream,
            data: data.to_vec(),
        });
        self.tail_bytes += data.len();
        while self.tail_bytes > self.tail_capacity {
            let discard = self.tail_bytes - self.tail_capacity;
            let front = self.tail.front_mut().expect("tail is not empty");
            let count = discard.min(front.data.len());
            front.data.drain(..count);
            self.tail_bytes -= count;
            if front.data.is_empty() {
                self.tail.pop_front();
            }
        }
    }

    fn fail(&mut self, message: String) {
        self.error.get_or_insert(message);
        self.file = None;
    }

    fn finalize(&mut self) {
        if let Some(mut file) = self.file.take()
            && let Err(error) = file.flush().and_then(|()| file.sync_data())
        {
            self.error.get_or_insert(error.to_string());
        }
        self.sha256 = Some(hex_digest(self.hasher.clone().finalize().as_slice()));
    }

    fn preview(&self, limit: usize) -> Vec<OutputChunk> {
        let mut skip = self.tail_bytes.saturating_sub(limit);
        let mut output = Vec::new();
        for chunk in &self.tail {
            if skip >= chunk.data.len() {
                skip -= chunk.data.len();
                continue;
            }
            output.push(OutputChunk {
                stream: chunk.stream,
                data: chunk.data[skip..].to_vec(),
            });
            skip = 0;
        }
        output
    }

    fn output_file(&self, complete: bool, expires_at: SystemTime) -> RunOutputFile {
        RunOutputFile {
            artifact_id: self.artifact_id,
            path: self.path.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256.clone().unwrap_or_default(),
            complete: complete && self.error.is_none(),
            expires_at,
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) enum Input {
    Bytes {
        data: Vec<u8>,
        _permit: OwnedSemaphorePermit,
    },
    Close,
}

impl Session {
    pub fn new(
        execution: Execution,
        limits: Limits,
        output_spool: Option<OutputSpool>,
    ) -> Arc<Self> {
        let stdin = match execution.io {
            IoMode::Pipes { stdin: false } => StdinState::Closed,
            _ => StdinState::Open,
        };
        Arc::new(Self {
            data: Mutex::new(Data {
                execution,
                journal: Journal::new(limits.max_retained_output_bytes),
                launched: false,
                root_exited: false,
                finished_at: None,
                stop_deadline: None,
                stop_reason: None,
                control: None,
                stdin,
                input_tx: None,
                input_receipts: HashMap::new(),
                interrupt_receipts: HashMap::new(),
                output_spool,
            }),
            changed: Notify::new(),
            input_capacity: Arc::new(Semaphore::new(limits.max_queued_input_bytes)),
            stop: Notify::new(),
            limits,
        })
    }

    pub fn snapshot(&self) -> Execution {
        self.data.lock().unwrap().execution.clone()
    }

    pub async fn wait_launched(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.data.lock().unwrap().launched {
                return;
            }
            changed.await;
        }
    }

    pub async fn wait_finished(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.snapshot().state == ExecutionState::Finished {
                return;
            }
            changed.await;
        }
    }

    pub fn terminate(&self, grace: Duration, reason: StopReason) -> Execution {
        let mut data = self.data.lock().unwrap();
        if data.execution.state != ExecutionState::Finished
            && !data.root_exited
            && data.stop_deadline.is_none()
        {
            data.stop_deadline = Some(Instant::now() + grace);
            data.stop_reason = Some(reason);
            data.execution.state = ExecutionState::Stopping;
            data.journal.revision += 1;
        }
        let snapshot = data.execution.clone();
        drop(data);
        self.stop.notify_one();
        self.changed.notify_waiters();
        snapshot
    }

    pub fn run_result(
        &self,
        run_id: String,
        output_limit: usize,
        expires_at: SystemTime,
    ) -> Result<RunResult> {
        let data = self.data.lock().unwrap();
        if data.execution.state != ExecutionState::Finished {
            return Err(Error::new(ErrorCode::InvalidState, "run is not finished"));
        }
        let spool = data
            .output_spool
            .as_ref()
            .ok_or_else(|| Error::new(ErrorCode::InvalidState, "run has no output file"))?;
        let output = spool.preview(output_limit);
        let preview_bytes = output.iter().map(|chunk| chunk.data.len() as u64).sum();
        let complete = !data.execution.output_incomplete;
        Ok(RunResult {
            run_id,
            execution: data.execution.clone(),
            output_file: spool.output_file(complete, expires_at),
            output,
            output_truncated: !complete || spool.captured_bytes > preview_bytes,
        })
    }

    pub fn remove_output_file(&self) {
        let path = self
            .data
            .lock()
            .unwrap()
            .output_spool
            .as_ref()
            .map(|spool| spool.path.clone());
        if let Some(path) = path {
            let _ = std::fs::remove_file(path);
        }
    }

    pub fn write(&self, input_id: String, bytes: Vec<u8>) -> Result<InputReceipt> {
        if input_id.is_empty() {
            return Err(Error::invalid("input_id is empty"));
        }
        let hash: [u8; 32] = Sha256::digest(&bytes).into();
        let mut data = self.data.lock().unwrap();
        if let Some((previous_hash, receipt)) = data.input_receipts.get(&input_id) {
            if *previous_hash != hash {
                return Err(Error::new(
                    ErrorCode::IdempotencyConflict,
                    "input_id was used for different bytes",
                ));
            }
            return Ok(receipt.clone());
        }
        let receipt = InputReceipt {
            input_id: input_id.clone(),
            accepted_bytes: bytes.len(),
        };
        if bytes.is_empty() {
            if data.input_receipts.len() >= self.limits.max_input_receipts {
                return Err(Error::new(
                    ErrorCode::ResourceLimit,
                    "input receipt limit reached",
                ));
            }
            data.input_receipts
                .insert(input_id, (hash, receipt.clone()));
            return Ok(receipt);
        }
        if data.execution.state != ExecutionState::Running {
            return Err(Error::new(
                ErrorCode::InvalidState,
                "execution is not running",
            ));
        }
        if data.stdin != StdinState::Open {
            return Err(Error::new(
                ErrorCode::StdinClosed,
                "stdin is closing or closed",
            ));
        }
        if data.input_receipts.len() >= self.limits.max_input_receipts {
            return Err(Error::new(
                ErrorCode::ResourceLimit,
                "input receipt limit reached",
            ));
        }
        self.enqueue(&data, bytes)?;
        data.input_receipts
            .insert(input_id, (hash, receipt.clone()));
        Ok(receipt)
    }

    pub fn enqueue(&self, data: &Data, bytes: Vec<u8>) -> Result<()> {
        let count = u32::try_from(bytes.len())
            .map_err(|_| Error::new(ErrorCode::ResourceLimit, "input exceeds queue capacity"))?;
        let permit = self
            .input_capacity
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| Error::new(ErrorCode::ResourceLimit, "input queue is full"))?;
        data.input_tx
            .as_ref()
            .ok_or_else(|| Error::new(ErrorCode::StdinClosed, "stdin is closed"))?
            .send(Input::Bytes {
                data: bytes,
                _permit: permit,
            })
            .map_err(|_| Error::new(ErrorCode::StdinClosed, "stdin is closed"))
    }

    pub fn close_input(&self) -> Result<StdinState> {
        let mut data = self.data.lock().unwrap();
        if matches!(data.execution.io, IoMode::Pty { .. }) {
            return Err(Error::new(
                ErrorCode::UnsupportedOperation,
                "a terminal does not support pipe-style EOF",
            ));
        }
        if data.stdin == StdinState::Open {
            data.input_tx
                .as_ref()
                .ok_or_else(|| Error::new(ErrorCode::StdinClosed, "stdin is unavailable"))?
                .send(Input::Close)
                .map_err(|_| Error::new(ErrorCode::StdinClosed, "stdin is closed"))?;
            data.stdin = StdinState::Closing;
        }
        Ok(data.stdin)
    }

    pub(crate) fn finish(&self, result: ExecutionResult) {
        let mut data = self.data.lock().unwrap();
        if let Some(spool) = data.output_spool.as_mut() {
            spool.finalize();
            if spool.error.is_some() {
                data.execution.output_incomplete = true;
            }
        }
        data.execution.state = ExecutionState::Finished;
        data.execution.result = Some(result);
        data.execution.finished_at = Some(SystemTime::now());
        data.finished_at = Some(Instant::now());
        data.launched = true;
        data.stdin = StdinState::Closed;
        data.input_tx = None;
        data.control = None;
        data.journal.revision += 1;
        drop(data);
        self.changed.notify_waiters();
    }
}

pub(crate) async fn run(
    session: Arc<Session>,
    launch: backend::Launch,
    shutdown: CancellationToken,
) {
    let process = match backend::spawn(launch) {
        Ok(process) => process,
        Err(error) => {
            session.finish(ExecutionResult::StartFailed {
                message: error.message,
            });
            return;
        }
    };
    let backend::Process {
        control,
        readers,
        writer,
        mut wait,
    } = process;
    let (input_tx, input_rx) = mpsc::unbounded_channel();
    {
        let mut data = session.data.lock().unwrap();
        data.control = Some(control.clone());
        data.input_tx = writer.as_ref().map(|_| input_tx);
        data.launched = true;
        if data.stop_deadline.is_none() {
            data.execution.state = ExecutionState::Running;
        }
        data.execution.started_at = Some(SystemTime::now());
        data.journal.revision += 1;
    }
    session.changed.notify_waiters();

    let mut output = JoinSet::new();
    for (stream, reader) in readers {
        output.spawn(read_output(session.clone(), stream, reader));
    }
    let input_task =
        writer.map(|writer| tokio::spawn(write_input(session.clone(), writer, input_rx)));
    let mut stopping = false;
    let mut killed = false;
    let mut shutdown_seen = false;
    let mut failure = None;
    let exit = loop {
        let stop_deadline = session.data.lock().unwrap().stop_deadline;
        if let Some(deadline) = stop_deadline {
            if !stopping {
                stopping = true;
                let terminal_interrupt =
                    cfg!(windows) && matches!(session.snapshot().io, IoMode::Pty { .. });
                let graceful = if terminal_interrupt {
                    session
                        .enqueue(&session.data.lock().unwrap(), vec![3])
                        .map(|()| true)
                } else {
                    control.terminate()
                };
                match graceful {
                    Ok(true) => (),
                    Ok(false) => {
                        failure = control.kill().err();
                        killed = true;
                    }
                    Err(error) => {
                        failure = Some(error);
                        let _ = control.kill();
                        killed = true;
                    }
                }
            }
            if !killed && Instant::now() >= deadline {
                if let Err(error) = control.kill() {
                    failure = Some(error);
                }
                killed = true;
            }
        }
        tokio::select! {
            status = &mut wait => break status,
            () = shutdown.cancelled(), if !shutdown_seen => {
                shutdown_seen = true;
                session.terminate(session.limits.termination_grace, StopReason::RuntimeShutdown);
            }
            () = session.stop.notified() => (),
            () = async { tokio::time::sleep_until(stop_deadline.unwrap()).await }, if stop_deadline.is_some() && !killed => (),
        }
    };
    // Stop accepting input as soon as the direct child exits, including during output draining.
    {
        let mut data = session.data.lock().unwrap();
        data.root_exited = true;
        data.execution.state = ExecutionState::Stopping;
        data.journal.revision += 1;
        data.stdin = StdinState::Closed;
        data.input_tx = None;
    }
    session.changed.notify_waiters();
    if let Some(task) = input_task {
        task.abort();
        let _ = task.await;
    }

    // A child exiting during its grace period does not excuse surviving descendants.
    if stopping && !killed {
        let deadline = session.data.lock().unwrap().stop_deadline.unwrap();
        tokio::time::sleep_until(deadline).await;
    }
    // After the root exits, clean up remaining owned work before finalizing output.
    if let Err(error) = control.kill() {
        failure = Some(error);
    }
    session.data.lock().unwrap().control = None;
    // Closing ConPTY can wait for its output consumer, so close it off the async
    // executor while the reader tasks are still draining.
    let release = tokio::task::spawn_blocking(move || drop(control));
    let drained = tokio::time::timeout(session.limits.output_drain_timeout, async {
        while let Some(result) = output.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                session.data.lock().unwrap().execution.output_incomplete = true;
            }
        }
    })
    .await;
    if drained.is_err() {
        session.data.lock().unwrap().execution.output_incomplete = true;
        output.abort_all();
        while output.join_next().await.is_some() {}
    }
    if let Err(error) = release.await {
        failure = Some(Error::new(ErrorCode::Io, error.to_string()));
    }
    let stop_reason = session.data.lock().unwrap().stop_reason;
    let result = match (exit, failure) {
        (_, Some(error)) => ExecutionResult::Lost {
            message: error.message,
        },
        (Err(error), _) => ExecutionResult::Lost {
            message: error.to_string(),
        },
        (Ok(status), _) if stopping && stop_reason == Some(StopReason::Timeout) => {
            ExecutionResult::TimedOut {
                exit_code: status.code,
                signal: status.signal,
            }
        }
        (Ok(status), _) if stopping => ExecutionResult::Terminated {
            exit_code: status.code,
            signal: status.signal,
        },
        (Ok(status), _) => ExecutionResult::Exited {
            exit_code: status.code,
            signal: status.signal,
        },
    };
    session.finish(result);
}

async fn read_output(
    session: Arc<Session>,
    stream: OutputStream,
    mut reader: backend::Reader,
) -> std::io::Result<()> {
    let mut buffer = vec![0u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        {
            let mut data = session.data.lock().unwrap();
            data.journal.append(stream, buffer[..count].to_vec());
            if let Some(spool) = data.output_spool.as_mut() {
                spool.append(stream, &buffer[..count]);
                if spool.error.is_some() {
                    data.execution.output_incomplete = true;
                }
            }
        }
        session.changed.notify_waiters();
        tokio::task::yield_now().await;
    }
}

async fn write_input(
    session: Arc<Session>,
    mut writer: backend::Writer,
    mut queue: mpsc::UnboundedReceiver<Input>,
) {
    let result: std::io::Result<()> = async {
        while let Some(input) = queue.recv().await {
            match input {
                Input::Bytes { data, _permit } => {
                    writer.write_all(&data).await?;
                    writer.flush().await?;
                }
                Input::Close => {
                    writer.shutdown().await?;
                    break;
                }
            }
        }
        Ok(())
    }
    .await;
    let mut data = session.data.lock().unwrap();
    data.stdin = StdinState::Closed;
    data.input_tx = None;
    if let Err(error) = result {
        data.execution.input_error = Some(error.to_string());
        data.journal.revision += 1;
    }
    drop(data);
    session.changed.notify_waiters();
}
