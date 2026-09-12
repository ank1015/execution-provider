use process_execution_core::*;
use std::{path::PathBuf, sync::OnceLock, time::Duration};

fn fixture() -> &'static PathBuf {
    static FIXTURE: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &FIXTURE
        .get_or_init(|| {
            let directory = tempfile::tempdir().unwrap();
            let executable =
                directory
                    .path()
                    .join(if cfg!(windows) { "child.exe" } else { "child" });
            let status = std::process::Command::new("rustc")
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/child.rs"
                ))
                .arg("-o")
                .arg(&executable)
                .status()
                .unwrap();
            assert!(status.success());
            (directory, executable)
        })
        .1
}

fn config() -> Config {
    let mut config = Config::new(std::env::temp_dir());
    config.limits.termination_grace = Duration::from_millis(30);
    config.limits.output_drain_timeout = Duration::from_millis(100);
    config
}

fn command(args: &[&str]) -> Command {
    Command::program(fixture(), args.iter().copied())
}
fn request(id: &str, args: &[&str]) -> StartRequest {
    StartRequest::new(id, command(args))
}
fn bytes(observation: &Observation) -> Vec<u8> {
    observation
        .output
        .iter()
        .flat_map(|chunk| chunk.data.clone())
        .collect()
}

async fn finish(core: &ProcessExecutionCore, handle: ExecutionHandle) -> Observation {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut request = ObserveRequest::new(handle);
        request.wait_ms = 5000;
        request.return_when = WaitMode::FinishedOrTimeout;
        loop {
            let observation = core.observe_execution(request.clone()).await.unwrap();
            if observation.execution.state == ExecutionState::Finished {
                return observation;
            }
            request.after_cursor = Some(observation.next_cursor);
        }
    })
    .await
    .expect("execution did not finish")
}

async fn wait_for(core: &ProcessExecutionCore, handle: ExecutionHandle, needle: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut request = ObserveRequest::new(handle);
        request.wait_ms = 1000;
        let mut output = Vec::new();
        loop {
            let observation = core.observe_execution(request.clone()).await.unwrap();
            output.extend(bytes(&observation));
            if String::from_utf8_lossy(&output).contains(needle) {
                return;
            }
            assert_ne!(
                observation.execution.state,
                ExecutionState::Finished,
                "missing {needle:?}: {output:?}"
            );
            request.after_cursor = Some(observation.next_cursor);
        }
    })
    .await
    .expect("expected output was not received");
}

#[tokio::test]
async fn direct_execution_preserves_arguments_context_and_streams() {
    let directory = tempfile::tempdir().unwrap();
    let core = ProcessExecutionCore::new(config()).unwrap();
    let mut start = request(
        "context",
        &[
            "context",
            "space and $literal",
            "a\"b",
            "ends\\",
            "",
            "Unicode λ",
        ],
    );
    start.cwd = Some(directory.path().to_path_buf());
    start.env.insert("PROCESS_CORE_TEST".into(), "hello".into());
    start.wait_ms = 1000;
    let result = core.start_execution(start).await.unwrap();
    let text = String::from_utf8(bytes(&result)).unwrap();
    assert!(text.contains("env=hello"));
    assert!(text.contains("arg=space and $literal"));
    assert!(text.contains("arg=a\"b"));
    assert!(text.contains("arg=ends\\"));
    assert!(text.contains("arg=Unicode λ"));
    assert!(text.contains(directory.path().file_name().unwrap().to_str().unwrap()));
    assert_eq!(result.execution.state, ExecutionState::Finished);
    assert!(result.execution.resolved_shell.is_none());
    let mut start = request("streams", &["echo", "hello"]);
    start.wait_ms = 1000;
    let result = core.start_execution(start).await.unwrap();
    assert!(
        result
            .output
            .iter()
            .any(|c| c.stream == OutputStream::Stdout && c.data.starts_with(b"hello"))
    );
    assert!(
        result
            .output
            .iter()
            .any(|c| c.stream == OutputStream::Stderr)
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn yield_returns_while_process_keeps_running_and_shutdown_finishes_it() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let mut start = request("server", &["sleep"]);
    start.wait_ms = 50;
    let result = core.start_execution(start).await.unwrap();
    assert_eq!(result.execution.state, ExecutionState::Running);
    assert_eq!(result.return_reason, ReturnReason::WaitElapsed);
    core.shutdown().await.unwrap();
    assert_eq!(
        core.get_execution(result.execution.handle)
            .await
            .unwrap()
            .state,
        ExecutionState::Finished
    );
    assert_eq!(
        core.start_execution(request("new", &["sleep"]))
            .await
            .unwrap_err()
            .code,
        ErrorCode::Unavailable
    );
}

#[tokio::test]
async fn output_cursors_replay_and_split_binary_data_without_consuming_other_readers() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let started = core
        .start_execution(request("binary", &["bytes", "4097"]))
        .await
        .unwrap();
    finish(&core, started.execution.handle).await;
    let mut request = ObserveRequest::new(started.execution.handle);
    request.max_output_bytes = Some(31);
    let first = core.observe_execution(request.clone()).await.unwrap();
    let replay = core.observe_execution(request.clone()).await.unwrap();
    assert_eq!(bytes(&first), bytes(&replay));
    assert!(first.has_more);
    assert_eq!(first.execution.state, ExecutionState::Finished);
    let mut collected = Vec::new();
    loop {
        let result = core.observe_execution(request.clone()).await.unwrap();
        collected.extend(bytes(&result));
        if !result.has_more {
            break;
        }
        request.after_cursor = Some(result.next_cursor);
    }
    assert_eq!(
        collected,
        (0..4097).map(|i| (i % 256) as u8).collect::<Vec<_>>()
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn discarded_output_reports_a_gap() {
    let mut config = config();
    config.limits.max_retained_output_bytes = 37;
    let core = ProcessExecutionCore::new(config).unwrap();
    let started = core
        .start_execution(request("binary", &["bytes", "4097"]))
        .await
        .unwrap();
    finish(&core, started.execution.handle).await;
    let result = core
        .observe_execution(ObserveRequest::new(started.execution.handle))
        .await
        .unwrap();
    assert!(result.output_gap);
    assert_eq!(
        bytes(&result),
        (4060..4097).map(|i| (i % 256) as u8).collect::<Vec<_>>()
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn writable_pipes_deduplicate_input_and_deliver_eof_after_queued_bytes() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let mut start = request("copy", &["copy"]);
    start.io = IoMode::Pipes { stdin: true };
    let handle = core.start_execution(start).await.unwrap().execution.handle;
    let data = vec![0, 255, b'a', b'\n'];
    core.write_input(handle, "one", data.clone()).await.unwrap();
    core.write_input(handle, "one", data.clone()).await.unwrap();
    assert_eq!(
        core.write_input(handle, "one", vec![42])
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );
    core.write_input(handle, "two", b"second".to_vec())
        .await
        .unwrap();
    core.close_input(handle).await.unwrap();
    core.close_input(handle).await.unwrap();
    assert_eq!(
        core.write_input(handle, "late", vec![1])
            .await
            .unwrap_err()
            .code,
        ErrorCode::StdinClosed
    );
    let result = finish(&core, handle).await;
    assert_eq!(bytes(&result), [data, b"second".to_vec()].concat());
    assert_eq!(
        core.write_input(handle, "one", vec![0, 255, b'a', b'\n'])
            .await
            .unwrap()
            .accepted_bytes,
        4
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn terminal_interaction_and_resize() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let mut start = request("terminal", &["interactive"]);
    start.io = IoMode::Pty { rows: 24, cols: 80 };
    let handle = core.start_execution(start).await.unwrap().execution.handle;
    wait_for(&core, handle, "ready").await;
    let resized = core.resize_terminal(handle, 40, 100).await.unwrap();
    assert_eq!(
        resized.io,
        IoMode::Pty {
            rows: 40,
            cols: 100
        }
    );
    assert_eq!(
        core.close_input(handle).await.unwrap_err().code,
        ErrorCode::UnsupportedOperation
    );
    let newline = if cfg!(windows) { "\r" } else { "\n" };
    core.write_input(handle, "name", format!("hello{newline}").into_bytes())
        .await
        .unwrap();
    wait_for(&core, handle, "received:hello").await;
    core.write_input(handle, "end", format!("quit{newline}").into_bytes())
        .await
        .unwrap();
    let result = finish(&core, handle).await;
    assert!(matches!(
        result.execution.result,
        Some(ExecutionResult::Exited {
            exit_code: Some(0),
            ..
        })
    ));
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_starts_share_one_execution_and_conflicting_retries_fail() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let start = request("same", &["sleep"]);
    let (a, b) = tokio::join!(
        core.start_execution(start.clone()),
        core.start_execution(start.clone())
    );
    assert_eq!(a.unwrap().execution.handle, b.unwrap().execution.handle);
    let mut conflicting = start;
    conflicting.command = command(&["exit", "0"]);
    assert_eq!(
        core.start_execution(conflicting).await.unwrap_err().code,
        ErrorCode::IdempotencyConflict
    );
    assert_eq!(
        core.list_executions(ListRequest::default())
            .await
            .unwrap()
            .executions
            .len(),
        1
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_start_observation_preserves_accepted_work() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let other = core.clone();
    let mut start = request("cancelled", &["sleep"]);
    start.wait_ms = 5000;
    let task = tokio::spawn(async move { other.start_execution(start).await });
    let handle = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let page = core.list_executions(ListRequest::default()).await.unwrap();
            if let Some(execution) = page.executions.first() {
                break execution.handle;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    wait_for(&core, handle, "ready").await;
    assert_eq!(
        core.get_execution(handle).await.unwrap().state,
        ExecutionState::Running
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn activity_wait_and_collect_wait_have_distinct_behavior() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let started = core
        .start_execution(request("delayed", &["delayed"]))
        .await
        .unwrap();
    let mut observe = ObserveRequest::new(started.execution.handle);
    observe.after_cursor = Some(started.next_cursor);
    observe.wait_ms = 1000;
    let first = core.observe_execution(observe.clone()).await.unwrap();
    assert_eq!(first.return_reason, ReturnReason::Activity);
    assert_eq!(first.execution.state, ExecutionState::Running);
    observe.return_when = WaitMode::FinishedOrTimeout;
    observe.after_cursor = None;
    let all = core.observe_execution(observe).await.unwrap();
    let output = String::from_utf8(bytes(&all)).unwrap();
    assert!(output.contains("first") && output.contains("last"));
    assert_eq!(all.execution.state, ExecutionState::Finished);
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn launch_failure_is_retained_and_distinct_from_nonzero_exit() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let start = StartRequest::new(
        "missing",
        Command::program("/does/not/exist", std::iter::empty::<String>()),
    );
    let result = core.start_execution(start.clone()).await.unwrap();
    assert!(matches!(
        result.execution.result,
        Some(ExecutionResult::StartFailed { .. })
    ));
    assert_eq!(
        core.start_execution(start).await.unwrap().execution.handle,
        result.execution.handle
    );
    let started = core
        .start_execution(request("nonzero", &["exit", "7"]))
        .await
        .unwrap();
    assert!(matches!(
        finish(&core, started.execution.handle)
            .await
            .execution
            .result,
        Some(ExecutionResult::Exited {
            exit_code: Some(7),
            ..
        })
    ));
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn capacity_and_generation_are_enforced() {
    let mut config = config();
    config.limits.max_active_executions = 1;
    let core = ProcessExecutionCore::new(config.clone()).unwrap();
    let started = core
        .start_execution(request("first", &["sleep"]))
        .await
        .unwrap();
    assert_eq!(
        core.start_execution(request("second", &["sleep"]))
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResourceLimit
    );
    let other = ProcessExecutionCore::new(config).unwrap();
    assert_eq!(
        other
            .get_execution(started.execution.handle)
            .await
            .unwrap_err()
            .code,
        ErrorCode::GenerationMismatch
    );
    core.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn list_pages_filter_and_freeze_membership_at_first_page() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    for id in ["a", "b", "c"] {
        let mut start = request(id, &["sleep"]);
        start.labels.insert("project".into(), "one".into());
        core.start_execution(start).await.unwrap();
    }
    let mut request = ListRequest {
        limit: 2,
        ..ListRequest::default()
    };
    request.labels.insert("project".into(), "one".into());
    let first = core.list_executions(request.clone()).await.unwrap();
    assert_eq!(first.executions.len(), 2);
    core.start_execution(StartRequest::new("later", command(&["sleep"])))
        .await
        .unwrap();
    request.page_cursor = first.next_page_cursor;
    let last = core.list_executions(request).await.unwrap();
    assert_eq!(last.executions.len(), 1);
    assert!(last.next_page_cursor.is_none());
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn finished_records_expire_and_start_ids_have_the_same_retention() {
    let mut config = config();
    config.limits.finished_retention = Duration::from_millis(60);
    let core = ProcessExecutionCore::new(config).unwrap();
    let mut start = request("expires", &["exit", "0"]);
    start.wait_ms = 1000;
    let first = core.start_execution(start.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        core.get_execution(first.execution.handle)
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    let second = core.start_execution(start).await.unwrap();
    assert_ne!(first.execution.handle, second.execution.handle);
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn shell_selection_is_reported_and_explicit_missing_shell_is_an_error() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let script = match core.runtime_info().default_shell.kind {
        ShellKind::PowerShell => "Write-Output 'shell-ok'",
        ShellKind::Cmd => "echo shell-ok",
        _ => "printf shell-ok",
    };
    let mut start = StartRequest::new("shell", Command::shell(script));
    start.wait_ms = 1000;
    let result = core.start_execution(start).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes(&result)).contains("shell-ok"));
    assert_eq!(
        result.execution.resolved_shell,
        Some(core.runtime_info().default_shell)
    );
    let command = Command::Shell {
        script: "echo test".into(),
        shell: Some(Shell {
            executable: "/missing/shell".into(),
            kind: ShellKind::Bash,
        }),
        login: false,
    };
    assert_eq!(
        core.start_execution(StartRequest::new("bad-shell", command))
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidArgument
    );
    core.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn terminate_escalates_when_sigterm_is_ignored() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let start = StartRequest::new(
        "stubborn",
        Command::shell("trap '' TERM; printf ready; while :; do sleep 1; done"),
    );
    let handle = core.start_execution(start).await.unwrap().execution.handle;
    wait_for(&core, handle, "ready").await;
    core.terminate_execution(handle, Some(Duration::from_millis(30)))
        .await
        .unwrap();
    core.terminate_execution(handle, Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let result = finish(&core, handle).await;
    assert!(matches!(
        result.execution.result,
        Some(ExecutionResult::Terminated { .. })
    ));
    core.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn interrupt_can_leave_program_running() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let start = StartRequest::new(
        "interrupt",
        Command::shell("trap '' INT; printf ready; while :; do sleep 1; done"),
    );
    let handle = core.start_execution(start).await.unwrap().execution.handle;
    wait_for(&core, handle, "ready").await;
    core.interrupt_execution(handle, "first").await.unwrap();
    assert_eq!(
        core.get_execution(handle).await.unwrap().state,
        ExecutionState::Running
    );
    core.interrupt_execution(handle, "first").await.unwrap();
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn stdin_queue_capacity_and_receipt_capacity_are_bounded() {
    let mut config = config();
    config.limits.max_queued_input_bytes = 8;
    config.limits.max_input_receipts = 1;
    let core = ProcessExecutionCore::new(config).unwrap();
    let mut start = request("queue", &["copy"]);
    start.io = IoMode::Pipes { stdin: true };
    let handle = core.start_execution(start).await.unwrap().execution.handle;
    assert_eq!(
        core.write_input(handle, "large", vec![1; 9])
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResourceLimit
    );
    core.write_input(handle, "one", vec![42]).await.unwrap();
    assert_eq!(
        core.write_input(handle, "two", vec![43])
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResourceLimit
    );
    core.write_input(handle, "one", vec![42]).await.unwrap();
    core.close_input(handle).await.unwrap();
    assert_eq!(bytes(&finish(&core, handle).await), vec![42]);
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreign_output_cursor_is_rejected() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let a = core
        .start_execution(request("a", &["exit", "0"]))
        .await
        .unwrap();
    let b = core
        .start_execution(request("b", &["exit", "0"]))
        .await
        .unwrap();
    let mut observe = ObserveRequest::new(b.execution.handle);
    observe.after_cursor = Some(a.next_cursor);
    assert_eq!(
        core.observe_execution(observe).await.unwrap_err().code,
        ErrorCode::InvalidArgument
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn output_limit_returns_before_a_long_wait_finishes() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let mut start = request("limited", &["sleep"]);
    start.wait_ms = 5000;
    start.max_output_bytes = Some(2);
    let observation =
        tokio::time::timeout(Duration::from_secs(3), core.start_execution(start.clone()))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(observation.return_reason, ReturnReason::OutputLimit);
    assert_eq!(bytes(&observation).len(), 2);
    assert_eq!(observation.execution.state, ExecutionState::Running);
    // Replaying a successful start must not restart its initial five-second wait.
    tokio::time::timeout(Duration::from_secs(1), core.start_execution(start))
        .await
        .unwrap()
        .unwrap();
    core.shutdown().await.unwrap();
}

fn child_pid(observation: &Observation) -> u32 {
    String::from_utf8_lossy(&bytes(observation))
        .lines()
        .find_map(|line| {
            line.strip_prefix("child=")
                .and_then(|id| id.trim().parse().ok())
        })
        .unwrap()
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_TIMEOUT},
        System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject},
    };
    unsafe {
        let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
        if handle.is_null() {
            return false;
        }
        let running = WaitForSingleObject(handle, 0) == WAIT_TIMEOUT;
        CloseHandle(handle);
        running
    }
}

async fn assert_stopped(pid: u32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_alive(pid) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("managed descendant survived cleanup");
}

#[tokio::test]
async fn termination_cleans_up_descendants_in_both_io_modes() {
    for io in [
        IoMode::Pipes { stdin: false },
        IoMode::Pty { rows: 24, cols: 80 },
    ] {
        let core = ProcessExecutionCore::new(config()).unwrap();
        let mut start = request("tree", &["descendant"]);
        start.io = io;
        let handle = core.start_execution(start).await.unwrap().execution.handle;
        wait_for(&core, handle, "child=").await;
        let pid = child_pid(
            &core
                .observe_execution(ObserveRequest::new(handle))
                .await
                .unwrap(),
        );
        core.terminate_execution(handle, Some(Duration::from_millis(30)))
            .await
            .unwrap();
        finish(&core, handle).await;
        assert_stopped(pid).await;
        core.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn normal_exit_cleans_up_background_descendants() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let handle = core
        .start_execution(request("orphan", &["orphan"]))
        .await
        .unwrap()
        .execution
        .handle;
    let result = finish(&core, handle).await;
    assert!(!result.execution.output_incomplete);
    assert_stopped(child_pid(&result)).await;
    core.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn detached_descendant_cannot_hold_output_drain_open_forever() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let handle = core
        .start_execution(request("escaped", &["escaped"]))
        .await
        .unwrap()
        .execution
        .handle;
    let result = finish(&core, handle).await;
    let pid = child_pid(&result);
    // The fixture deliberately created another process group. Clean it up here.
    unsafe {
        libc::kill(pid as i32, libc::SIGKILL);
    }
    assert!(result.execution.output_incomplete);
    core.shutdown().await.unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn windows_pipe_interrupt_is_explicitly_unsupported() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let handle = core
        .start_execution(request("pipe", &["sleep"]))
        .await
        .unwrap()
        .execution
        .handle;
    assert_eq!(
        core.interrupt_execution(handle, "interrupt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedOperation
    );
    core.shutdown().await.unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn windows_cmd_shell_executes_scripts() {
    let core = ProcessExecutionCore::new(config()).unwrap();
    let script = Command::Shell {
        script: "echo cmd-ok".into(),
        shell: Some(Shell {
            executable: "cmd.exe".into(),
            kind: ShellKind::Cmd,
        }),
        login: false,
    };
    let mut start = StartRequest::new("cmd", script);
    start.wait_ms = 1000;
    let result = core.start_execution(start).await.unwrap();
    assert!(String::from_utf8_lossy(&bytes(&result)).contains("cmd-ok"));
    core.shutdown().await.unwrap();
}
