use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    io::Write,
    path::PathBuf,
    process::{Child, Stdio},
    sync::OnceLock,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_process-execution");

fn fixture() -> &'static PathBuf {
    static FIXTURE: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &FIXTURE
        .get_or_init(|| {
            let directory = tempfile::tempdir().unwrap();
            let executable =
                directory
                    .path()
                    .join(if cfg!(windows) { "child.exe" } else { "child" });
            let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../packages/process-execution-core/tests/fixtures/child.rs");
            assert!(
                std::process::Command::new("rustc")
                    .arg(source)
                    .arg("-o")
                    .arg(&executable)
                    .status()
                    .unwrap()
                    .success()
            );
            (directory, executable)
        })
        .1
}

struct Server {
    directory: tempfile::TempDir,
    endpoint: OsString,
    child: Child,
    generation: Value,
}

impl Server {
    async fn start() -> Self {
        fixture();
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        let endpoint = directory.path().join("s.sock").into_os_string();
        #[cfg(windows)]
        let endpoint = OsString::from(format!(
            r"\\.\pipe\process-execution-test-{}",
            Uuid::new_v4()
        ));
        let config = directory.path().join("config.json");
        std::fs::write(
            &config,
            serde_json::to_vec(&json!({
                "cwd": directory.path(), "env": {"PROCESS_CORE_TEST": "configured"},
                "limits": {"termination_grace_ms": 30, "output_drain_timeout_ms": 200}
            }))
            .unwrap(),
        )
        .unwrap();
        let stderr = std::fs::File::create(directory.path().join("stderr.log")).unwrap();
        let stdout = std::fs::File::create(directory.path().join("stdout.log")).unwrap();
        let child = std::process::Command::new(BINARY)
            .arg("serve")
            .arg("--endpoint")
            .arg(&endpoint)
            .arg("--config")
            .arg(config)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap();
        let mut server = Self {
            directory,
            endpoint,
            child,
            generation: Value::Null,
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let output = Command::new(BINARY)
                    .arg("health")
                    .arg("--endpoint")
                    .arg(&server.endpoint)
                    .arg("--timeout-ms")
                    .arg("500")
                    .output()
                    .await
                    .unwrap();
                if output.status.success() {
                    server.generation =
                        serde_json::from_slice::<Value>(&output.stdout).unwrap()["generation_id"]
                            .clone();
                    break;
                }
                assert!(
                    server.child.try_wait().unwrap().is_none(),
                    "server exited: {}",
                    std::fs::read_to_string(server.directory.path().join("stderr.log")).unwrap()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("server failed to become ready");
        server
    }

    fn request(&self, operation: &str, params: Value) -> Value {
        let mut request = json!({"protocol_version": process_execution_protocol::VERSION, "request_id": Uuid::new_v4().to_string(),
            "expected_generation_id": self.generation, "operation": operation});
        if !params.is_null() {
            request["params"] = params;
        }
        request
    }

    fn start_request(&self, id: &str, args: &[&str], io: Option<Value>) -> Value {
        let mut params = json!({"start_id": id, "command": {"type": "program", "executable": fixture(), "args": args}, "wait_ms": 1000});
        if let Some(io) = io {
            params["io"] = io;
        }
        self.request("execution.start", params)
    }

    async fn rpc(&self, request: &Value) -> Value {
        let output = rpc(&self.endpoint, request).await;
        assert!(
            !output.stdout.is_empty(),
            "RPC returned no JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(response["request_id"], request["request_id"]);
        assert_eq!(response["status"] == "ok", output.status.success());
        response
    }

    async fn ok(&self, request: &Value) -> Value {
        let response = self.rpc(request).await;
        assert_eq!(response["status"], "ok", "{response}");
        response["result"].clone()
    }

    async fn shutdown(&mut self) {
        self.ok(&self.request("runtime.shutdown", Value::Null))
            .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("supervisor failed to shut down");
        assert!(
            std::fs::read(self.directory.path().join("stdout.log"))
                .unwrap()
                .is_empty()
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let request = self.request("runtime.shutdown", Value::Null);
        if let Ok(mut rpc) = std::process::Command::new(BINARY)
            .arg("rpc")
            .arg("--endpoint")
            .arg(&self.endpoint)
            .arg("--timeout-ms")
            .arg("1000")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            let mut stdin = rpc.stdin.take().unwrap();
            let _ = writeln!(stdin, "{request}");
            drop(stdin);
            let _ = rpc.wait();
        }
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        for _ in 0..100 {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn spawn_rpc(endpoint: &OsString, request: &Value) -> tokio::process::Child {
    let mut child = Command::new(BINARY)
        .arg("rpc")
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--timeout-ms")
        .arg("10000")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    child
}

async fn rpc(endpoint: &OsString, request: &Value) -> std::process::Output {
    let mut child = spawn_rpc(endpoint, request).await;
    // Keep stdin open deliberately: a JSON newline must be enough to process the request.
    let stdin = child.stdin.take();
    let result = tokio::time::timeout(Duration::from_secs(12), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    drop(stdin);
    result
}

fn output_bytes(result: &Value) -> Vec<u8> {
    result["output"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|chunk| {
            STANDARD
                .decode(chunk["data_base64"].as_str().unwrap())
                .unwrap()
        })
        .collect()
}

async fn observe_finished(server: &Server, handle: &Value) -> Value {
    let request = server.request(
        "execution.observe",
        json!({"handle": handle, "wait_ms": 5000, "return_when": "finished_or_timeout"}),
    );
    let result = server.ok(&request).await;
    assert_eq!(result["execution"]["state"], "finished");
    result
}

#[tokio::test]
async fn version_health_and_graceful_shutdown() {
    let version = Command::new(BINARY).arg("version").output().await.unwrap();
    assert!(version.status.success());
    let version: Value = serde_json::from_slice(&version.stdout).unwrap();
    assert_eq!(version["binary"], "process-execution");
    assert_eq!(
        version["protocol_version"],
        process_execution_protocol::VERSION
    );
    let mut server = Server::start().await;
    let info = server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
    assert_eq!(info["runtime"]["generation_id"], server.generation);
    let endpoint = server.endpoint.clone();
    server.shutdown().await;
    #[cfg(unix)]
    assert!(!std::path::Path::new(&endpoint).exists());
    #[cfg(windows)]
    {
        assert!(
            tokio::net::windows::named_pipe::ClientOptions::new()
                .open(endpoint)
                .is_err()
        );
    }
}

#[tokio::test]
async fn rpc_supports_batches_and_sets_exit_code_for_partial_failure() {
    let server = Server::start().await;
    let batch = json!({"protocol_version": process_execution_protocol::VERSION, "request_id": "batch", "mode": "parallel", "operations": [
        {"request_id": "one", "operation": "runtime.info"},
        {"request_id": "two", "operation": "execution.list", "params": {}}
    ]});
    let output = rpc(&server.endpoint, &batch).await;
    assert!(output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["result"]["results"].as_array().unwrap().len(), 2);
    let mut batch = batch;
    batch["mode"] = json!("sequential");
    batch["operations"][0] =
        json!({"request_id": "invalid", "operation": "execution.list", "params": {"limit": 0}});
    let output = rpc(&server.endpoint, &batch).await;
    assert!(!output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["result"]["results"][0]["status"], "error");
    assert_eq!(response["result"]["results"][1]["status"], "skipped");
}

#[tokio::test]
async fn starts_use_configured_environment_and_working_directory() {
    let server = Server::start().await;
    let result = server
        .ok(&server.start_request(
            "context",
            &["context", "space $literal", "a\"b", "Unicode λ"],
            None,
        ))
        .await;
    let text = String::from_utf8(output_bytes(&result)).unwrap();
    assert!(text.contains("env=configured"));
    assert!(
        text.contains(
            server
                .directory
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert!(text.contains("arg=space $literal"));
    assert!(text.contains("arg=a\"b"));
    assert!(text.contains("arg=Unicode λ"));
    assert_eq!(result["execution"]["result"]["exit_code"], 0);
}

#[tokio::test]
async fn shell_commands_use_the_discovered_shell() {
    let server = Server::start().await;
    let result = server
        .ok(&server.request(
            "execution.start",
            json!({
                "start_id": "shell", "command": {"type": "shell", "script": "echo shell-ready"},
                "wait_ms": 5000
            }),
        ))
        .await;
    assert_eq!(result["execution"]["result"]["exit_code"], 0);
    assert!(String::from_utf8_lossy(&output_bytes(&result)).contains("shell-ready"));
    assert_eq!(result["execution"]["command"]["login"], false);
    let info = server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
    assert_eq!(
        result["execution"]["resolved_shell"],
        info["runtime"]["default_shell"]
    );
}

#[tokio::test]
async fn pipe_interrupt_matches_the_advertised_capability() {
    let server = Server::start().await;
    let result = server
        .ok(&server.start_request("interrupt", &["sleep"], None))
        .await;
    let handle = &result["execution"]["handle"];
    let info = server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
    let interrupt = server.request(
        "execution.interrupt",
        json!({"handle": handle, "operation_id": "interrupt-once"}),
    );
    let response = server.rpc(&interrupt).await;
    if info["runtime"]["pipe_interrupt"] == true {
        assert_eq!(response["status"], "ok");
        let finished = observe_finished(&server, handle).await;
        assert_eq!(finished["execution"]["result"]["reason"], "exited");
        // A replay acknowledges the original interrupt even after completion.
        server.ok(&interrupt).await;
    } else {
        assert_eq!(response["error"]["code"], "unsupported_operation");
        server
            .ok(&server.request("execution.terminate", json!({"handle": handle})))
            .await;
        observe_finished(&server, handle).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_shuts_down_the_supervisor_and_its_execution() {
    let mut server = Server::start().await;
    let result = server
        .ok(&server.start_request("descendant", &["descendant"], None))
        .await;
    let output = String::from_utf8(output_bytes(&result)).unwrap();
    let pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("child="))
        .unwrap()
        .parse()
        .unwrap();
    unsafe {
        libc::kill(server.child.id() as i32, libc::SIGTERM);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!std::path::Path::new(&server.endpoint).exists());
    // A child can briefly remain a zombie until its new parent reaps it on Linux.
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        assert_eq!(stat.rsplit_once(") ").unwrap().1.chars().next(), Some('Z'));
    }
    #[cfg(not(target_os = "linux"))]
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
}

#[tokio::test]
async fn cursors_page_binary_output_and_remain_replayable() {
    let server = Server::start().await;
    let mut start = server.start_request("bytes", &["bytes", "257"], None);
    start["params"]["max_output_bytes"] = json!(31);
    let first = server.ok(&start).await;
    let handle = &first["execution"]["handle"];
    let mut all = output_bytes(&first);
    let mut next = first["next_cursor"].clone();
    loop {
        let request = server.request(
            "execution.observe",
            json!({"handle": handle, "after_cursor": next,
            "wait_ms": 1000, "max_output_bytes": 31, "return_when": "finished_or_timeout"}),
        );
        let result = server.ok(&request).await;
        let replay = server.ok(&request).await;
        assert_eq!(output_bytes(&result), output_bytes(&replay));
        all.extend(output_bytes(&result));
        if result["execution"]["state"] == "finished" && result["has_more"] == false {
            break;
        }
        next = result["next_cursor"].clone();
    }
    assert_eq!(all, (0..257).map(|n| (n % 256) as u8).collect::<Vec<_>>());
}

#[tokio::test]
async fn writes_deduplicate_and_close_delivers_eof() {
    let server = Server::start().await;
    let mut start = server.start_request(
        "copy",
        &["copy"],
        Some(json!({"type": "pipes", "stdin": true})),
    );
    start["params"]["wait_ms"] = json!(0);
    let result = server.ok(&start).await;
    let handle = &result["execution"]["handle"];
    let bytes = vec![0, 255, b'a', b'\n'];
    let write = server.request(
        "execution.write_input",
        json!({"handle": handle, "input_id": "one", "data_base64": STANDARD.encode(&bytes)}),
    );
    assert_eq!(server.ok(&write).await["accepted_bytes"], bytes.len());
    server.ok(&write).await;
    let mut conflict = write.clone();
    conflict["params"]["data_base64"] = json!(STANDARD.encode(b"different"));
    assert_eq!(
        server.rpc(&conflict).await["error"]["code"],
        "idempotency_conflict"
    );
    server
        .ok(&server.request("execution.close_input", json!({"handle": handle})))
        .await;
    let output = observe_finished(&server, handle).await;
    assert_eq!(output_bytes(&output), bytes);
}

#[tokio::test]
async fn terminal_resize_input_and_completion() {
    let server = Server::start().await;
    let result = server
        .ok(&server.start_request(
            "terminal",
            &["interactive"],
            Some(json!({"type": "pty", "rows": 24, "cols": 80})),
        ))
        .await;
    assert_eq!(result["execution"]["state"], "running", "{result}");
    let handle = &result["execution"]["handle"];
    let resized = server
        .ok(&server.request(
            "execution.resize_terminal",
            json!({"handle": handle, "rows": 40, "cols": 100}),
        ))
        .await;
    assert_eq!(resized["io"]["cols"], 100);
    assert_eq!(
        server
            .rpc(&server.request("execution.close_input", json!({"handle": handle})))
            .await["error"]["code"],
        "unsupported_operation"
    );
    let input = if cfg!(windows) {
        b"hello\rquit\r".as_slice()
    } else {
        b"hello\nquit\n".as_slice()
    };
    server
        .ok(&server.request(
            "execution.write_input",
            json!({"handle": handle, "input_id": "lines", "data_base64": STANDARD.encode(input)}),
        ))
        .await;
    let finished = observe_finished(&server, handle).await;
    assert!(String::from_utf8_lossy(&output_bytes(&finished)).contains("received:hello"));
}

#[tokio::test]
async fn retry_and_listing_use_the_same_core_execution() {
    let server = Server::start().await;
    let start = server.start_request("same", &["exit", "7"], None);
    let first = server.ok(&start).await;
    let second = server.ok(&start).await;
    assert_eq!(first["execution"]["handle"], second["execution"]["handle"]);
    assert_eq!(first["execution"]["result"]["exit_code"], 7);
    server
        .ok(&server.start_request("second", &["exit", "0"], None))
        .await;
    let page = server
        .ok(&server.request("execution.list", json!({"state": "all", "limit": 1})))
        .await;
    assert_eq!(page["executions"].as_array().unwrap().len(), 1);
    let last = server
        .ok(&server.request(
            "execution.list",
            json!({"state": "all", "limit": 1, "page_cursor": page["next_page_cursor"]}),
        ))
        .await;
    assert_eq!(last["executions"].as_array().unwrap().len(), 1);
    assert!(last["next_page_cursor"].is_null());
}

#[tokio::test]
async fn killed_rpc_does_not_kill_the_managed_command() {
    let server = Server::start().await;
    let mut start = server.start_request("disconnected", &["sleep"], None);
    start["params"]["wait_ms"] = json!(5000);
    let mut relay = spawn_rpc(&server.endpoint, &start).await;
    let handle = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = server
                .ok(&server.request("execution.list", json!({})))
                .await;
            if let Some(execution) = page["executions"].as_array().unwrap().first() {
                break execution["handle"].clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    relay.kill().await.unwrap();
    let _ = relay.wait().await;
    let execution = server
        .ok(&server.request("execution.get", json!({"handle": handle})))
        .await;
    assert_eq!(execution["state"], "running");
    server
        .ok(&server.request(
            "execution.terminate",
            json!({"handle": handle, "grace_period_ms": 20}),
        ))
        .await;
    let finished = observe_finished(&server, &handle).await;
    assert_eq!(finished["execution"]["result"]["reason"], "terminated");
}

#[tokio::test]
async fn generation_protocol_and_payload_errors_leave_supervisor_usable() {
    let server = Server::start().await;
    let mut request = server.request("runtime.shutdown", Value::Null);
    request["expected_generation_id"] = json!(Uuid::new_v4());
    assert_eq!(
        server.rpc(&request).await["error"]["code"],
        "generation_mismatch"
    );
    let mut request = server.request("runtime.info", Value::Null);
    request["protocol_version"] = json!(99);
    assert_eq!(
        server.rpc(&request).await["error"]["code"],
        "invalid_argument"
    );
    let result = server
        .ok(&server.start_request("finished", &["exit", "0"], None))
        .await;
    let handle = &result["execution"]["handle"];
    let request = server.request(
        "execution.observe",
        json!({"handle": handle, "after_cursor": "not-a-cursor"}),
    );
    assert_eq!(
        server.rpc(&request).await["error"]["code"],
        "invalid_argument"
    );
    let request = server.request(
        "execution.write_input",
        json!({"handle": handle, "input_id": "bad", "data_base64": "!invalid!"}),
    );
    assert_eq!(
        server.rpc(&request).await["error"]["code"],
        "invalid_argument"
    );
    server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
}

#[tokio::test]
async fn another_server_cannot_take_over_a_live_endpoint() {
    let server = Server::start().await;
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(BINARY)
            .arg("serve")
            .arg("--endpoint")
            .arg(&server.endpoint)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn unix_endpoint_is_private_and_malformed_requests_are_rejected() {
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::AsyncBufReadExt;
    let server = Server::start().await;
    assert_eq!(
        std::fs::metadata(&server.endpoint)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let mut connection = tokio::net::UnixStream::connect(std::path::Path::new(&server.endpoint))
        .await
        .unwrap();
    connection.write_all(b"{broken}\n").await.unwrap();
    let mut response = String::new();
    tokio::io::BufReader::new(connection)
        .read_line(&mut response)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&response).unwrap()["error"]["code"],
        "invalid_argument"
    );
    server
        .ok(&server.request("runtime.info", Value::Null))
        .await;
}

#[cfg(unix)]
#[tokio::test]
async fn stale_unix_socket_is_recovered_and_regular_files_are_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let endpoint = directory.path().join("stale.sock");
    drop(std::os::unix::net::UnixListener::bind(&endpoint).unwrap());
    let mut child = Command::new(BINARY)
        .arg("serve")
        .arg("--endpoint")
        .arg(&endpoint)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let request = json!({"protocol_version": process_execution_protocol::VERSION, "request_id": "stop", "operation": "runtime.shutdown"});
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let output = rpc(&endpoint.clone().into_os_string(), &request).await;
            if output.status.success() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(child.wait().await.unwrap().success());
    std::fs::write(&endpoint, b"keep-me").unwrap();
    let output = Command::new(BINARY)
        .arg("serve")
        .arg("--endpoint")
        .arg(&endpoint)
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read(&endpoint).unwrap(), b"keep-me");
}
