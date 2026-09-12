use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Child, Stdio},
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    process::Command,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_process-execution-host-daemon");
const TOKEN: &str = "test-host-credential-never-log-this";
type Socket = WebSocketStream<TcpStream>;

fn fixture() -> &'static PathBuf {
    static FIXTURE: OnceLock<(tempfile::TempDir, PathBuf)> = OnceLock::new();
    &FIXTURE
        .get_or_init(|| {
            let directory = tempfile::tempdir().unwrap();
            let path = directory
                .path()
                .join(if cfg!(windows) { "child.exe" } else { "child" });
            assert!(
                std::process::Command::new("rustc")
                    .arg(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../../packages/process-execution-core/tests/fixtures/child.rs"
                    ))
                    .arg("-o")
                    .arg(&path)
                    .status()
                    .unwrap()
                    .success()
            );
            (directory, path)
        })
        .1
}

struct Host {
    directory: tempfile::TempDir,
    child: Child,
    id: Uuid,
}
impl Host {
    async fn start(listener: &TcpListener) -> Self {
        fixture();
        let directory = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let url = format!("http://{}", listener.local_addr().unwrap());
        // Reconfiguration also exercises replacing an existing credential on Windows.
        for _ in 0..2 {
            let mut configure = Command::new(BINARY)
                .arg("--state-dir")
                .arg(directory.path())
                .args([
                    "configure",
                    "--gateway-url",
                    &url,
                    "--host-id",
                    &id.to_string(),
                    "--allow-insecure-loopback",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            configure
                .stdin
                .take()
                .unwrap()
                .write_all(format!("{TOKEN}\n").as_bytes())
                .await
                .unwrap();
            let output = configure.wait_with_output().await.unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains(TOKEN));
        }
        let config = directory.path().join("config.json");
        std::fs::write(&config, json!({"execution": {"cwd": directory.path(), "env": {"PROCESS_CORE_TEST": "configured"}, "limits": {"termination_grace_ms": 30, "output_drain_timeout_ms": 500}}}).to_string()).unwrap();
        let child = std::process::Command::new(BINARY)
            .arg("--state-dir")
            .arg(directory.path())
            .args(["run", "--allow-insecure-loopback", "--config"])
            .arg(config)
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(directory.path().join("stdout.log")).unwrap())
            .stderr(std::fs::File::create(directory.path().join("stderr.log")).unwrap())
            .spawn()
            .unwrap();
        Self {
            directory,
            child,
            id,
        }
    }

    async fn status(&self) -> Value {
        let output = Command::new(BINARY)
            .arg("--state-dir")
            .arg(self.directory.path())
            .arg("status")
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains(TOKEN));
        serde_json::from_str(&text).unwrap()
    }

    async fn exited(&mut self, code: i32) {
        timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert_eq!(status.code(), Some(code));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("daemon did not stop");
        assert!(
            std::fs::read(self.directory.path().join("stdout.log"))
                .unwrap()
                .is_empty()
        );
        assert!(
            !std::fs::read_to_string(self.directory.path().join("stderr.log"))
                .unwrap()
                .contains(TOKEN)
        );
    }

    async fn revoke(&mut self, socket: &mut Socket) {
        send(socket, json!({"type": "disconnect", "code": "revoked"})).await;
        self.exited(2).await;
        let status = self.status().await;
        assert_eq!(status["running"], false);
        assert_eq!(status["last_status"]["state"], "needs_configuration");
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        #[cfg(unix)]
        {
            unsafe {
                libc::kill(self.child.id() as i32, libc::SIGTERM);
            }
            for _ in 0..100 {
                if self.child.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// Tungstenite fixes the handshake callback's error type.
#[allow(clippy::result_large_err)]
async fn accept(listener: &TcpListener, id: Uuid) -> (Socket, Value) {
    timeout(Duration::from_secs(10), async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, move |request: &Request, response: Response| {
            assert_eq!(
                request.headers().get("authorization").unwrap(),
                &format!("Bearer {TOKEN}")
            );
            assert_eq!(request.uri().path(), format!("/v1/hosts/{id}/connect"));
            Ok(response)
        })
        .await
        .unwrap();
        let hello = receive(&mut socket).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["host_id"], id.to_string());
        assert!(!hello.to_string().contains(TOKEN));
        (socket, hello)
    })
    .await
    .expect("daemon did not connect")
}

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn welcome(socket: &mut Socket, interval: u64) {
    send(socket, json!({"type": "welcome", "protocol_version": 1, "connection_id": Uuid::new_v4(), "heartbeat_interval_ms": interval})).await;
}
async fn receive(socket: &mut Socket) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            match socket.next().await.expect("daemon disconnected").unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                Message::Pong(_) => (),
                message => panic!("unexpected message {message:?}"),
            }
        }
    })
    .await
    .expect("daemon response timed out")
}

async fn rpc(socket: &mut Socket, mut request: Value) -> Value {
    request["protocol_version"] = json!(1);
    request["request_id"] = json!(Uuid::new_v4());
    send(socket, json!({"type": "request", "request": request})).await;
    let response = receive(socket).await;
    assert_eq!(response["type"], "response");
    assert_eq!(response["response"]["request_id"], request["request_id"]);
    response["response"].clone()
}

fn start(id: &str, args: &[&str]) -> Value {
    json!({"operation": "execution.start", "params": {"start_id": id, "command": {"type": "program", "executable": fixture(), "args": args}, "io": {"type": "pipes", "stdin": true}}})
}

#[tokio::test]
async fn authenticated_execution_batches_and_local_administration_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (mut socket, hello) = accept(&listener, host.id).await;
    welcome(&mut socket, 1000).await;
    let batch = rpc(&mut socket, json!({"expected_generation_id": hello["runtime"]["generation_id"], "mode": "parallel", "operations": [
        {"request_id": "info", "operation": "runtime.info"},
        {"request_id": "list", "operation": "execution.list", "params": {}}
    ]})).await;
    assert_eq!(batch["result"]["succeeded"], true);
    assert_eq!(
        batch["result"]["results"][0]["result"]["binary"]["binary"],
        "process-execution-host-daemon"
    );
    let shutdown = rpc(&mut socket, json!({"operation": "runtime.shutdown"})).await;
    assert_eq!(shutdown["status"], "error");
    let mut command = start("context", &["context"]);
    command["params"]["wait_ms"] = json!(5000);
    let response = rpc(&mut socket, command).await;
    assert_eq!(response["result"]["execution"]["result"]["exit_code"], 0);
    let output: Vec<u8> = response["result"]["output"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|chunk| {
            STANDARD
                .decode(chunk["data_base64"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    assert!(String::from_utf8_lossy(&output).contains("env=configured"));
    let mismatched = rpc(&mut socket, json!({"operation": "execution.list", "params": {}, "expected_generation_id": Uuid::new_v4()})).await;
    assert_eq!(mismatched["error"]["code"], "generation_mismatch");
    host.revoke(&mut socket).await;
}

#[tokio::test]
async fn missed_heartbeats_reconnect_without_losing_execution_or_accepted_batch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (mut first, hello) = accept(&listener, host.id).await;
    welcome(&mut first, 50).await;
    let started = rpc(&mut first, start("copy", &["copy"])).await;
    let handle = &started["result"]["execution"]["handle"];
    let mut later = start("later", &["exit", "0"]);
    later["request_id"] = json!("later");
    send(&mut first, json!({"type": "request", "request": {"protocol_version": 1, "request_id": "accepted-batch", "mode": "sequential", "operations": [
        {"request_id": "wait", "operation": "execution.observe", "params": {"handle": handle, "wait_ms": 10000, "return_when": "finished_or_timeout"}}, later
    ]}})).await;
    // Leave TCP open but stop reading/responding to heartbeats, as with a stale network.
    let (mut second, resumed) = accept(&listener, host.id).await;
    assert_eq!(
        hello["runtime"]["generation_id"],
        resumed["runtime"]["generation_id"]
    );
    assert_eq!(hello["installation_id"], resumed["installation_id"]);
    welcome(&mut second, 1000).await;
    drop(first);
    let still_running = rpc(
        &mut second,
        json!({"operation": "execution.get", "params": {"handle": handle}}),
    )
    .await;
    assert_eq!(still_running["result"]["state"], "running");
    let write = json!({"operation": "execution.write_input", "params": {"handle": handle, "input_id": "once", "data_base64": STANDARD.encode(b"retained")}});
    assert_eq!(rpc(&mut second, write.clone()).await["status"], "ok");
    assert_eq!(rpc(&mut second, write).await["status"], "ok");
    rpc(
        &mut second,
        json!({"operation": "execution.close_input", "params": {"handle": handle}}),
    )
    .await;
    let finished = rpc(&mut second, json!({"operation": "execution.observe", "params": {"handle": handle, "wait_ms": 5000, "return_when": "finished_or_timeout"}})).await;
    assert_eq!(finished["result"]["execution"]["state"], "finished");
    assert_eq!(
        finished["result"]["output"][0]["data_base64"],
        STANDARD.encode(b"retained")
    );
    timeout(Duration::from_secs(5), async {
        loop {
            let list = rpc(
                &mut second,
                json!({"operation": "execution.list", "params": {"state": "all"}}),
            )
            .await;
            if list["result"]["executions"].as_array().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("accepted batch did not continue after reconnect");
    host.revoke(&mut second).await;
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn unauthorized_handshake_stops_without_reconnect_or_credential_logging() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (stream, _) = timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let result = accept_hdr_async(stream, |_: &Request, _: Response| {
        Err(tokio_tungstenite::tungstenite::http::Response::builder()
            .status(401)
            .body(None)
            .unwrap())
    })
    .await;
    assert!(result.is_err());
    host.exited(2).await;
    assert_eq!(
        host.status().await["last_status"]["state"],
        "needs_configuration"
    );
    assert!(
        timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn incompatible_protocol_stops_and_a_second_daemon_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (mut socket, _) = accept(&listener, host.id).await;
    let duplicate = Command::new(BINARY)
        .arg("--state-dir")
        .arg(host.directory.path())
        .args(["run", "--allow-insecure-loopback"])
        .output()
        .await
        .unwrap();
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("another daemon"));
    send(&mut socket, json!({"type": "welcome", "protocol_version": 99, "connection_id": Uuid::new_v4(), "heartbeat_interval_ms": 1000})).await;
    host.exited(2).await;
}

#[tokio::test]
async fn configuration_requires_secure_transport_and_keeps_gateway_credentials_bound() {
    let directory = tempfile::tempdir().unwrap();
    let status = Command::new(BINARY)
        .arg("--state-dir")
        .arg(directory.path())
        .arg("status")
        .output()
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&status.stdout).unwrap()["configured"],
        false
    );
    let missing = Command::new(BINARY)
        .arg("--state-dir")
        .arg(directory.path())
        .arg("run")
        .output()
        .await
        .unwrap();
    assert!(!missing.status.success());
    for url in [
        "http://example.com",
        "https://user:secret@example.com",
        "https://example.com?secret=x",
    ] {
        let output = Command::new(BINARY)
            .arg("--state-dir")
            .arg(directory.path())
            .args([
                "configure",
                "--gateway-url",
                url,
                "--host-id",
                &Uuid::new_v4().to_string(),
                "--allow-insecure-loopback",
            ])
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (mut socket, _) = accept(&listener, host.id).await;
    welcome(&mut socket, 1000).await;
    host.revoke(&mut socket).await;
    let output = Command::new(BINARY)
        .arg("--state-dir")
        .arg(host.directory.path())
        .args([
            "run",
            "--allow-insecure-loopback",
            "--gateway-url",
            "https://different.example.com",
        ])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("differs from the registered gateway")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(host.directory.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(host.directory.path().join("credential.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn local_sigterm_shuts_down_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut host = Host::start(&listener).await;
    let (mut socket, _) = accept(&listener, host.id).await;
    welcome(&mut socket, 1000).await;
    rpc(&mut socket, start("copy", &["copy"])).await;
    unsafe {
        libc::kill(host.child.id() as i32, libc::SIGTERM);
    }
    host.exited(0).await;
    assert_eq!(host.status().await["last_status"]["state"], "stopped");
}
