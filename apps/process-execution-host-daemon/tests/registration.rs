use serde_json::{Value, json};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    time::timeout,
};
use uuid::Uuid;

const BINARY: &str = env!("CARGO_BIN_EXE_process-execution-host-daemon");
const TOKEN: &str = "registration-token-do-not-log";
const CREDENTIAL: &str = "machine-credential-do-not-log";

#[tokio::test]
async fn enrollment_uses_stable_identity_and_preserves_credentials_on_failure() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway = format!("http://{}/base", listener.local_addr().unwrap());
    let machine = Uuid::new_v4();
    let mut identity = Value::Null;
    // A successful enrollment, rejected token, wrong machine, and redirect.
    for (index, status, body) in [
        (0, 200, json!({"machineId":machine,"credential":CREDENTIAL})),
        (1, 401, json!({"error":TOKEN})),
        (
            2,
            200,
            json!({"machineId":Uuid::new_v4(),"credential":TOKEN}),
        ),
        (3, 307, json!({"error":TOKEN})),
    ] {
        let mut child = Command::new(BINARY)
            .arg("--state-dir")
            .arg(directory.path())
            .args([
                "register",
                "--gateway-url",
                &gateway,
                "--machine-id",
                &machine.to_string(),
                "--allow-insecure-loopback",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{TOKEN}\r\n").as_bytes())
            .await
            .unwrap();
        let (mut stream, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (headers, request) = timeout(Duration::from_secs(5), async {
            let mut bytes = Vec::new();
            loop {
                let mut buffer = [0; 2048];
                let n = stream.read(&mut buffer).await.unwrap();
                assert_ne!(n, 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        let request: Value =
                            serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap();
                        break (headers, request);
                    }
                }
            }
        })
        .await
        .unwrap();
        assert!(headers.starts_with(&format!(
            "POST /base/v1/machines/{machine}/register HTTP/1.1"
        )));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains(&format!("authorization: bearer {TOKEN}"))
        );
        if index == 0 {
            identity = request["installationId"].clone();
        }
        assert_eq!(request, json!({"installationId":identity}));
        let body = body.to_string();
        stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLocation: /redirect-must-not-receive-token\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        drop(stream);
        let output = timeout(Duration::from_secs(5), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.status.success(), index == 0);
        for text in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(text);
            assert!(!text.contains(TOKEN));
            assert!(!text.contains(CREDENTIAL));
        }
        let saved: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join("credential.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(saved["token"], CREDENTIAL);
        assert_eq!(saved["host_id"], machine.to_string());
        assert_eq!(saved["gateway_url"], format!("{gateway}/"));
    }
}
