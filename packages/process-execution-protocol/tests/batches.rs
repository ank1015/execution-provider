use process_execution_core::{Config, ProcessExecutionCore};
use process_execution_protocol::{Dispatcher, Outcome, Request, version_info};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::OnceLock};

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
                        "/../process-execution-core/tests/fixtures/child.rs"
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

fn runtime() -> (ProcessExecutionCore, Dispatcher) {
    let core = ProcessExecutionCore::new(Config::new(std::env::temp_dir())).unwrap();
    let dispatcher = Dispatcher::local(core.clone(), version_info("test", "0"));
    (core, dispatcher)
}

fn start(id: &str) -> Value {
    json!({"request_id": id, "operation": "execution.start", "params": {
        "start_id": id, "command": {"type": "program", "executable": fixture(), "args": ["copy"]},
        "io": {"type": "pipes", "stdin": true}
    }})
}

async fn dispatch(dispatcher: &Dispatcher, payload: Value) -> Value {
    let mut payload = payload;
    payload["protocol_version"] = json!(1);
    payload["request_id"] = json!("envelope");
    let response = dispatcher
        .dispatch(serde_json::from_value(payload).unwrap())
        .await;
    serde_json::to_value(response).unwrap()
}

#[tokio::test]
async fn sequential_preserves_order_stops_on_errors_and_deduplicates_starts() {
    let (core, dispatcher) = runtime();
    let mut bad = start("invalid");
    bad["params"]["wait_ms"] = json!(300001);
    let batch =
        json!({"mode": "sequential", "operations": [start("first"), bad, start("skipped")]});
    let first = dispatch(&dispatcher, batch.clone()).await;
    let retry = dispatch(&dispatcher, batch).await;
    assert_eq!(first["result"]["succeeded"], false);
    let results = &first["result"]["results"];
    assert_eq!(results[0]["status"], "ok");
    assert_eq!(results[1]["status"], "error");
    assert_eq!(results[2]["status"], "skipped");
    assert_eq!(
        results[0]["result"]["execution"]["handle"],
        retry["result"]["results"][0]["result"]["execution"]["handle"]
    );
    assert_eq!(
        core.list_executions(Default::default())
            .await
            .unwrap()
            .executions
            .len(),
        1
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn parallel_input_and_eof_can_complete_an_observation_in_the_same_batch() {
    let (core, dispatcher) = runtime();
    let first = dispatch(&dispatcher, start("copy")).await;
    let handle = &first["result"]["execution"]["handle"];
    let result = dispatch(&dispatcher, json!({"mode": "parallel", "operations": [
        {"request_id": "observe", "operation": "execution.observe", "params": {"handle": handle, "wait_ms": 5000, "return_when": "finished_or_timeout"}},
        {"request_id": "close", "operation": "execution.close_input", "params": {"handle": handle}},
        {"request_id": "info", "operation": "runtime.info"}
    ]})).await;
    assert_eq!(result["result"]["succeeded"], true);
    assert_eq!(result["result"]["results"][0]["request_id"], "observe");
    assert_eq!(
        result["result"]["results"][0]["result"]["execution"]["state"],
        "finished"
    );
    assert_eq!(result["result"]["results"][1]["request_id"], "close");
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_batches_and_generation_fences_have_no_side_effects() {
    let (core, dispatcher) = runtime();
    for operations in [
        vec![],
        vec![start("duplicate"), start("duplicate")],
        vec![
            start("first"),
            json!({"request_id": "stop", "operation": "runtime.shutdown"}),
        ],
        (0..33).map(|n| start(&n.to_string())).collect(),
    ] {
        let result = dispatch(&dispatcher, json!({"operations": operations})).await;
        assert_eq!(result["status"], "error", "{result}");
    }
    let result = dispatch(
        &dispatcher,
        json!({"expected_generation_id": uuid::Uuid::new_v4(), "operations": [start("fenced")]}),
    )
    .await;
    assert_eq!(result["error"]["code"], "generation_mismatch");
    assert!(
        core.list_executions(Default::default())
            .await
            .unwrap()
            .executions
            .is_empty()
    );
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_dispatcher_rejects_shutdown() {
    let (core, _) = runtime();
    let dispatcher = Dispatcher::gateway(core.clone(), version_info("test", "0"));
    let response = dispatcher.dispatch(serde_json::from_value(json!({"protocol_version": 1,"request_id": "stop", "operation": "runtime.shutdown"})).unwrap()).await;
    assert!(matches!(response.outcome, Outcome::Error { .. }));
    assert!(dispatch(&dispatcher, start("still-running")).await["result"]["execution"].is_object());
    core.shutdown().await.unwrap();
}

#[test]
fn mixed_and_nested_batches_are_rejected() {
    for value in [
        json!({"protocol_version": 1,"request_id": "bad", "operation": "runtime.info", "operations": []}),
        json!({"protocol_version": 1,"request_id": "bad", "operations": [{"request_id": "nested", "operations": []}]}),
    ] {
        assert!(serde_json::from_value::<Request>(value).is_err());
    }
}

#[tokio::test]
async fn oversized_request_id_cannot_overflow_the_error_response() {
    let (core, dispatcher) = runtime();
    let request: Request = serde_json::from_value(json!({"protocol_version": 1,
        "request_id": "x".repeat(1024), "operation": "runtime.info"}))
    .unwrap();
    let response = dispatcher.dispatch(request).await;
    assert!(!response.is_ok());
    assert!(response.request_id.is_none());
    assert!(
        process_execution_protocol::encode_response(&response)
            .unwrap()
            .len()
            < 1024
    );
    core.shutdown().await.unwrap();
}
