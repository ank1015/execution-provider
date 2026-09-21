use process_execution_core::{Config, ProcessExecutionCore};
use process_execution_protocol::{Dispatcher, Request, Response, VERSION, version_info};
use serde_json::{Value, json};

async fn dispatch(dispatcher: &Dispatcher, value: Value) -> Value {
    let request: Request = serde_json::from_value(value).unwrap();
    serde_json::to_value(dispatcher.dispatch(request).await).unwrap()
}

#[tokio::test]
async fn patch_dispatch_and_batch_status_preserve_structured_rejection() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("file"), "before\n").unwrap();
    let core = ProcessExecutionCore::new(Config::new(directory.path())).unwrap();
    let dispatcher = Dispatcher::gateway(core, version_info("test", "0"));
    let params = json!({"mutation_id":"protocol-patch", "patch":{"format":"text_replacements",
        "files":[{"path":"file", "edits":[{"oldText":"before","newText":"after"}]}]}});
    let result = dispatch(
        &dispatcher,
        json!({"protocol_version":VERSION,"request_id":"one",
        "operation":"filesystem.apply_patch","params":params}),
    )
    .await;
    assert_eq!(result["status"], "ok");
    assert_eq!(result["result"]["status"], "applied");
    assert!(
        serde_json::from_value::<Response>(result.clone())
            .unwrap()
            .succeeded()
    );
    assert!(result["result"]["changes"][0]["after_sha256"].is_string());
    assert_eq!(
        std::fs::read_to_string(directory.path().join("file")).unwrap(),
        "after\n"
    );

    let rejected = dispatch(&dispatcher, json!({"protocol_version":VERSION,"request_id":"two",
        "mode":"sequential", "operations":[
            {"request_id":"patch", "operation":"filesystem.apply_patch", "params":{
                "mutation_id":"rejected-patch", "patch":{"format":"codex", "text":"*** Begin Patch\n*** Update File: file\n@@\n-missing\n+x\n*** End Patch"}}},
            {"request_id":"following", "operation":"runtime.info"}
        ]})).await;
    assert_eq!(rejected["result"]["succeeded"], false);
    assert!(
        !serde_json::from_value::<Response>(rejected.clone())
            .unwrap()
            .succeeded()
    );
    assert_eq!(
        rejected["result"]["results"][0]["result"]["status"],
        "rejected"
    );
    assert_eq!(rejected["result"]["results"][1]["status"], "skipped");
    assert!(serde_json::from_value::<Request>(json!({"protocol_version":VERSION,"request_id":"bad",
        "operation":"filesystem.apply_patch","params":{"mutation_id":"x","patch":{"format":"unknown"}}})).is_err());
}
