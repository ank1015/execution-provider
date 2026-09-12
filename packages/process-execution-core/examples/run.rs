use process_execution_core::{
    Command, Config, ExecutionState, ObserveRequest, ProcessExecutionCore, StartRequest, WaitMode,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let core = ProcessExecutionCore::new(Config::new(std::env::current_dir()?))?;
    let mut start = StartRequest::new("rust-version", Command::program("rustc", ["--version"]));
    start.wait_ms = 1000;
    let mut observation = core.start_execution(start).await?;
    loop {
        for chunk in &observation.output {
            print!("{}", String::from_utf8_lossy(&chunk.data));
        }
        if observation.execution.state == ExecutionState::Finished && !observation.has_more {
            break;
        }
        let mut next = ObserveRequest::new(observation.execution.handle);
        next.after_cursor = Some(observation.next_cursor);
        next.wait_ms = 1000;
        next.return_when = WaitMode::FinishedOrTimeout;
        observation = core.observe_execution(next).await?;
    }
    println!("Result: {:?}", observation.execution.result);
    core.shutdown().await?;
    Ok(())
}
