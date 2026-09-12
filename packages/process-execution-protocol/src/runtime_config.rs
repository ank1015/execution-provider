use process_execution_core::{Config, Limits, Shell};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    cwd: Option<PathBuf>,
    default_shell: Option<Shell>,
    env: BTreeMap<String, String>,
    limits: LimitOverrides,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LimitOverrides {
    max_active_executions: Option<usize>,
    max_retained_executions: Option<usize>,
    max_retained_output_bytes: Option<usize>,
    max_output_bytes_per_response: Option<usize>,
    max_queued_input_bytes: Option<usize>,
    max_input_receipts: Option<usize>,
    max_interrupt_receipts: Option<usize>,
    max_wait_ms: Option<u64>,
    finished_retention_ms: Option<u64>,
    termination_grace_ms: Option<u64>,
    max_termination_grace_ms: Option<u64>,
    output_drain_timeout_ms: Option<u64>,
}

pub fn load(path: Option<&Path>, cwd: Option<PathBuf>) -> crate::Result<Config> {
    let file: RuntimeConfig = match path {
        Some(path) => serde_json::from_reader(std::fs::File::open(path)?)?,
        None => RuntimeConfig::default(),
    };
    file.into_core(cwd)
}

impl RuntimeConfig {
    pub fn into_core(self, cwd: Option<PathBuf>) -> crate::Result<Config> {
        let mut config = Config::new(cwd.or(self.cwd).unwrap_or(std::env::current_dir()?));
        config.default_shell = self.default_shell;
        config.env = self.env;
        self.limits.apply(&mut config.limits);
        Ok(config)
    }
}

impl LimitOverrides {
    fn apply(self, limits: &mut Limits) {
        // Keep the core's defaults authoritative when an override is absent.
        if let Some(v) = self.max_active_executions {
            limits.max_active_executions = v;
        }
        if let Some(v) = self.max_retained_executions {
            limits.max_retained_executions = v;
        }
        if let Some(v) = self.max_retained_output_bytes {
            limits.max_retained_output_bytes = v;
        }
        if let Some(v) = self.max_output_bytes_per_response {
            limits.max_output_bytes_per_response = v;
        }
        if let Some(v) = self.max_queued_input_bytes {
            limits.max_queued_input_bytes = v;
        }
        if let Some(v) = self.max_input_receipts {
            limits.max_input_receipts = v;
        }
        if let Some(v) = self.max_interrupt_receipts {
            limits.max_interrupt_receipts = v;
        }
        if let Some(v) = self.max_wait_ms {
            limits.max_wait = Duration::from_millis(v);
        }
        if let Some(v) = self.finished_retention_ms {
            limits.finished_retention = Duration::from_millis(v);
        }
        if let Some(v) = self.termination_grace_ms {
            limits.termination_grace = Duration::from_millis(v);
        }
        if let Some(v) = self.max_termination_grace_ms {
            limits.max_termination_grace = Duration::from_millis(v);
        }
        if let Some(v) = self.output_drain_timeout_ms {
            limits.output_drain_timeout = Duration::from_millis(v);
        }
    }
}
