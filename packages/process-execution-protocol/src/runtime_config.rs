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
    shell_snapshot: ShellSnapshotOverrides,
    limits: LimitOverrides,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ShellSnapshotOverrides {
    enabled: Option<bool>,
    max_cached_scopes: Option<usize>,
    capture_timeout_ms: Option<u64>,
    max_capture_bytes: Option<usize>,
    max_state_bytes: Option<usize>,
    retry_backoff_ms: Option<u64>,
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
    max_file_read_bytes: Option<usize>,
    max_file_write_bytes: Option<usize>,
    max_file_mutation_receipts: Option<usize>,
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
        self.shell_snapshot.apply(&mut config.shell_snapshot);
        self.limits.apply(&mut config.limits);
        Ok(config)
    }
}

impl ShellSnapshotOverrides {
    fn apply(self, config: &mut process_execution_core::ShellSnapshotConfig) {
        if let Some(value) = self.enabled {
            config.enabled = value;
        }
        if let Some(value) = self.max_cached_scopes {
            config.max_cached_scopes = value;
        }
        if let Some(value) = self.capture_timeout_ms {
            config.capture_timeout = Duration::from_millis(value);
        }
        if let Some(value) = self.max_capture_bytes {
            config.max_capture_bytes = value;
        }
        if let Some(value) = self.max_state_bytes {
            config.max_state_bytes = value;
        }
        if let Some(value) = self.retry_backoff_ms {
            config.retry_backoff = Duration::from_millis(value);
        }
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
        if let Some(v) = self.max_file_read_bytes {
            limits.max_file_read_bytes = v;
        }
        if let Some(v) = self.max_file_write_bytes {
            limits.max_file_write_bytes = v;
        }
        if let Some(v) = self.max_file_mutation_receipts {
            limits.max_file_mutation_receipts = v;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_snapshot_overrides_are_applied() {
        let parsed: RuntimeConfig = serde_json::from_str(
            r#"{
                "shell_snapshot": {
                    "enabled": false,
                    "max_cached_scopes": 7,
                    "capture_timeout_ms": 123,
                    "max_capture_bytes": 2000,
                    "max_state_bytes": 1000,
                    "retry_backoff_ms": 45
                }
            }"#,
        )
        .unwrap();
        let config = parsed.into_core(Some(std::env::temp_dir())).unwrap();
        assert!(!config.shell_snapshot.enabled);
        assert_eq!(config.shell_snapshot.max_cached_scopes, 7);
        assert_eq!(
            config.shell_snapshot.capture_timeout,
            Duration::from_millis(123)
        );
        assert_eq!(config.shell_snapshot.max_capture_bytes, 2000);
        assert_eq!(config.shell_snapshot.max_state_bytes, 1000);
        assert_eq!(
            config.shell_snapshot.retry_backoff,
            Duration::from_millis(45)
        );
    }
}
