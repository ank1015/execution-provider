use crate::{Error, Result, Shell};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Maximum file payload that can safely fit in the versioned protocol frame
/// after base64 and JSON encoding.
pub const MAX_FILE_BYTES: usize = 5 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub cwd: PathBuf,
    pub default_shell: Option<Shell>,
    pub env: BTreeMap<String, String>,
    pub limits: Limits,
}

impl Config {
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            default_shell: None,
            env: BTreeMap::new(),
            limits: Limits::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_active_executions: usize,
    pub max_retained_executions: usize,
    pub max_retained_output_bytes: usize,
    pub max_output_bytes_per_response: usize,
    pub max_queued_input_bytes: usize,
    pub max_input_receipts: usize,
    pub max_interrupt_receipts: usize,
    pub max_wait: Duration,
    pub finished_retention: Duration,
    pub termination_grace: Duration,
    pub max_termination_grace: Duration,
    pub output_drain_timeout: Duration,
    pub max_file_read_bytes: usize,
    pub max_file_write_bytes: usize,
    pub max_file_mutation_receipts: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_active_executions: 64,
            max_retained_executions: 1024,
            max_retained_output_bytes: 1024 * 1024,
            max_output_bytes_per_response: 64 * 1024,
            max_queued_input_bytes: 1024 * 1024,
            max_input_receipts: 4096,
            max_interrupt_receipts: 1024,
            max_wait: Duration::from_secs(300),
            finished_retention: Duration::from_secs(15 * 60),
            termination_grace: Duration::from_secs(2),
            max_termination_grace: Duration::from_secs(30),
            output_drain_timeout: Duration::from_secs(1),
            max_file_read_bytes: MAX_FILE_BYTES,
            max_file_write_bytes: MAX_FILE_BYTES,
            max_file_mutation_receipts: 4096,
        }
    }
}

impl Limits {
    pub(crate) fn validate(&self) -> Result<()> {
        if [
            self.max_active_executions,
            self.max_retained_executions,
            self.max_retained_output_bytes,
            self.max_output_bytes_per_response,
            self.max_queued_input_bytes,
            self.max_input_receipts,
            self.max_interrupt_receipts,
            self.max_file_read_bytes,
            self.max_file_write_bytes,
            self.max_file_mutation_receipts,
        ]
        .contains(&0)
        {
            return Err(Error::invalid("resource limits must be positive"));
        }
        if self.max_queued_input_bytes > u32::MAX as usize {
            return Err(Error::invalid("input queue limit must fit in u32"));
        }
        if self.termination_grace > self.max_termination_grace {
            return Err(Error::invalid("default termination grace exceeds maximum"));
        }
        if self.max_file_write_bytes > self.max_file_read_bytes {
            return Err(Error::invalid(
                "file write limit cannot exceed the file read limit",
            ));
        }
        if self.max_file_read_bytes > MAX_FILE_BYTES {
            return Err(Error::invalid(
                "file byte limits exceed the protocol-safe maximum",
            ));
        }
        Ok(())
    }
}
