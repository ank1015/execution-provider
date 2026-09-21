# process-execution

A local process supervisor and a small RPC client for Linux, macOS, and Windows.
The binary exposes [process-execution-core](../../packages/process-execution-core/README.md)
through private local IPC. A long-lived `serve` process owns commands; short-lived
`rpc` processes submit requests and collect responses.

## Build

From the workspace root, with stable Rust installed:

```sh
cargo build --release -p process-execution --locked
```

The executable is `target/release/process-execution` on Linux/macOS, or
`target/release/process-execution.exe` on Windows. Build on the destination OS and
architecture.

Merges to `main` that affect either execution binary publish Linux x86-64, Windows
x86-64, and universal macOS archives at `https://downloads.acentric.dev`.
Immutable releases live under
`releases/COMMIT_SHA/`; `latest/` points to the newest release. Each directory has
SHA-256 checksums and a JSON manifest. For example:

```sh
curl --fail --remote-name https://downloads.acentric.dev/latest/process-execution-linux-x86_64.tar.gz
curl --fail --remote-name https://downloads.acentric.dev/latest/checksums.sha256
sha256sum --check --ignore-missing checksums.sha256
tar -xzf process-execution-linux-x86_64.tar.gz
```

Windows requires Windows 10 version 1809+ or Windows Server 2019+ for ConPTY support.
The following examples assume the executable is on `PATH`.

## Run

Linux/macOS, in one terminal:

```sh
mkdir -p "$HOME/.process-execution"
chmod 700 "$HOME/.process-execution"
process-execution serve --endpoint "$HOME/.process-execution/runtime.sock"
```

In another terminal:

```sh
process-execution health --endpoint "$HOME/.process-execution/runtime.sock"
printf '%s\n' '{"protocol_version":4,"request_id":"request-1","operation":"execution.start","params":{"start_id":"hello-1","command":{"type":"program","executable":"echo","args":["hello"]},"wait_ms":1000}}' |
  process-execution rpc --endpoint "$HOME/.process-execution/runtime.sock"
```

Windows, in PowerShell:

```powershell
process-execution.exe serve --endpoint '\\.\pipe\process-execution'
```

In another PowerShell terminal:

```powershell
process-execution.exe health --endpoint '\\.\pipe\process-execution'
'{"protocol_version":4,"request_id":"request-1","operation":"execution.start","params":{"start_id":"hello-1","command":{"type":"shell","script":"echo hello"},"wait_ms":1000}}' |
  process-execution.exe rpc --endpoint '\\.\pipe\process-execution'
```

`serve` runs in the foreground. Start it through the host's service manager or sandbox
startup mechanism when it must outlive a provider exec call. Its logs go to stderr;
it does not print process output to the server's stdout. The endpoint is required;
`--socket` is an alias for `--endpoint`.

Unix socket parents must exist. Use a directory owned by the serving user. The socket
has mode `0600`, and accepted peers must have the same effective UID. A lock file
beside the socket coordinates supervisor ownership; it intentionally remains after
shutdown. A stale owned socket can be recovered on the next start. Windows uses a
local named pipe whose ACL permits the current user and rejects remote clients.

This endpoint grants local command execution as the serving user. It is not a network
service or an OS isolation boundary.

## CLI contract

| Command | Behavior |
|---|---|
| `serve --endpoint ENDPOINT [--config FILE] [--cwd DIR]` | Own the runtime and serve concurrent requests |
| `rpc --endpoint ENDPOINT [--timeout-ms N]` | Read one JSON request from stdin and print one response |
| `health --endpoint ENDPOINT [--timeout-ms N]` | Call `runtime.info`, returning identity and capabilities |
| `version` | Print binary version, protocol version, OS, and architecture as JSON |

RPC stdin is UTF-8 JSON on one line. A newline submits the request without waiting for
EOF; EOF can also end a single frame. Responses are one JSON line. Direct socket/pipe
clients use the same framing, with one request and response per connection. Both
directions have an 8 MiB frame limit, including the newline. Keep output/page sizes
below that limit, accounting for base64 and execution metadata.

`rpc` defaults to a 360,000 ms connection/exchange timeout; `health` defaults to
5,000 ms. These timeouts begin after stdin is read. The server allows 10 seconds to
read a request or write a response and at most 128 concurrent handlers. Excess
connections are closed and can be retried. Observation time is controlled separately
by each request's `wait_ms` and the core's configured maximum wait.

Successful RPC responses exit with code 0. A remote error prints its JSON response
and exits with code 1. Local parsing, transport, or timeout errors print a diagnostic
to stderr and exit with code 1; CLI usage errors exit with code 2. A managed command's
nonzero exit code is an execution result, not an RPC failure.

## Protocol version 4

The shared [process-execution-protocol](../../packages/process-execution-protocol/README.md)
package owns request/response types and dispatch. Single-operation requests remain
unchanged. It also supports batches through `operations: [...]` and
`mode: "sequential"` (default) or `"parallel"`. See the shared package for batch
limits, retry semantics, and examples. A batch containing errors or skipped operations
prints the per-operation results and exits with code 1.

Every request has this envelope:

```json
{
  "protocol_version": 4,
  "request_id": "unique-correlation-id",
  "expected_generation_id": "UUID-from-health",
  "operation": "execution.get",
  "params": {
    "handle": {"id": "execution-UUID", "generation_id": "UUID-from-health"}
  }
}
```

Replace the UUID placeholders with actual values. `expected_generation_id` is optional;
include it to reject requests after a supervisor restart, including retried starts and
shutdown requests. Handles always contain their generation. `request_id` correlates a
response; it does not deduplicate operations.

Successful responses have `status: "ok"` and `result`:

```json
{
  "protocol_version": 4,
  "request_id": "unique-correlation-id",
  "generation_id": "supervisor-UUID",
  "status": "ok",
  "result": {}
}
```

Errors have `status: "error"` and `error: {"code": "invalid_argument", "message": "..."}`
instead of `result`. Error codes use the core's snake_case names. Malformed requests
can have a null response `request_id` when it cannot be recovered.

| Operation | `params` | `result` |
|---|---|---|
| `runtime.info` | Omit | `{runtime, binary}`; runtime includes generation, default shell, and PTY/interrupt capabilities |
| `runtime.shutdown` | Omit | `{shutdown: true}`, after managed executions finish cleanup |
| `execution.start` | See below | Observation |
| `execution.run` | See below | Final execution, complete output-file metadata, and bounded tail output |
| `execution.terminate_run` | `{run_id, grace_period_ms?}` | Pending, terminating, or finished cancellation receipt |
| `execution.get` | `{handle}` | Execution snapshot |
| `execution.observe` | `{handle, after_cursor?, wait_ms?, return_when?, max_output_bytes?}` | Observation |
| `execution.write_input` | `{handle, input_id, data_base64}` | `{input_id, accepted_bytes}` |
| `execution.close_input` | `{handle}` | `{stdin_state: "open" or "closing" or "closed"}` |
| `execution.interrupt` | `{handle, operation_id}` | Execution snapshot acknowledging interrupt |
| `execution.terminate` | `{handle, grace_period_ms?}` | Execution snapshot acknowledging termination |
| `execution.resize_terminal` | `{handle, rows, cols}` | Execution snapshot with terminal dimensions |
| `execution.list` | `{state?, labels?, limit?, page_cursor?}` | `{executions, next_page_cursor}` |
| `filesystem.get_metadata` | `{path, cwd?}` | File type, size, symlink, and modification metadata |
| `filesystem.read_file` | `{path, cwd?, max_bytes?}` | Metadata, padded-base64 bytes, and SHA-256 |
| `filesystem.write_file` | `{mutation_id, path, cwd?, data_base64, create_parent_directories?, mode?, precondition?}` | Conditional or overwrite atomic-replacement receipt |
| `filesystem.remove_file` | `{mutation_id, path, cwd?, precondition}` | Conditional file-removal receipt |

The `?` suffix in the table denotes an optional field, not part of its name.
Start parameters:

```json
{
  "start_id": "unique-start-id",
  "command": {"type": "program", "executable": "cargo", "args": ["build"]},
  "cwd": "relative/to/server-cwd",
  "env": {"EXAMPLE": "value"},
  "shell_snapshot": {"scope_id": "stable-session-id"},
  "io": {"type": "pipes", "stdin": false},
  "wait_ms": 1000,
  "max_output_bytes": 65536,
  "labels": {"owner": "example"}
}
```

Only `start_id` and `command` are required. Defaults are the configured cwd/environment,
closed piped stdin, zero wait, the core output limit, and no labels. `program` passes
arguments directly; `args` defaults to an empty array. For an interactive terminal,
use `io: {"type": "pty", "rows": 24, "cols": 80}`.

Run parameters are deliberately non-interactive:

```json
{
  "run_id": "unique-run-id",
  "command": {"type": "program", "executable": "cargo", "args": ["test"]},
  "cwd": "relative/to/server-cwd",
  "env": {"EXAMPLE": "value"},
  "shell_snapshot": {"scope_id": "stable-session-id"},
  "timeout_ms": 300000,
  "max_output_bytes": 65536,
  "labels": {"owner": "example"}
}
```

Only `run_id` and `command` are required. Omitted `timeout_ms` means no process
timeout. The operation uses closed-stdin pipes and returns only after process cleanup
and output draining. Its `output` is the last `max_output_bytes` raw bytes, 64 KiB by
default and at most 1 MiB. `output_truncated` reports whether earlier or incomplete
bytes are absent. `output_file` identifies the complete combined machine-local `.log`
with `artifact_id`, absolute `path`, `size_bytes`, `sha256`, `complete`, and `expires_at`.
The file contains raw stdout/stderr bytes in capture order; preview chunks retain their
stream identity.

Submit `execution.terminate_run` separately to cancel by stable `run_id`. An early
cancellation is retained for a racing run and prevents it from launching. Repeated
cancellation is safe; the first timeout or explicit termination cause determines the
final result.

`shell_snapshot` is optional. When supplied, the Unix host loads and caches the selected
user shell's interactive profile for that stable scope. Its environment and shell state
are applied before the command, while values in `env` still win. Capture failures do not
fail the command. Use a session/workspace identity as `scope_id`; do not put shell code or
secrets in it.

For shell syntax, use `command: {"type": "shell", "script": "echo hello"}`. The optional
`shell` object specifies `executable` and `kind` (`sh`, `bash`, `zsh`, `power_shell`,
or `cmd`). Otherwise the configured/discovered shell is used. `login` defaults to
false. Shell scripts must match the selected shell's language; see the core's shell
selection and invocation rules.

An observation contains `execution`, `output`, `next_cursor`, `has_more`, `output_gap`,
and `return_reason`. Each output chunk is `{stream, data_base64}`, where `stream` is
`stdout`, `stderr`, or `terminal`. Bytes use standard padded base64; decode them before
interpreting text. Input uses the same encoding and adds no newline automatically.

Pass `next_cursor` as `after_cursor` to read onward. Cursors are opaque strings; store
and return them unchanged. Omitting a cursor reads from the earliest retained output.
`wait_ms` defaults to zero. `return_when` is `activity` by default, or
`finished_or_timeout`. A response may also return early when its output limit fills.
Keep reading while `has_more` is true, even when the execution is finished.

Snapshots use states `starting`, `running`, `stopping`, and `finished`. The final
`result.reason` is `exited`, `terminated`, `timed_out`, `start_failed`, or `lost`. Timestamps are
objects containing `secs_since_epoch` and `nanos_since_epoch`; pending timestamps and
results are null. Environment values are not included in snapshots.

Listing defaults to `state: "active"`, no label filter, and `limit: 50`. Other states
are `finished` and `all`. Preserve the filters when following `next_page_cursor`.

Filesystem paths are absolute or resolve relative to `cwd`, which itself resolves against
the configured runtime cwd. Reads are whole-file and fail rather than truncate when the
requested/configured byte limit is exceeded. File bytes use standard padded base64. A
conditional write or remove precondition is either `{"type":"missing"}` or
`{"type":"sha256","sha256":"64-lowercase-hex-characters"}`. The write mode defaults
to `"conditional"` and requires that precondition, preserving existing requests.
Use `"mode":"overwrite"` without a precondition to replace a file without reading
its old hash. The new content must fit the configured write limit (5 MiB by default),
even if the old file is larger. Both modes can create parent directories in the same
operation with `create_parent_directories: true`.

Overwrite follows the final symlink to its target, including a dangling link, and
leaves the link in place. Conditional writes retain their existing behavior: they read
through a symlink, then replace the link itself when a write is needed. A write stages
the new file beside the destination and renames it into place. Readers see either the
old or new file; other hard links retain the old inode and contents. If the destination
already has the requested content and fits the read limit, no replacement occurs and
the receipt says `already_applied`. The rename does not provide a power-loss durability
guarantee or a compare-and-swap against processes outside the runtime.

Mutations affect regular files only; removal is never recursive. Reuse a `mutation_id`
only with the same path, bytes, directory option, and mode/precondition. The runtime
retains the first result for that ID while its generation lives. After a restart, a
matching final file can return `already_applied`; a large old file is replaced without
first comparing its contents.

## Lifetime, retries, and shutdown

A disconnect, killed `rpc` process, observation timeout, or failed response delivery
does not cancel accepted work. An error delivering a response does not prove that
the requested action did not occur. Reconnect to the same supervisor and retry with
the same `start_id`, `run_id`, `input_id`, or interrupt `operation_id`. Reuse IDs only for the
same action and payload. Deduplication lasts as long as the core retains the record.

Termination and interrupt responses acknowledge a request; observe until `finished`
to confirm completion. Piped stdin can be closed after queued input drains. PTYs do
not support pipe-style EOF. Windows pipe interrupt is unsupported; use the capability
flags returned by `health` and terminate when appropriate.

`runtime.shutdown`, Ctrl-C, or Unix SIGTERM shuts down the core, waits for managed
process cleanup, and releases the endpoint. For example, submit this through `rpc`:

```json
{"protocol_version":4,"request_id":"shutdown-1","operation":"runtime.shutdown"}
```

Abruptly killing the supervisor cannot run graceful shutdown. Output, retry records,
and execution records are in memory and are not restored on restart. A restart always
creates a new generation. Execution expiration and sandbox pause/resume remain the
responsibility of the outer host/provider.

## Configuration

`serve --config config.json` accepts a JSON object; omitted fields retain core defaults:

```json
{
  "cwd": ".",
  "run_output_directory": ".process-execution-runs",
  "env": {"EXAMPLE": "value"},
  "shell_snapshot": {
    "enabled": true,
    "max_cached_scopes": 64,
    "capture_timeout_ms": 10000,
    "max_capture_bytes": 4194304,
    "max_state_bytes": 524288,
    "retry_backoff_ms": 1000
  },
  "limits": {
    "max_active_executions": 64,
    "max_retained_executions": 1024,
    "max_retained_output_bytes": 1048576,
    "max_output_bytes_per_response": 65536,
    "max_queued_input_bytes": 1048576,
    "max_input_receipts": 4096,
    "max_interrupt_receipts": 1024,
    "max_wait_ms": 300000,
    "finished_retention_ms": 900000,
    "run_output_retention_ms": 86400000,
    "termination_grace_ms": 2000,
    "max_termination_grace_ms": 30000,
    "output_drain_timeout_ms": 1000,
    "max_file_read_bytes": 5242880,
    "max_file_write_bytes": 5242880,
    "max_file_mutation_receipts": 4096
  }
}
```

An optional `default_shell` uses the same `{executable, kind}` object as commands.
`--cwd` overrides the JSON cwd. Relative configuration paths and cwd resolve against
the launch directory. Unknown configuration fields are rejected so typos do not silently
change behavior. The core README describes each limit and platform process ownership.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The binary integration tests run real `serve` and `rpc` subprocesses, covering pipes,
PTYs, shell selection, cursor replay, binary input/output, deduplication, disconnects,
generation checks, endpoint ownership, and shutdown. Native tests on each OS are
required to validate runtime behavior; cross-compilation only checks compatibility.
