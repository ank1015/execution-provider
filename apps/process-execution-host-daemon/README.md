# process-execution-host-daemon

A foreground host daemon for Linux, macOS, and Windows. It runs as the local user's
account, embeds `process-execution-core`, and maintains an authenticated outbound
WebSocket to an execution gateway. It opens no inbound network server.

The default gateway is `https://execution.acentric.dev`. Register this installation
before running it. The gateway issues a machine credential, which the daemon stores
privately and uses for subsequent connections. Use `--gateway-url` during registration
to connect to a different deployment.

## Install and configure

With Rust installed, run from the workspace root:

```sh
cargo install --path apps/process-execution-host-daemon --locked
```

Alternatively, `cargo build --workspace --release --locked` creates the daemon in
`target/release/`; append `.exe` on Windows. Merges to `main` publish Linux x86-64,
Windows x86-64, and universal macOS archives at `https://downloads.acentric.dev`.
Use `latest/` for the newest build
or `releases/COMMIT_SHA/` for an immutable build. Each location includes
`checksums.sha256` and `manifest.json`. For example:

```sh
curl --fail --remote-name https://downloads.acentric.dev/latest/process-execution-host-daemon-linux-x86_64.tar.gz
curl --fail --remote-name https://downloads.acentric.dev/latest/checksums.sha256
sha256sum --check --ignore-missing checksums.sha256
tar -xzf process-execution-host-daemon-linux-x86_64.tar.gz
```

Windows requires Windows 10 version 1809+ or Windows Server 2019+.

Create a machine with the gateway user API (`POST /v1/machines`). Supply its returned
machine ID and single-use registration token to the daemon, then run:

```sh
process-execution-host-daemon register \
  --machine-id 00000000-0000-0000-0000-000000000001 < /path/to/private-token.txt
process-execution-host-daemon connect
```

The example machine UUID is a placeholder. The registration token is one ASCII line on stdin;
it is never accepted as a command-line argument or printed. Keep any source token file
private and remove it when it is no longer needed. Registration tokens expire after
15 minutes. If registration is interrupted after the token is consumed, request a new
one through `POST /v1/machines/:machineId/registration` and repeat `register`.

PowerShell can provide the same input:

```powershell
Get-Content -Raw C:\private\token.txt | process-execution-host-daemon.exe register `
  --machine-id 00000000-0000-0000-0000-000000000001
process-execution-host-daemon.exe run
```

The gateway connection URL is derived from the base URL as
`/v1/machines/MACHINE_ID/connect` (retaining any base path). HTTPS becomes WSS. TLS validates
the gateway using the operating system's trusted roots. The host credential identifies
the device to the gateway.

`connect` installs the daemon as a service for the current user, starts it immediately,
and starts it again at future logins. `run` remains available for foreground use and
troubleshooting. `run --gateway-url URL` overrides the configured base URL, but it must match the gateway
bound to the stored credential. To change gateways, stop the daemon and register with the new gateway. There is no automatic credential forwarding or
fallback to an unrelated server.

## CLI and state

| Command | Purpose |
|---|---|
| `register [--gateway-url URL] --machine-id UUID` | Exchange the registration token from stdin and save the machine credential |
| `configure [--gateway-url URL] --machine-id UUID` | Save an already-issued machine credential from stdin (`--host-id` remains an alias) |
| `run [--gateway-url URL] [--config FILE]` | Maintain the connection and serve execution requests |
| `connect [--config FILE]` | Install, enable, and start the current binary as this user's login service |
| `disconnect` | Stop and disable the login service while retaining registration |
| `update [--manifest-url URL]` | Download, verify, and install the latest published daemon; restart it only when it was running |
| `status` | Print configuration presence, whether the state lock is held, and the last runtime/connection status |
| `version` | Print binary, execution protocol, OS, and architecture versions |

All commands accept `--state-dir DIRECTORY`. Its default is the user's local application
data directory plus `process-execution-host-daemon`:

- Linux: `$XDG_DATA_HOME`, normally `~/.local/share`.
- macOS: `~/Library/Application Support`.
- Windows: the user's local AppData directory.

Choose a dedicated application directory. The daemon sets it private to the current
account: mode `0700` on Unix or an inheritable current-user-only DACL on Windows.
Credentials use atomic replacement and mode `0600` on Unix. The directory stores
`identity.json`, `credential.json`, `status.json`, and `daemon.lock`. Only one daemon or
registration/configuration operation can own the directory at a time.

Status contains no credential. `running: false` means the saved connection snapshot is
historical. An installation ID survives restarts; a runtime generation does not. Execution
records, request receipts, and bounded journals remain in memory. Complete
`execution.run` output files use the private state directory's `run-output` subdirectory
unless runtime configuration selects another location.
Existing profiles keep working; the stored `host_id` identifies the gateway machine.

Logs go to stderr. `run` leaves stdout empty. Configuration/authentication/protocol
failures exit with code 2; a local Ctrl-C or Unix SIGTERM shuts down cleanly with code 0.

## Runtime configuration

`run --config host.json` accepts:

```json
{
  "gateway_url": "https://gateway.example.com",
  "execution": {
    "cwd": ".",
    "run_output_directory": "/private/path/to/run-output",
    "env": {"EXAMPLE": "value"},
    "shell_snapshot": {"enabled": true, "max_cached_scopes": 64},
    "limits": {
      "max_active_executions": 64,
      "max_retained_output_bytes": 1048576,
      "termination_grace_ms": 2000
    }
  }
}
```

Fields are optional; unknown fields are rejected. `execution` accepts the same
configuration as [process-execution](../process-execution/README.md#configuration).
Relative paths resolve against the daemon's launch directory. The gateway selection
order for `run` is CLI, JSON configuration, then stored registration. `register` and
`configure` default to `https://execution.acentric.dev` when `--gateway-url` is omitted.

For local development, `register`, `configure`, and `run` support `--allow-insecure-loopback`.
The run configuration can also set `allow_insecure_loopback: true`. This permits HTTP/WS
only for loopback addresses or `localhost`; there is no option to disable TLS verification.

## Connection and execution behavior

The daemon sends a versioned hello with host identity, runtime generation, shell, and
PTY/interrupt capabilities. The gateway then sends the welcome and execution requests
defined in [process-execution-protocol](../../packages/process-execution-protocol/README.md).
All execution and bounded filesystem operations and sequential/parallel batches are supported. `runtime.shutdown`
is restricted to local administration and is rejected over this connection.

Connect, welcome, and write deadlines are 10 seconds. Heartbeats run independently of
execution waits and response writes. Liveness uses both wall and monotonic clocks to
handle suspension and clock changes. Missed heartbeats, dropped TCP connections, and
network failures reconnect with jittered exponential backoff from approximately one
second to a 30-second cap. A connection lasting at least 30 seconds resets the backoff.

There are at most 32 ordinary outstanding request envelopes, plus four reserved slots
for single `execution.terminate_run` requests, 32 operations per batch, and eight
parallel operations per batch. Output queues and messages are bounded. Excess requests
receive a resource-limit error; a stalled connection is dropped. Slow or disconnected
gateways cannot block the core's output collection indefinitely.

`execution.run` uses closed-stdin pipes, applies its optional process timeout on the
host, streams complete combined output to a private file, and returns a bounded tail
only after cleanup and output drain finish. `execution.terminate_run` cancels it by
stable `run_id`; early cancellation is retained across admission races. Network loss
does not itself terminate a run.

Accepted commands and batches belong to the daemon. When both peers negotiate
`request_recovery`, the daemon records acceptance before dispatch, retains each complete
operation/batch response across reconnects, and releases it only after the gateway
acknowledges durable completion. Reconnecting resends retained responses; recovery queries
return a pending receipt, the saved response, or `missing`. They never execute work.
Repeating the same unacknowledged request ID and normalized input returns its receipt;
changed input closes the connection. The gateway never redispatches an ambiguous job.

Receipt storage is bounded to 128 envelopes and 256 MiB, including an 8 MiB response
reservation for each pending envelope. Capacity pressure rejects new requests; it does
not evict unacknowledged responses. Receipts have no in-runtime expiry. The gateway waits
up to 24 hours for a disconnected runtime to recover its responses, then settles remaining
jobs as `unknown`. A daemon restart changes generation and makes old receipts unavailable.
An acknowledgement for a terminal job releases its receipt even if that job was already
settled as `unknown`; terminal results are immutable.

Legacy gateways that omit recovery negotiation keep connection-scoped responses. With
those peers, reconnects preserve command/batch progress but lose pending network responses.
Callers can still use list/get/observe and per-operation retry identities. No network
timeout serves as a command execution deadline. No automatic command observation or
command-exit notification is added.

HTTP 401/403, revocation, replacement, or an incompatible protocol stops reconnection,
records `needs_configuration`, and shuts down owned processes. The credential stays
stored for inspection/replacement. Local shutdown also terminates owned work and waits
for cleanup. Abrupt daemon termination cannot perform graceful cleanup or recover its
in-memory records on restart.

## User service and updates

`connect` manages the native, non-elevated startup mechanism for the current user:

- Linux: a systemd user unit at
  `~/.config/systemd/user/process-execution-host.service`.
- macOS: a LaunchAgent named `dev.acentric.process-execution-host-daemon`.
- Windows: a limited Task Scheduler task named `Acentric Process Execution Host`.

The generated service always uses absolute paths for the current executable, state
directory, and optional `--config` file. Move the binary before running `connect`; moving
it afterward leaves the service pointing at the old path. `disconnect` is idempotent and
does not delete credentials. The daemon performs network reconnection itself. Service
restart loops are disabled for configuration, authentication, and protocol failures that
exit with code 2. On Windows, Task Scheduler starts a hidden PowerShell runner so the
background daemon does not open a console window. `connect` waits for the daemon process
to acquire its state lock and returns an error if startup does not complete within 15
seconds.

`connect` also snapshots the caller's non-empty `PATH` into the generated user service.
This lets background executions discover the same tools as the shell that installed the
service without assuming platform- or package-manager-specific directories. No other
shell variables are copied. Run `connect` again after changing the list of directories in
`PATH`; installing another executable into an existing directory needs no refresh.
Foreground `run` inherits its caller's environment directly. An explicit
`execution.env.PATH` in the runtime configuration takes precedence over the captured
service value, which is useful when `connect` is invoked from a GUI or automation with a
minimal environment.

`update` reads `https://downloads.acentric.dev/latest/manifest.json`, chooses the archive
for the current OS and architecture, verifies the declared byte length and SHA-256 digest,
extracts only the daemon executable, and atomically replaces the running installation.
Only HTTPS manifest and artifact URLs are accepted. If the user service is running, it is
stopped for replacement and started again; a disconnected installation remains
disconnected. Windows completes replacement in a short-lived helper process after the
CLI exits, because Windows does not allow an executing `.exe` to replace itself.

Published automatic-update targets are Linux x86-64, universal macOS (Intel and Apple
Silicon), and Windows x86-64. A user-writable installation location such as
`~/.local/bin` is recommended. The managed user service requires no administrator
privileges; machine-wide service installation is intentionally out of scope.

## Permissions and verification

Pairing grants the trusted gateway ongoing execution as the daemon's OS account.
There is no per-command approval UI. Commands have that account's file and network access;
a working directory is not a filesystem sandbox. Credentials are not injected into child
environments, but commands running as the same account can access its private files.

The gateway must enforce caller-to-host authorization. Host credentials authenticate the
device; they do not replace the gateway's user permissions. Sandbox expiration, activity
leases, and pause/resume belong to future outer adapters.

Integration tests start the real daemon against a mock gateway and verify authentication,
batch dispatch, missed-heartbeat reconnection, retained execution/output, batch continuation,
request receipt replay/acknowledgements, generation checks, revocation, incompatible versions, exclusive ownership, credential
binding/privacy, and local shutdown. The CI matrix runs the workspace suite and produces
binary artifacts on Linux, macOS, and Windows.
