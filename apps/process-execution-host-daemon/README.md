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
process-execution-host-daemon run
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

`run --gateway-url URL` overrides the configured base URL, but it must match the gateway
bound to the stored credential. To change gateways, stop the daemon and register with the new gateway. There is no automatic credential forwarding or
fallback to an unrelated server.

## CLI and state

| Command | Purpose |
|---|---|
| `register [--gateway-url URL] --machine-id UUID` | Exchange the registration token from stdin and save the machine credential |
| `configure [--gateway-url URL] --machine-id UUID` | Save an already-issued machine credential from stdin (`--host-id` remains an alias) |
| `run [--gateway-url URL] [--config FILE]` | Maintain the connection and serve execution requests |
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
records, request receipts, and output remain in memory, not in the state directory.
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
    "env": {"EXAMPLE": "value"},
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

There are at most 32 outstanding request envelopes, 32 operations per batch, and eight
parallel operations per batch. Output queues and messages are bounded. Excess requests
receive a resource-limit error; a stalled connection is dropped. Slow or disconnected
gateways cannot block the core's output collection indefinitely.

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

## Start automatically under the user account

Register and verify the daemon first, then use the OS's user startup facilities. The
binary itself does not install or modify a service automatically.

On Linux, create `~/.config/systemd/user/process-execution-host.service` with absolute
paths for the executable and configuration:

```ini
[Unit]
Description=Process execution host

[Service]
ExecStart=/absolute/path/process-execution-host-daemon run --config /absolute/path/host.json
Restart=on-failure
RestartSec=5
RestartPreventExitStatus=2

[Install]
WantedBy=default.target
```

Then run `systemctl --user daemon-reload` and
`systemctl --user enable --now process-execution-host.service`. Stop it with
`systemctl --user disable --now process-execution-host.service`.

On macOS, use a user LaunchAgent with `RunAtLoad: true`, absolute `ProgramArguments`
for the binary and `run --config /absolute/path/host.json`, and `KeepAlive: false`.
The daemon handles network retries internally. Load/unload it with `launchctl bootstrap`
and `launchctl bootout` for the user's GUI domain. Keeping unconditional restart disabled
prevents a revoked credential from producing a service restart loop.

On Windows, create a Task Scheduler task for the current user at logon, using the installed
`.exe` and arguments `run --config "C:\absolute\host.json"`. Do not request elevated
privileges. Disable the task before replacing configuration or removing the binary.

These are user-session startup options. Machine-wide services, signed installers, and
automatic updates are separate deployment work.

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
