# execution-gateway

Rust HTTP/WebSocket gateway for user-owned machines and durable execution jobs.
It provides admin-created users, scoped API keys, machine registration, single
operations and batches through one job endpoint, retained responses, and signed
lightweight job-result webhooks. PostgreSQL is the durable store and queue.

The gateway does not monitor command completion after an operation returns.
For example, a successful `execution.start` job may contain a running execution
handle. The caller submits `execution.observe` later when it wants more output.

- [API reference](docs/api-reference.md)
- [Architecture](docs/architecture.md)
- [Shared execution protocol](../../packages/process-execution-protocol/README.md)

## Build and run

Build from the workspace root:

```sh
cargo build -p execution-gateway --release --locked
target/release/execution-gateway version
```

Append `.exe` on Windows. The binary embeds its SQL migrations. PostgreSQL 14+
is required; the integration suite uses PostgreSQL 17.

Configure a dedicated database and generate secrets outside command arguments:

```sh
export DATABASE_URL=postgresql://localhost/execution_gateway
export ADMIN_API_KEY="$(openssl rand -hex 32)"
export ENCRYPTION_KEY="$(openssl rand -hex 32)"
export WEBHOOK_ALLOWED_ORIGINS=https://your-backend.example.com

target/release/execution-gateway migrate
target/release/execution-gateway serve
```

Retain the generated admin/encryption secrets in your deployment's secret store.
Do not regenerate the encryption key during restart: existing webhook secrets
would become unreadable. Environment files are not loaded automatically.
`migrate` needs only `DATABASE_URL`; `serve` requires the other required settings.
Use HTTPS termination in front of HTTP/WebSocket traffic. The daemon validates
TLS; insecure transport is supported only through its explicit loopback test mode.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | Required | PostgreSQL URL; configure TLS as required by the deployment. |
| `ADMIN_API_KEY` | Required | Random 32–512 printable, non-whitespace ASCII characters. |
| `ENCRYPTION_KEY` | Required | 64 hexadecimal characters encoding a persistent 32-byte key. |
| `LISTEN_ADDR` | `127.0.0.1:3000` | HTTP listen address; use `0.0.0.0:3000` behind a container ingress if needed. |
| `WEBHOOK_ALLOWED_ORIGINS` | Empty | Comma-separated canonical trusted HTTPS origins, with no trailing slash. Empty denies callback delivery. |
| `REQUEST_RETENTION_DAYS` | `7` | Input retention after terminal completion; range 1–365. |

The callback URL is saved once per user. A stored URL does not itself authorize
outbound requests: its origin must also be in the operator allowlist. Redirects
are rejected. Trust the configured origins/DNS and apply appropriate deployment
egress controls. No external LLM or sandbox provider is required.

## Processes and deployment

Run one `serve` process per database. It owns the HTTP API, machine sockets,
dispatch tasks, input cleanup, terminal-job listener, and an independent webhook
delivery loop. A dedicated PostgreSQL session advisory lock rejects a second gateway
instance.
Loss of that ownership connection shuts down serving. Multi-instance routing
is not implemented.

Run migrations explicitly before deploying a new binary. Startup never applies
migrations. `/healthz` checks liveness and `/readyz` checks database/job-table
availability; neither promises that an individual machine is online.

Production deployment files live in [`deploy`](deploy). Changes to the gateway,
its migrations, its shared protocol, or those deployment files trigger
`Deploy execution gateway` after they reach `main`. The workflow builds and pushes
an immutable image, runs embedded SQLx migrations while the current gateway still
serves, replaces the container, and checks both health endpoints. A failed startup
restores the previous image. Database migrations remain applied, so every migration
must be additive and compatible with both the previous and new gateway versions.
Keep DDL short; PostgreSQL locks taken by a migration can affect live requests even
though the old gateway remains online during migration.

The current single-owner design requires a short handover instead of overlapping
gateway replicas. Accepted HTTP requests receive up to the 15-second application
shutdown deadline to finish. Caddy remains online, but requests arriving between
the old process exiting and the new process becoming ready can receive `502`.
Machine WebSockets close and reconnect automatically. Daemons that negotiated
response recovery reconcile accepted operations from retained receipts without
executing them again; daemon-side processes continue running. Older daemons can
leave an interrupted response in the `unknown` state. True zero-downtime gateway
deployment requires multi-instance connection ownership and routing.

GitHub authenticates through the `execution-provider` Workload Identity provider
and the dedicated `execution-gateway-deployer` service account. Runtime values are
read on the VM from the `execution-gateway-2-database-url`,
`execution-gateway-2-admin-api-key`, and `execution-gateway-2-encryption-key`
Secret Manager entries. GitHub stores no database or gateway credentials.

Logs go to stderr and omit credentials, command payloads, output, and raw database
or callback errors. CLI configuration/startup failures exit with code 2.
SIGINT/SIGTERM initiates shutdown: stop admission, close machine sockets without
revoking them, stop workers, and allow up to 15 seconds for cleanup. Local daemon
processes continue. Interrupted webhook claims recover after their 60-second
leases expire; duplicate delivery remains possible.

Database connections have five-second acquisition/statement limits, three-second
lock waits, and a 30-second idle-transaction limit. The pool has ten connections,
one of which is reserved by the terminal-job listener, plus the dedicated ownership
connection. Bounded wait requests retain neither a database transaction nor a pool
connection. No transaction is held while awaiting daemon results or sending callbacks.

## Current daemon compatibility

The daemon's `register` command exchanges the single-use registration token for a
private machine credential. Its stable installation ID is created automatically:

```sh
process-execution-host-daemon register \
  --gateway-url https://gateway.example.com \
  --machine-id MACHINE_UUID < /path/to/private-registration-token.txt
process-execution-host-daemon run
```

See the [daemon README](../process-execution-host-daemon/README.md) for Windows,
local development, and existing credential profiles. Both `/v1/machines/:id/connect`
and the legacy `/v1/hosts/:id/connect` path are accepted; hello retains `host_id`.

Current peers negotiate bounded request receipt recovery:

- A daemon acceptance moves the job from `dispatching` to `waiting_response`.
- The daemon preserves operation/batch responses until the gateway commits the
  result and its notification atomically, then acknowledges persistence.
- Reconnects and gateway restarts recover receipts from the same runtime generation.
  Neither recovery queries nor retries of retained responses execute another operation.
- Missing receipts, a replaced runtime, or an expired 24-hour disconnected recovery
  window settle the job as `unknown`. Late results do not rewrite terminal jobs.
- Queued work that was never dispatched may still run, but fails after 30 seconds
  without dispatch. That deadline does not terminate a process.
- Older daemons without negotiated recovery retain the original behavior: lost
  responses become `unknown` on disconnect or gateway restart.

The daemon's receipt storage is in memory: at most 128 requests and 256 MiB, including
8 MiB reserved per pending result. Daemon restart loses its receipts and process/output
records. Operation response recovery does not monitor command completion.

Run `execution-gateway migrate` before starting the updated gateway; migration 0002 adds
the per-job recovery capability and disconnected recovery deadline, while migration
0003 adds versioned webhook payload selection.

Disabling a user/machine blocks new dispatches while preserving connections and
already-dispatched work. Revoking a user API key does not cancel accepted jobs.
Deleting a machine or replacing its machine credential revokes its old connection;
the current daemon then shuts down its owned processes. Gateway shutdown and
ordinary connection loss do not revoke the machine.

## Limits and retention

- Management JSON: 16 KiB. Job JSON: 8 MiB, including room for transport metadata.
- Up to 256 outstanding jobs per user and 32 dispatched requests per machine.
- Batches: 1–32 operations; parallel batches run up to eight operations at once.
- Handshake/write timeout: ten seconds. Heartbeat: 15 seconds; liveness: 45 seconds.
- Bounded job-result wait: five minutes maximum.
- Concurrent job-result waits: 64 per user and 1,024 per gateway process.
- Registration token lifetime: 15 minutes, single-use.
- Job input expires seven days after completion by default. Expired input is
  hidden immediately and cleaned in bounded batches.
- Job responses, idempotency records, machine history, and webhook history have
  no automatic purge in this release. Capacity planning must include them.

No automatic execution tracking, generic job cancellation, command replay,
multi-machine batches, or sandbox lifecycle management is implemented.

## Tests

Ordinary tests require no database:

```sh
cargo test --workspace --locked --no-fail-fast
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Run the PostgreSQL/HTTP/WebSocket/HTTPS suite explicitly:

```sh
cargo build --workspace --bins --locked
TEST_DATABASE_URL=postgresql://localhost/postgres \
  cargo test -p execution-gateway --test gateway --locked -- --ignored
```

The supplied database URL identifies a test server where the test role may
create and drop databases. Each test creates a uniquely named database and drops
only that database, including on failure. Tests use local sockets, an ephemeral
HTTPS certificate generated by `openssl`, mock machines, and the actual
host daemon. They do not contact real providers or user callback destinations.

Coverage includes migrations, user/key management, credential privacy and rotation,
machine ownership/enrollment, concurrent idempotent submissions, batching, generation
checks, retained results/input expiry, no automatic command observation, connection
loss, restart/single-instance ownership, and signed webhook retry/redelivery/recovery.
CI runs the database suite on Linux and builds/tests all workspace applications on
Linux, macOS, and Windows.
