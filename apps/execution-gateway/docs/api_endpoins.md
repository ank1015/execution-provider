# Execution gateway API

## Status and scope

The v1 HTTP API, machine connection handling, durable jobs, and webhook delivery
are implemented. See [Database schemas](db_schemas.md) for persistence and
[installation and operations](../README.md) for configuration and commands.
The daemon supports registration and negotiated request receipt recovery across
connection loss and gateway restart.

The gateway is a standalone service for users, machines, and execution jobs.
It does not understand agents or their conversations. The caller owns the mapping
from a gateway job to its own application state.

A job represents one requested operation or one batch of operations. When that
operation or batch returns, the gateway stores its response and sends a job
notification. It does not automatically monitor commands after an operation
returns, collect their eventual exit results, or send command-exit notifications.
The caller submits another operation when it wants to observe or control them.

## Conventions

- API base path: `/v1`.
- Authentication: `Authorization: Bearer <credential>`.
- Admin keys, user API keys, registration tokens, and machine credentials have
  separate purposes and are not interchangeable.
- User-owned resources are scoped to the authenticated user. A machine ID, job
  ID, or execution handle never grants access by itself.
- Missing or foreign-owned resources return `404`; invalid/inactive credentials
  return `401`. Public errors do not expose database errors or credentials.
- Management and job IDs are UUIDs. List endpoints use cursor pagination.
- API responses use `Cache-Control: no-store`.
- Newly issued secret values are returned only at issuance. Read/list endpoints
  return safe metadata, never plaintext credentials or their stored hashes.
- The gateway's public identifiers use `machineId`. Existing execution payloads
  retain their shared protocol field names, such as `start_id` and `wait_ms`.

## 1. Admin: users and API keys

Authentication: deployment-level **admin key**.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/v1/admin/users` | Create a user with a name and optional callback URL; issue the initial user key and webhook secret. |
| GET | `/v1/admin/users` | List users. |
| GET | `/v1/admin/users/:userId` | Get user details. |
| PATCH | `/v1/admin/users/:userId` | Update name, callback URL, or enabled status. |
| POST | `/v1/admin/users/:userId/keys` | Issue an additional or replacement user key. |
| GET | `/v1/admin/users/:userId/keys` | List key metadata. |
| DELETE | `/v1/admin/users/:userId/keys/:keyId` | Revoke a user key. |

User creation and initial key creation are atomic. Issuing another key does not
automatically revoke existing keys. Disabling a user blocks subsequent access
and dispatch of new work; enabling restores access through unrevoked user keys.
Revoking one user key does not revoke the user's other keys or machine credentials.
An empty `callbackUrl` disables webhook creation for future job results.

## 2. User: profile and callback settings

Authentication: **user API key**.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/v1/me` | Get the current user and callback settings. |
| PATCH | `/v1/me` | Update name or callback URL. |
| POST | `/v1/me/webhook-secret/rotate` | Rotate the signing secret and return the new value once. |

User settings cannot change ownership, enabled status, or another user's data.
Updating the callback URL affects future events; existing events retain their
captured destination. Delivery uses the current signing secret, with a possible
brief overlap for already claimed deliveries during rotation.

## 3. User: machine management

Authentication: **user API key**; all operations are owner-scoped.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/v1/machines` | Create a named machine and return its ID plus a short-lived, single-use registration token. |
| GET | `/v1/machines` | List owned machines and their connection status. |
| GET | `/v1/machines/:machineId` | Get settings, capabilities, connection status, and reported runtime generation. |
| PATCH | `/v1/machines/:machineId` | Update name or enabled status. |
| POST | `/v1/machines/:machineId/registration` | Issue a replacement enrollment token for a reinstalled or reconfigured daemon. |
| DELETE | `/v1/machines/:machineId` | Revoke and soft-delete the machine, preserving historical jobs. |

A machine belongs to one user and has one current machine credential. Issuing a
replacement registration token invalidates previous unused tokens for that
machine. Successful redemption replaces the current machine credential.

`enabled` is an administrative setting. `online` means the gateway currently has
a live authenticated connection. Last-reported metadata may remain available
while the machine is offline and must be identified as historical.

The machine ID survives ordinary reconnects and daemon restarts. The installation
ID identifies the local installation; the runtime generation changes on daemon
restart; the connection ID changes on each connection.

## 4. Daemon: enrollment and connection

| Method | Endpoint | Authentication | Purpose |
| --- | --- | --- | --- |
| POST | `/v1/machines/:machineId/register` | Registration token | Exchange the token and installation details for a machine credential. |
| GET, upgraded to WebSocket | `/v1/machines/:machineId/connect` | Machine credential | Establish the outbound long-lived daemon connection. |

Registration flow:

1. The user creates the machine using a user key.
2. The gateway returns a machine ID and registration token.
3. The user supplies the registration token to the daemon.
4. The daemon redeems it and saves the issued machine credential privately.
5. The daemon connects and reports its runtime and binary information.

The daemon does not retain the broader user API key. Ordinary reconnects reuse
the machine credential and do not require enrollment. If the one-time credential
response is lost, enrollment can be restarted with a replacement registration token.

The daemon opens no inbound network server. It connects over WSS and exchanges
versioned hello/welcome, request/response, and heartbeat messages. The gateway
authorizes user-to-machine access before dispatch and checks current user and
machine settings again when queued work is about to be sent.

## 5. User: execution jobs

Authentication: **user API key**.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| POST | `/v1/jobs` | Durably submit one operation or a batch for one owned machine; return `202` with the job ID. |
| GET | `/v1/jobs` | List jobs, with machine, status, and exact idempotency-key filters. |
| GET | `/v1/jobs/:jobId` | Retrieve status and the stored daemon response or gateway error. |

There is **no separate batch endpoint**. One batch submission creates one gateway
job with individual operation results, not independently scheduled child jobs.
There is no generic job-cancellation endpoint in v1. To stop a command, submit
`execution.interrupt` or `execution.terminate`. Abandoning an observation does not
stop its command.

### Submission

The public submission contains `machineId`, `idempotencyKey`, and a `request`
containing the existing operation or batch payload. The gateway supplies the
daemon envelope's `protocol_version` and stable outer `request_id` from the job.
An optional `expected_generation_id` in `request` constrains the target runtime.

Single operation:

```json
{
  "machineId": "00000000-0000-0000-0000-000000000001",
  "idempotencyKey": "inspect-runtime-1",
  "request": {
    "operation": "runtime.info"
  }
}
```

Batch:

```json
{
  "machineId": "00000000-0000-0000-0000-000000000001",
  "idempotencyKey": "inspect-machine-1",
  "request": {
    "mode": "parallel",
    "operations": [
      { "request_id": "info", "operation": "runtime.info" },
      {
        "request_id": "list",
        "operation": "execution.list",
        "params": { "state": "active", "limit": 50 }
      }
    ]
  }
}
```

The gateway saves the job and input before returning `202 { id, status }`.
Idempotency is scoped to the user. Identical normalized submissions with the
same key return the existing job; different input with the same key conflicts.
Resolve existing idempotent submissions before rejecting new work because a
machine is offline or no longer usable. Ownership and caller authentication
still apply to the existing job.

For v1, reject new submissions to an already-offline machine. A disconnect after
acceptance does not discard the saved job. Network response deadlines are not
command execution deadlines.

### Supported operations

| Operation | Purpose |
| --- | --- |
| `runtime.info` | Get runtime information and capabilities. |
| `execution.start` | Start a command with the requested observation wait. |
| `execution.get` | Get an execution's current state. |
| `execution.observe` | Read output and optionally wait for activity or completion. |
| `execution.write_input` | Write stdin or terminal input. |
| `execution.close_input` | Close stdin. |
| `execution.interrupt` | Send an interrupt. |
| `execution.terminate` | Terminate an execution. |
| `execution.resize_terminal` | Resize a PTY. |
| `execution.list` | List executions retained by the daemon. |

These are payload operations, not additional HTTP endpoints. Remote
`runtime.shutdown` is unavailable. Parameters and byte/cursor encoding follow
[the existing execution API](../../process-execution/README.md#protocol-version-1)
and [shared protocol](../../../packages/process-execution-protocol/README.md).

### Batch behavior

- `mode` is `sequential` (default) or `parallel`.
- A batch contains 1–32 operations, all for the selected machine.
- Each operation has a unique `request_id` of 1–256 bytes and its normal parameters.
- Parallel mode runs up to eight operations concurrently.
- Sequential mode waits for each operation response, stops on the first operation
  error, and marks remaining operations skipped.
- Results remain in input order. Partial results are preserved on failure.
- A nonzero command exit or failed launch is an execution result, not automatically
  an operation error. Starting a command can successfully return a running handle.
- Sequential dispatch does not imply waiting for command exit, and does not lock
  the machine against other jobs.
- No nested batches, rollback, or references to earlier batch results.
- The shared transport limit is 8 MiB per message, including envelope overhead.

### Completion and recovery

When the requested operation or batch returns, the gateway stores that response
and atomically creates its terminal notification. It does not keep observing
executions mentioned in that response. A later observation is a new caller job.

The gateway pins the runtime generation before first dispatch. A daemon `accepted`
receipt moves the job from `dispatching` to `waiting_response`. Completed responses
and their notifications are committed before acknowledgement releases the daemon receipt.
Duplicate responses are acknowledged without creating duplicate events.

On connection loss or gateway restart, the gateway queries receipts in that same
runtime generation. It never resends the operation. A missing receipt, runtime
replacement, or 24 hours without recovery settles the job as `unknown`. Terminal jobs
are immutable; a late result is acknowledged and discarded. A gateway restart does
not stop daemon-owned processes. Daemon restart loses its in-memory receipts.

Peers negotiate `request_recovery` in hello/welcome. With a legacy daemon that omits
this capability, connection loss or gateway restart still makes dispatched jobs
`unknown`. Queued jobs that have never been dispatched may run, but fail after 30
seconds without dispatch. These are transport deadlines, not process deadlines.

Per-operation retry identities (`start_id`, `input_id`, interrupt `operation_id`)
retain their existing semantics. Whole-request receipts additionally cover single
operations and entire batches until acknowledgement. The daemon retains at most
128 receipts and 256 MiB, including an 8 MiB reservation for each pending result;
capacity pressure rejects new work without evicting accepted receipts.

## 6. User: webhook deliveries

Authentication: **user API key**.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/v1/webhook-deliveries` | List delivery status, optionally filtered by job. |
| GET | `/v1/webhook-deliveries/:deliveryId` | Get the event and delivery attempt history. |
| POST | `/v1/webhook-deliveries/:deliveryId/redeliver` | Schedule another delivery of the same terminal event. |

Events are `job.succeeded`, `job.failed`, and `job.unknown`. A batch produces one
terminal job event. There is no `execution.finished` event.
Users with an empty callback URL do not create webhook delivery records.

Send signed notifications to the user's saved callback URL. Include stable event
and job IDs plus the job outcome. Delivery is at least once: receivers verify
signatures and deduplicate event IDs. Callback failures do not change job results
or prevent retrieval. Delivery retries and manual redelivery never repeat the
execution operation. Manual redelivery retains payload, destination, and history.

Delivery uses independent 60-second leases, a ten-second HTTP timeout, and at
most eight attempts in a 24-hour retry cycle. Retry network failures and HTTP
408, 429, 500, 502, 503, and 504. Other unsuccessful statuses are terminal.
Backoff starts at 30 seconds, doubles to a one-hour cap, and uses 0.5–1.0 jitter;
a valid `Retry-After` is a minimum, subject to the cycle deadline.

Only HTTPS destinations whose exact origins appear in `WEBHOOK_ALLOWED_ORIGINS`
can receive callbacks; redirects are not followed. The default empty allowlist
rejects delivery without changing job results. Receivers verify:

- `X-Execution-Gateway-Event-Id`: stable event UUID.
- `X-Execution-Gateway-Timestamp`: Unix seconds for this attempt.
- `X-Execution-Gateway-Signature`: `v1=` followed by hexadecimal HMAC-SHA256.

Sign the exact bytes `timestamp + "." + eventId + "." + rawBody` using the literal
webhook secret. Check timestamp freshness, body/header event identity, and
deduplicate events before returning success. Any 2xx acknowledges delivery.

## 7. Operational health

These endpoints are unauthenticated and expose no credentials or connection details.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/healthz` | Check process liveness. |
| GET | `/readyz` | Check readiness to accept work, including database availability. |

Readiness does not mean a particular machine is online.

## Response shapes and limits

- User creation returns `{ user, key, webhookSecret }`; an issued key includes
  `secret` once. User/key read responses never include secret material.
- Machine creation returns `{ machine, machineId, registrationToken, expiresAt }`.
  Replacement token issuance returns `{ machineId, registrationToken, expiresAt }`.
- Registration accepts `{ installationId }` and returns `{ machineId, credential }`.
  Tokens expire after 15 minutes and can be redeemed once.
- Machine responses include `online` and `runtimeGenerationId` from live state,
  alongside `lastRuntimeInfo`, `lastBinaryInfo`, and `lastSeenAt` snapshots.
- Job details include metadata plus `response`, `error`, `request`,
  `requestStatus: "retained" | "expired"`, and `requestExpiresAt`.
- Lists return `{ data, nextCursor }`. `limit` is 1–100, default 50. Use the opaque
  returned cursor with unchanged filters; ordering is newest creation time first
  with ID as tie-breaker and full PostgreSQL timestamp precision.
- Delivery lists accept `jobId` and `status`. Details add `payload` and
  `attempts: { data, nextCursor }`; use `attemptLimit` (1–100, default 50) and
  the numeric `attemptCursor` to page attempts newest first.
- Management bodies are limited to 16 KiB and job submissions to 8 MiB, with
  additional room reserved inside that limit for generated protocol metadata.
- Names contain 1–200 characters after trimming. Idempotency keys contain 1–200
  bytes without whitespace. Management bodies reject unknown fields; optional
  PATCH fields that are null are treated as omitted. At least one setting is required.
- A user may have at most 256 outstanding jobs; excess admission returns `429`.
  Each connection dispatches at most 32 outstanding requests. The daemon retains
  its independent per-execution and batch limits.

## Connection and administrative behavior

The initial deployment is one Rust gateway process owning HTTP, machine sockets,
dispatch, and independent webhook tasks. A PostgreSQL session advisory lock
prevents a second gateway from serving the same database. Losing that connection
stops admission and shuts the gateway down. Multiple replicas are not supported.

The gateway accepts both `/v1/machines/:machineId/connect` and the existing
`/v1/hosts/:hostId/connect` route. The WebSocket hello still uses `host_id` to
preserve wire compatibility. Hello and socket writes have ten-second deadlines;
heartbeats run every 15 seconds, with 45 seconds without valid traffic closing
the connection. A duplicate connection does not replace or stop a live runtime.

Disabling a user or machine blocks new dispatches, preserves existing connections,
and lets already-dispatched operations return. Machine authentication still allows
heartbeats/reconnects while disabled; registration requires enabled ownership and
an enabled machine. Re-enabling restores dispatch. Removing a machine or replacing
its credential revokes the old connection. The current daemon then shuts down
its owned processes; responses lost during that shutdown are reported as unknown.
Gateway shutdown closes connections without revoking credentials or stopping the
daemon's processes.

## Daemon integration

`process-execution-host-daemon register --gateway-url URL --machine-id UUID` reads
the registration token from stdin, redeems it with its stable installation ID, and
atomically saves the machine credential. `configure` remains available for an already
issued credential; `--host-id` remains an alias for `--machine-id` there.

See the [shared transport contract](../../../packages/process-execution-protocol/README.md#gateway-transport-contract)
for acceptance, recovery, missing-receipt, and persistence acknowledgement messages.

There is no automatic command monitoring, multi-machine batch execution, sandbox
pause/resume, or durable daemon process/output recovery in this release.
