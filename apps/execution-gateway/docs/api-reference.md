# Execution Gateway API reference

The execution gateway lets an application manage its machines and submit process
operations to a connected `process-execution-host-daemon`. The gateway persists each
submission as a job and returns its result through immediate lookup or a bounded wait.
An optional lightweight webhook can notify the application that the result is ready.

This reference describes HTTP API version 1. For the system design and failure
semantics, see [Architecture](architecture.md). The execution payload is shared with
[`process-execution`](../../process-execution/README.md#protocol-version-4).

## Base URL and authentication

The production base URL is:

```text
https://execution.acentric.dev
```

Authenticated requests use a bearer credential:

```http
Authorization: Bearer <credential>
```

The API has four credential types. Each credential is valid only for its stated
purpose.

| Credential | Typical prefix | Used by | Scope |
| --- | --- | --- | --- |
| Admin key | Deployment-defined | Operator | User and user-key administration |
| User API key | `egw_` | Customer application | Its user, machines, jobs, and webhook deliveries |
| Registration token | `egr_` | Daemon installer | One machine enrollment within 15 minutes |
| Machine credential | `egm_` | Host daemon | One machine's WebSocket connection |

User API keys, registration tokens, machine credentials, and webhook secrets are
returned in plaintext only when issued. Store them in a secret manager. List and get
responses contain safe metadata and prefixes, never plaintext credentials or hashes.

## HTTP conventions

- Send JSON request bodies with `Content-Type: application/json`.
- Resource IDs are UUIDs. Timestamps are RFC 3339 UTC strings.
- Management fields use `camelCase`. Execution operation fields use `snake_case` to
  match the shared process-execution protocol.
- Management bodies and the top-level job request reject unknown fields. Operation
  parameters follow the shared process-execution protocol.
- All responses include `Cache-Control: no-store`.
- A missing resource and a resource owned by another user both return `404`.
- Management request bodies are limited to 16 KiB. Job request bodies are limited to
  8 MiB, including transport overhead.

Errors have one shape:

```json
{
  "error": {
    "code": "machine_offline",
    "message": "machine is offline"
  }
}
```

Common status codes are:

| Status | Meaning |
| --- | --- |
| `400` | Invalid JSON, fields, identifiers, filters, or operation payload |
| `401` | Missing, invalid, revoked, or inactive credential |
| `404` | Resource missing or outside the authenticated user's scope |
| `409` | Current state conflicts with the request, such as an offline machine |
| `413` | Request body or execution payload exceeds its limit |
| `415` | A JSON endpoint did not receive `application/json` |
| `429` | The user has too many outstanding jobs |
| `500` | The request could not be completed |
| `503` | The gateway is shutting down or temporarily unavailable |

Error `code` values are stable machine-readable categories. Handle at least these
public codes:

| Code | Where it appears | Meaning |
| --- | --- | --- |
| `invalid_argument` | HTTP error | The request cannot be parsed or validated |
| `unauthorized` | HTTP error | The credential is missing, invalid, revoked, or inactive |
| `not_found` | HTTP or operation error | The scoped resource or execution does not exist |
| `idempotency_conflict` | HTTP error | The idempotency key already identifies different input |
| `machine_offline` | HTTP error | The target has no ready connection |
| `machine_disabled` | HTTP or terminal job error | The target became unavailable for admission or dispatch |
| `generation_mismatch` | HTTP or terminal job error | The expected runtime is no longer active |
| `resource_limit` | HTTP or operation error | A configured concurrency or storage limit was reached |
| `dispatch_timeout` | Terminal job error | A queued job could not be dispatched within its deadline |
| `receipt_missing` | Terminal job error | The daemon no longer has the accepted request receipt |
| `runtime_replaced` | Terminal job error | Recovery reached a different runtime generation |
| `recovery_unavailable` | Terminal job error | The response-recovery window expired |
| `internal_error` | HTTP error | The gateway could not complete the request |
| `unavailable` | HTTP error | The service is shutting down or temporarily unavailable |

Operation responses can also contain error codes defined by
`process-execution-core`. Preserve unknown codes so newer servers can add more specific
errors without breaking clients.

## Pagination

Top-level list endpoints accept `limit` and `cursor`:

```http
GET /v1/machines?limit=50&cursor=<opaque-cursor>
```

`limit` defaults to 50 and must be between 1 and 100. Lists are ordered newest first
and return:

```json
{
  "data": [],
  "nextCursor": null
}
```

Pass `nextCursor` unchanged on the next request and retain the same filters. A null
cursor means the page is final.

## Quick start

Set a user API key without placing it directly in shell history:

```sh
export EXECUTION_GATEWAY_URL=https://execution.acentric.dev
read -rsp "API key: " EXECUTION_GATEWAY_API_KEY && export EXECUTION_GATEWAY_API_KEY
```

List the user's connected machines:

```sh
curl --fail-with-body \
  -H "Authorization: Bearer $EXECUTION_GATEWAY_API_KEY" \
  "$EXECUTION_GATEWAY_URL/v1/machines"
```

Submit a runtime inspection job:

```sh
curl --fail-with-body \
  -X POST \
  -H "Authorization: Bearer $EXECUTION_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  "$EXECUTION_GATEWAY_URL/v1/jobs" \
  -d '{
    "machineId": "00000000-0000-0000-0000-000000000001",
    "idempotencyKey": "runtime-info-1",
    "request": {"operation": "runtime.info"}
  }'
```

The gateway returns `202 Accepted` with a job ID. Wait up to five minutes for terminal
state:

```sh
curl --fail-with-body \
  -H "Authorization: Bearer $EXECUTION_GATEWAY_API_KEY" \
  "$EXECUTION_GATEWAY_URL/v1/jobs/JOB_ID/wait?timeoutMs=300000"
```

If the job is still nonterminal after the bounded wait, the same endpoint returns its
current state and may be called again.

## Admin API

Admin endpoints require the deployment admin key.

### Users

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/admin/users` | `201` | Create a user and issue its first key and webhook secret |
| `GET` | `/v1/admin/users` | `200` | List users |
| `GET` | `/v1/admin/users/{userId}` | `200` | Get one user |
| `PATCH` | `/v1/admin/users/{userId}` | `200` | Change user settings |

Create a user:

```json
{
  "name": "Example application",
  "callbackUrl": "",
  "webhookPayloadVersion": 2
}
```

`callbackUrl` is required. Use an empty string to disable webhooks. A nonempty value
must be HTTPS and cannot contain credentials or a fragment. `webhookPayloadVersion`
may be 1 or 2 and defaults to 2. The response contains all three one-time secrets or
resources created by the request:

```json
{
  "user": {
    "id": "00000000-0000-0000-0000-000000000001",
    "name": "Example application",
    "enabled": true,
    "callbackUrl": "",
    "webhookPayloadVersion": 2,
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  },
  "key": {
    "id": "00000000-0000-0000-0000-000000000002",
    "userId": "00000000-0000-0000-0000-000000000001",
    "name": null,
    "keyPrefix": "egw_example",
    "createdAt": "2026-01-01T00:00:00Z",
    "revokedAt": null,
    "secret": "egw_returned-once"
  },
  "webhookSecret": "whsec_returned-once"
}
```

A user patch accepts at least one of `name`, `callbackUrl`, `enabled`, or
`webhookPayloadVersion`:

```json
{"enabled": false}
```

Disabling a user invalidates its user API keys and prevents new dispatch and webhook
claims. It does not delete historical resources.

### User API keys

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/admin/users/{userId}/keys` | `201` | Issue another user API key |
| `GET` | `/v1/admin/users/{userId}/keys` | `200` | List key metadata |
| `DELETE` | `/v1/admin/users/{userId}/keys/{keyId}` | `204` | Revoke a key |

Issue a named key with `{"name":"production"}` or send `{}` for an unnamed key.
The response includes `secret` once. Issuing or revoking one key does not affect the
user's other keys or its machine credentials.

## User profile API

These endpoints require a user API key.

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v1/me` | `200` | Read the authenticated user |
| `PATCH` | `/v1/me` | `200` | Change `name`, `callbackUrl`, or webhook payload version |
| `POST` | `/v1/me/webhook-secret/rotate` | `200` | Replace and return the webhook secret |

The user profile cannot change its own `enabled` state. Secret rotation returns:

```json
{"webhookSecret":"whsec_returned-once"}
```

## Machine API

Machine-management endpoints require a user API key and are scoped to its owner.

### Create and register a machine

Create the machine record:

```http
POST /v1/machines
```

```json
{"name":"build-agent-windows"}
```

The `201 Created` response includes the machine, a single-use token, and its expiry:

```json
{
  "machineId": "00000000-0000-0000-0000-000000000001",
  "registrationToken": "egr_returned-once",
  "expiresAt": "2026-01-01T00:15:00Z",
  "machine": {
    "id": "00000000-0000-0000-0000-000000000001",
    "userId": "00000000-0000-0000-0000-000000000002",
    "name": "build-agent-windows",
    "enabled": true,
    "installationId": null,
    "credentialVersion": 0,
    "lastRuntimeInfo": null,
    "lastBinaryInfo": null,
    "lastSeenAt": null,
    "online": false,
    "runtimeGenerationId": null,
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z"
  }
}
```

Pass the returned token to the daemon through stdin:

```sh
printf '%s\n' "$REGISTRATION_TOKEN" | \
  process-execution-host-daemon register --machine-id "$MACHINE_ID"
process-execution-host-daemon run
```

The daemon calls the enrollment endpoint directly:

```http
POST /v1/machines/{machineId}/register
Authorization: Bearer egr_registration_token
Content-Type: application/json

{"installationId":"00000000-0000-0000-0000-000000000003"}
```

It receives `{"machineId":"...","credential":"egm_returned-once"}` and stores the
machine credential locally. Applications normally use the daemon command instead of
calling this endpoint themselves.

### Manage machines

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v1/machines` | `200` | List machines and live presence |
| `GET` | `/v1/machines/{machineId}` | `200` | Get settings, capabilities, and presence |
| `PATCH` | `/v1/machines/{machineId}` | `200` | Change `name` or `enabled` |
| `POST` | `/v1/machines/{machineId}/registration` | `201` | Issue a replacement registration token |
| `DELETE` | `/v1/machines/{machineId}` | `204` | Revoke and soft-delete a machine |

The replacement registration response has `machineId`, `registrationToken`, and
`expiresAt`. Issuing it revokes previous unused registration tokens. Redeeming it
replaces the existing machine credential and connection.

`online` reflects an authenticated connection to this gateway process.
`lastRuntimeInfo`, `lastBinaryInfo`, and `lastSeenAt` are historical snapshots and can
remain populated while `online` is false. The machine ID survives normal reconnects and
daemon restarts; `runtimeGenerationId` changes when the daemon runtime restarts.

Disabling a machine prevents new dispatches while keeping its connection alive.
Deleting it revokes its credential and disconnects the daemon.

### Daemon WebSocket

The daemon connects with its machine credential:

```http
GET /v1/machines/{machineId}/connect
Connection: Upgrade
Upgrade: websocket
Authorization: Bearer egm_machine_credential
```

The compatibility route `/v1/hosts/{machineId}/connect` is also accepted. The
WebSocket message contract is documented in the
[shared protocol](../../../packages/process-execution-protocol/README.md#gateway-transport-contract).

## Job API

Job endpoints require a user API key.

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `POST` | `/v1/jobs` | `202` | Submit one operation or one batch |
| `GET` | `/v1/jobs` | `200` | List jobs |
| `GET` | `/v1/jobs/{jobId}` | `200` | Get status, input, response, and gateway error |
| `GET` | `/v1/jobs/{jobId}/wait` | `200` | Wait for terminal state, then return the same job detail |

### Submit a job

Every submission identifies one machine, one idempotency key, and one execution
request. It may also carry an opaque `clientContext` object:

```json
{
  "machineId": "00000000-0000-0000-0000-000000000001",
  "idempotencyKey": "start-build-2026-01-01",
  "clientContext": {
    "receiver": "tool-pi-bash-v1",
    "reference": "build-2026-01-01"
  },
  "request": {
    "operation": "execution.start",
    "params": {
      "start_id": "build-2026-01-01",
      "command": {
        "type": "program",
        "executable": "git",
        "args": ["status", "--short"]
      },
      "wait_ms": 1000
    }
  }
}
```

The gateway responds after it has saved the job and input:

```json
{
  "id": "00000000-0000-0000-0000-000000000004",
  "status": "queued"
}
```

`idempotencyKey` must contain 1–200 bytes without whitespace. Its scope is the user.
Retrying the same normalized machine, request, and optional `clientContext` with the
same key returns the existing job. Adding, removing, or changing `clientContext` when
reusing a key returns `409 idempotency_conflict`, as does changing other input. The
gateway stores this object without interpreting its fields and does not send it to the
machine. Generate the key from the caller's durable action identity and keep it stable
across uncertain HTTP outcomes.

New submissions require the machine to be online. The gateway can return the existing
idempotent job even if that machine has since gone offline.

### Job states

| State | Terminal | Meaning |
| --- | --- | --- |
| `queued` | No | Saved and waiting to be sent |
| `dispatching` | No | Sent or being sent; daemon acceptance is not confirmed |
| `waiting_response` | No | Daemon accepted the request and owns its completion |
| `succeeded` | Yes | The operation or every batch item succeeded or returned an explicitly accepted error code |
| `failed` | Yes | A definite gateway, operation, or batch failure occurred |
| `unknown` | Yes | Work may have run, but its response cannot be recovered |

Do not treat `unknown` as a failed command or automatically replay the request. Use
operation retry identities and execution discovery when the caller can safely reconcile
the outcome.

### Job details

`GET /v1/jobs/{jobId}` returns job metadata plus:

- `request`: the normalized operation or batch while retained, otherwise null.
- `requestStatus`: `retained` or `expired`.
- `requestExpiresAt`: the input cleanup deadline after completion.
- `response`: the complete daemon response when one was received.
- `error`: a safe gateway error when the gateway produced the terminal result.

`GET /v1/jobs/{jobId}/wait` accepts only `timeoutMs`, an integer from 1 through
300000 that defaults to 300000. An already-terminal job returns immediately. Otherwise,
the request waits for terminal state or the timeout and then returns exactly the same
representation as job detail. A timeout is not an error: the response is `200` with the
current `queued`, `dispatching`, or `waiting_response` state, and the caller may wait
again. Waiters retain neither a database transaction nor a pool connection. The gateway
allows up to 64 concurrent waits per user and 1,024 across the process; excess waits
return `429 resource_limit`.

A successful daemon response has this envelope:

```json
{
  "protocol_version": 4,
  "request_id": "00000000-0000-0000-0000-000000000004",
  "generation_id": "00000000-0000-0000-0000-000000000005",
  "status": "ok",
  "result": {}
}
```

`execution.start` completes the gateway job when the start operation returns. Its result
may describe a process that is still running. Submit `execution.observe` in another job
to wait for more output or completion.

`execution.run` instead keeps the gateway job in progress until the non-interactive
process, timeout/termination handling, descendant cleanup, and output capture finish.
The final response contains a bounded output tail plus metadata for the complete
machine-local output file. The gateway does not upload that file. A timeout, explicit
termination, launch failure, or nonzero exit is a successful operation carrying that
execution result; protocol and infrastructure failures fail the gateway job.

Cancel a pending run by submitting a separate `execution.terminate_run` job with the
same `run_id`. Cancellation jobs are prioritized and have reserved machine-request
capacity. An HTTP disconnect or caller abort alone does not cancel accepted work.

List jobs with optional exact filters:

```http
GET /v1/jobs?machineId=<uuid>&status=succeeded&idempotencyKey=<key>&limit=50
```

### Supported operations

The remote API accepts every process-execution operation except `runtime.shutdown`.

| Operation | Parameters | Result |
| --- | --- | --- |
| `runtime.info` | Omit | Runtime generation, shell, capabilities, and binary information |
| `execution.start` | Start parameters below | Initial observation |
| `execution.run` | Run parameters below | Final execution, output-file metadata, and bounded output tail |
| `execution.terminate_run` | Stable `run_id`, optional `grace_period_ms` | Pending, terminating, or finished cancellation receipt |
| `execution.get` | `handle` | Execution snapshot |
| `execution.observe` | `handle`, optional cursor/wait/output controls | Observation |
| `execution.write_input` | `handle`, `input_id`, `data_base64` | Accepted byte count |
| `execution.close_input` | `handle` | Stdin state |
| `execution.interrupt` | `handle`, `operation_id` | Execution snapshot |
| `execution.terminate` | `handle`, optional `grace_period_ms` | Execution snapshot |
| `execution.resize_terminal` | `handle`, `rows`, `cols` | Execution snapshot |
| `execution.list` | Optional state, labels, limit, and page cursor | Execution page |
| `filesystem.get_metadata` | `path`, optional `cwd` | File metadata |
| `filesystem.read_file` | `path`, optional `cwd`/`max_bytes` | Metadata, padded-base64 bytes, and SHA-256 |
| `filesystem.write_file` | Stable `mutation_id`, path, padded-base64 bytes, precondition | Conditional atomic replacement receipt |
| `filesystem.remove_file` | Stable `mutation_id`, path, precondition | Conditional file removal receipt |

An execution handle contains both identities required to address a process:

```json
{
  "id": "00000000-0000-0000-0000-000000000006",
  "generation_id": "00000000-0000-0000-0000-000000000005"
}
```

Start parameters are:

```json
{
  "start_id": "caller-stable-start-id",
  "command": {
    "type": "shell",
    "script": "echo hello"
  },
  "cwd": "optional/path",
  "env": {"EXAMPLE": "value"},
  "shell_snapshot": {"scope_id": "stable-session-id"},
  "io": {"type": "pipes", "stdin": false},
  "wait_ms": 1000,
  "max_output_bytes": 65536,
  "labels": {"owner": "example"}
}
```

Only `start_id` and `command` are required. A direct program command uses
`{"type":"program","executable":"...","args":[]}`. A PTY uses
`{"type":"pty","rows":24,"cols":80}`.

Run parameters are:

```json
{
  "run_id": "caller-stable-run-id",
  "command": {
    "type": "program",
    "executable": "cargo",
    "args": ["test"]
  },
  "cwd": "optional/path",
  "env": {"EXAMPLE": "value"},
  "shell_snapshot": {"scope_id": "stable-session-id"},
  "timeout_ms": 300000,
  "max_output_bytes": 65536,
  "labels": {"owner": "example"}
}
```

Only `run_id` and `command` are required. It always uses closed-stdin pipes. Omitting
`timeout_ms` means no execution timeout. The response returns the last 64 KiB by
default; a request may lower that value, and host configuration may raise it to at most
1 MiB. `output_truncated` indicates that the machine-local file contains earlier bytes.
The `output_file` object includes `artifact_id`, absolute machine `path`, `size_bytes`,
`sha256`, `complete`, and `expires_at`. The default host retention is 24 hours.

`shell_snapshot` is optional. It asks a compatible Unix host to load and cache the
selected user's interactive shell profile for the stable scope before launching the
command. Per-start `env` values override captured values. The gateway transports the
request and never receives or stores the captured environment or shell state.

Observations contain an execution snapshot, output chunks, `next_cursor`, `has_more`,
`output_gap`, and `return_reason`. Output and input bytes use standard padded base64.
Pass `next_cursor` back unchanged as `after_cursor`; keep reading while `has_more` is
true. `return_when` can be `activity` or `finished_or_timeout`.

Use caller-stable retry identities for side-effecting operations:

- `start_id` for `execution.start`.
- `run_id` for `execution.run` and `execution.terminate_run`.
- `input_id` for `execution.write_input`.
- `operation_id` for `execution.interrupt`.
- `mutation_id` for `filesystem.write_file` and `filesystem.remove_file`.

Reuse one of these IDs only with the same action and payload. A request ID correlates a
batch item; it does not replace operation-level deduplication.

Filesystem reads are whole-file and bounded by the host's advertised limit. Their SHA-256
can be supplied as `{"type":"sha256","sha256":"..."}` to a later mutation; file creation
uses `{"type":"missing"}`. A mutation returns `already_applied` when its desired final state
already exists, allowing safe recovery after a lost response. Removal accepts files only
and is never recursive.

The optional top-level `expected_generation_id` rejects a submission if the live
runtime generation has changed:

```json
{
  "operation": "runtime.info",
  "expected_generation_id": "00000000-0000-0000-0000-000000000005"
}
```

### Batch jobs

Batches use the same `/v1/jobs` endpoint:

```json
{
  "machineId": "00000000-0000-0000-0000-000000000001",
  "idempotencyKey": "inspect-machine-1",
  "request": {
    "mode": "parallel",
    "accepted_error_codes": ["not_found"],
    "operations": [
      {"request_id": "info", "operation": "runtime.info"},
      {
        "request_id": "active",
        "operation": "execution.list",
        "params": {"state": "active", "limit": 50}
      }
    ]
  }
}
```

- `mode` is `sequential` by default or `parallel`.
- `accepted_error_codes` is optional. Listed per-item errors remain visible but do not
  fail the outer job or stop sequential dispatch. Unlisted errors still fail normally.
- A batch contains 1–32 operations with unique 1–256 byte `request_id` values.
- Sequential mode waits for each operation response. The first operation error marks
  the remaining items `skipped`.
- Parallel mode runs up to eight items concurrently.
- Results remain in input order, and partial results are retained.
- Nested batches, rollback, and references to earlier batch results are unsupported.
- Sequential operation ordering does not wait for a started process to exit unless its
  `wait_ms` causes the start operation itself to wait that long.

A valid batch response keeps its outer status `ok` and reports item outcomes:

```json
{
  "succeeded": false,
  "results": [
    {"request_id": "one", "status": "ok", "result": {}},
    {
      "request_id": "two",
      "status": "error",
      "error": {"code": "not_found", "message": "execution not found"}
    },
    {
      "request_id": "three",
      "status": "skipped",
      "reason": "a previous operation failed"
    }
  ]
}
```

A nonzero command exit or failed process launch is an execution result, so the operation
itself can still be successful. Inspect the execution snapshot's final `result`.

## Webhook API

Webhook-management endpoints require a user API key.

| Method | Endpoint | Response | Purpose |
| --- | --- | --- | --- |
| `GET` | `/v1/webhook-deliveries` | `200` | List delivery state |
| `GET` | `/v1/webhook-deliveries/{deliveryId}` | `200` | Get payload and attempt history |
| `POST` | `/v1/webhook-deliveries/{deliveryId}/redeliver` | `202` | Start another delivery cycle |

List filters are `jobId` and `status`. Delivery statuses are `pending`, `delivering`,
`retry_wait`, `delivered`, and `failed`. Delivery details support `attemptLimit` from
1–100 and a numeric `attemptCursor`.

The gateway creates one event when a job reaches a terminal state and the user has a
nonempty callback URL. Payload version 2 is lightweight and has this shape:

```json
{
  "schemaVersion": 2,
  "eventId": "00000000-0000-0000-0000-000000000007",
  "type": "job.succeeded",
  "jobId": "00000000-0000-0000-0000-000000000004",
  "machineId": "00000000-0000-0000-0000-000000000001",
  "clientContext": {
    "receiver": "tool-pi-bash-v1",
    "reference": "build-2026-01-01"
  },
  "completedAt": "2026-01-01T00:00:00Z"
}
```

Event types are `job.succeeded`, `job.failed`, and `job.unknown`. Version 2 does not
embed the request, response, gateway error, credentials, or account data. After durably
recording the event, retrieve the authoritative result from either job-detail endpoint.
When the submission includes `clientContext`, every terminal event includes the same
object without interpreting or modifying its contents. When omitted from the
submission, the event omits the field. This behavior applies to both payload versions.

Payload version 1 is retained for compatibility and omits `schemaVersion`; it adds
`response` and `error` fields containing the terminal job outcome. Users migrated from
an earlier release remain on version 1. Newly created users default to version 2. Set
`webhookPayloadVersion` to 1 or 2 through either user patch endpoint to control future
events. Existing delivery payloads remain immutable, including during redelivery.

The gateway does not emit a later event when a process started by a completed job exits.

Saving a callback URL does not by itself permit outbound traffic. Its exact HTTPS
origin must also be configured by the operator in `WEBHOOK_ALLOWED_ORIGINS`; otherwise
the delivery reaches the terminal `failed` state without changing the job result.

Every delivery includes:

```text
X-Execution-Gateway-Event-Id: <event UUID>
X-Execution-Gateway-Timestamp: <Unix seconds>
X-Execution-Gateway-Signature: v1=<hex HMAC-SHA256>
```

Verify the signature over the exact bytes below using the user's literal webhook
secret:

```text
timestamp + "." + eventId + "." + rawRequestBody
```

Check timestamp freshness, confirm the body and header event IDs match, and deduplicate
the event ID before returning any 2xx response. Delivery is at least once.

The gateway retries network failures and HTTP `408`, `429`, `500`, `502`, `503`, and
`504`. It makes at most eight attempts in a 24-hour cycle, uses exponential backoff from
30 seconds to one hour with jitter, and honors a valid `Retry-After` as a minimum.
Manual redelivery preserves the event ID, payload, destination, and attempt history. It
does not rerun the job.

## Health endpoints

Health endpoints require no authentication and expose no machine details.

| Method | Endpoint | Success | Meaning |
| --- | --- | --- | --- |
| `GET` | `/healthz` | `200 {"status":"ok"}` | The process is alive |
| `GET` | `/readyz` | `200 {"status":"ready"}` | The database and job schema are reachable |

Readiness does not imply that a particular machine is online.

## Service limits and lifecycle rules

- Names contain 1–200 characters after trimming.
- A user can have up to 256 jobs in nonterminal states.
- A machine connection dispatches up to 32 ordinary outstanding requests and reserves
  four additional slots for single `execution.terminate_run` requests.
- Registration tokens expire after 15 minutes and are single-use.
- A queued job that cannot dispatch within 30 seconds fails with `dispatch_timeout`.
- Recoverable dispatched jobs have a 24-hour disconnected recovery window.
- Completed request input is retained for `REQUEST_RETENTION_DAYS`, seven days by
  default. Job metadata, responses, idempotency records, machine history, and webhook
  history do not currently have automatic retention cleanup.
- There is no generic job-cancellation endpoint. Use `execution.interrupt` or
  `execution.terminate` with a known handle.
- The gateway does not automatically monitor processes, replay ambiguous work, execute
  one batch across multiple machines, or manage sandbox lifecycle.
