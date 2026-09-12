# Execution gateway database schemas

## Status and scope

This document describes the implemented PostgreSQL schema for
[the execution gateway API](api_endpoins.md). The eight tables and indexes are in
[the initial migration](../migrations/0001_initial.sql), embedded in the binary.
Run `execution-gateway migrate` explicitly before `serve`; startup never migrates.

The gateway owns users, machine registrations, submitted jobs, saved responses,
and job notifications. A job is one operation or one batch. It does not own a
second execution runtime and does not automatically track command completion.

## Tables

| Table | Responsibility |
| --- | --- |
| `users` | Gateway customers and callback settings |
| `user_api_keys` | User authentication and revocation |
| `machines` | Ownership, machine settings, and current daemon credential |
| `machine_registration_tokens` | Single-use enrollment |
| `jobs` | Durable submissions, lifecycle, and responses |
| `job_requests` | Retainable operation or batch input |
| `webhook_deliveries` | Durable terminal job notification outbox |
| `webhook_delivery_attempts` | Notification delivery history |

There are no executions, output-stream, batch-child-job, or agent tables.
Execution handles and output appear only inside requested operation responses.

## Conventions

- One shared schema with ownership expressed by `user_id`.
- IDs use `uuid`; timestamps use `timestamptz`; database columns use `snake_case`.
- Columns are non-null unless explicitly marked nullable.
- Random bearer credentials are stored only as hashes and returned only when
  issued. Credential reads/listings never expose hashes or plaintext secrets.
- Webhook signing secrets are encrypted because the gateway must recover them.
  Follow the llm-gateway owner/purpose-bound encryption pattern; keep encryption
  keys outside PostgreSQL.
- Protocol input, responses, and opaque metadata use `json` to preserve JSON
  content without adding PostgreSQL `jsonb` text restrictions to the wire format.
- Status fields use text with allowed-value constraints.
- Foreign keys preserve historical relationships; do not cascade-delete jobs
  when users or machines are disabled or removed.
- Every user-facing query is ownership-scoped. Foreign keys complement that
  authorization; they do not replace it.
- Initial timestamps and IDs may have database defaults. Services explicitly
  maintain `updated_at`, state transitions, and retention deadlines.

## 1. `users`

One user represents a customer application or backend, not an individual agent.
It can own multiple API keys, machines, and jobs.

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key |
| `name` | text | Display name |
| `enabled` | boolean | Whether the user can access the service |
| `callback_url` | text | Registered job notification destination |
| `webhook_secret_encrypted` | bytea | Encrypted signing secret |
| `created_at` | timestamptz | Creation time |
| `updated_at` | timestamptz | Last settings change |

Create the user and initial API key atomically. Admin credentials remain in
deployment configuration; there is no admin table. The signing secret is distinct
from user API keys and machine credentials.

## 2. `user_api_keys`

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key and management identifier |
| `user_id` | uuid | Foreign key to `users.id` |
| `name` | text, nullable | Optional label |
| `key_hash` | text | Unique hash of the random key |
| `key_prefix` | text | Safe display prefix |
| `created_at` | timestamptz | Issuance time |
| `revoked_at` | timestamptz, nullable | Revocation time |

Enforce unique `key_hash`. Authentication also requires an enabled owner.
Revocation preserves the first timestamp and does not revoke other keys or
machine credentials. Issuing a new key does not implicitly revoke an old one.

## 3. `machines`

Each machine has one owner and at most one current daemon credential in v1.

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key; public `machineId` |
| `user_id` | uuid | Foreign key to `users.id` |
| `name` | text | User-assigned name |
| `enabled` | boolean | Whether new execution requests are allowed |
| `installation_id` | uuid, nullable | Installation bound during enrollment |
| `credential_hash` | text, nullable | Unique hash of the current machine credential |
| `credential_version` | integer | Version incremented when the credential is replaced or revoked |
| `last_runtime_info` | json, nullable | Last reported generation, shell, and capabilities |
| `last_binary_info` | json, nullable | Last reported binary version, OS, and architecture |
| `last_seen_at` | timestamptz, nullable | Last recorded contact |
| `deleted_at` | timestamptz, nullable | Soft deletion time |
| `created_at` | timestamptz | Creation time |
| `updated_at` | timestamptz | Last settings or enrollment change |

Installation and credential fields are null before enrollment. Start the
credential version at zero and increment when issuing/replacing/revoking a
credential. Enforce uniqueness of non-null credential hashes and `(user_id, id)`
for composite job ownership references.

Machine IDs survive ordinary daemon restarts. Installation identity and runtime
generation have different lifetimes; reconnecting does not change the generation.
The machine record's reported generation does not replace a job's pinned generation.

Do not persist an authoritative `online` boolean. The initial gateway process's
live authenticated connection determines online status. Reported metadata and
`last_seen_at` are historical snapshots, not proof of current availability.

Soft deletion disables future use, revokes credentials/enrollment, and preserves
jobs. Temporary disablement blocks new dispatches while keeping connections and
already-dispatched operations alive. Credential replacement/removal revokes the
old connection; the current daemon then shuts down its owned processes. No separate machine-credentials table
is needed while only one current credential is supported.

## 4. `machine_registration_tokens`

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key |
| `machine_id` | uuid | Foreign key to `machines.id` |
| `token_hash` | text | Unique hash of the registration token |
| `expires_at` | timestamptz | Enrollment deadline |
| `consumed_at` | timestamptz, nullable | Successful redemption time |
| `revoked_at` | timestamptz, nullable | Explicit invalidation time |
| `created_at` | timestamptz | Issuance time |

Tokens are short-lived and single-use. Ownership is resolved through the machine.
Issue a replacement under a machine-row lock, invalidating previous unused tokens.
Redeem under a transaction that validates expiry, token state, machine usability,
and enabled ownership; binds installation identity; replaces the machine credential;
increments its version; and marks the token consumed. Concurrent redemption must
not issue multiple credentials from the same token.

Return the plaintext machine credential once. If that response is lost, issue a
replacement registration token rather than storing a recoverable plaintext secret.
Registration tokens expire 15 minutes after issuance.

## 5. `jobs`

One row is one operation or a whole batch for one machine. Batch items are not
independently scheduled jobs.

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key; public job ID |
| `user_id` | uuid | Owning user |
| `machine_id` | uuid | Target machine |
| `idempotency_key` | text | Caller submission identity |
| `request_hash` | text | Fingerprint of normalized machine and input |
| `status` | text | Job state |
| `runtime_generation_id` | uuid, nullable | Generation pinned before first dispatch |
| `response_recovery` | boolean, default false | Whether the dispatched daemon negotiated receipt recovery |
| `recovery_expires_at` | timestamptz, nullable | Deadline set on disconnect/restart; cleared on acceptance or completion |
| `response` | json, nullable | Complete daemon response, including batch item outcomes |
| `error` | json, nullable | Safe gateway error when applicable |
| `created_at` | timestamptz | Durable acceptance time |
| `dispatched_at` | timestamptz, nullable | First dispatch attempt time |
| `finished_at` | timestamptz, nullable | Terminal outcome time |
| `updated_at` | timestamptz | Last state transition |

### States

| State | Meaning |
| --- | --- |
| `queued` | Saved and awaiting dispatch |
| `dispatching` | Dispatch began; daemon acceptance is not confirmed |
| `waiting_response` | Daemon accepted the request; awaiting its operation/batch response |
| `succeeded` | The operation succeeded, or every batch operation succeeded |
| `failed` | An operation/batch reported failure, or a definite gateway failure occurred |
| `unknown` | Work may have executed, but its response cannot be recovered |

`succeeded`, `failed`, and `unknown` are terminal. Acceptance moves a dispatched
job to `waiting_response`; a complete operation/batch response settles it. With
negotiated recovery, connection loss or gateway restart preserves pending jobs for
receipt lookup in the original runtime. Missing receipts, runtime replacement,
removed machines, or an expired 24-hour disconnected recovery window produce
`unknown`. Legacy daemons without receipt recovery settle as `unknown` on loss.
There is no automatic replay. Never-dispatched queued jobs may run when a connection
becomes available, but fail after 30 seconds without dispatch. An unknown response
does not claim execution failure or cancellation.

### Constraints and response semantics

- Unique `(user_id, idempotency_key)`.
- Foreign key `user_id` references `users.id`; composite foreign key
  `(user_id, machine_id)` references `machines(user_id, id)`.
- Terminal states require `finished_at`; nonterminal states leave it null.
- Fingerprint includes machine ID and the full normalized request, including a
  caller's optional generation constraint. Exclude generated transport metadata.
- The same key/input returns the existing job; changed input conflicts. Keep the
  fingerprint after input cleanup so idempotency still works.
- Use the job UUID as the stable outer daemon request ID; no extra column is needed.
- Save the selected generation before sending. Recovery must not silently target
  a replacement runtime, even if it has the same machine ID.
- Preserve daemon responses on operation errors and partial batch failures.
  `error` describes gateway failures; operation errors already live in `response`.
- A valid batch envelope may report outer `status: "ok"` while its result has
  `succeeded: false`. That produces a failed gateway job with all item results kept.
- A nonzero command exit or launch failure remains an execution result according
  to the existing protocol. A successful start response containing a running handle
  completes the job; it does not start gateway monitoring of the command.

There is no job-attempt table for automatic command retries. Transport recovery
queries daemon receipts using the job UUID and original runtime generation. The
gateway never resends an ambiguous request or batch.

## 6. `job_requests`

| Column | Type | Purpose |
| --- | --- | --- |
| `job_id` | uuid | Primary key and foreign key to `jobs.id` |
| `request` | json | Complete normalized single-operation or batch input |
| `expires_at` | timestamptz, nullable | Input cleanup deadline |

Keep operation parameters and retry identities, batch mode/item IDs, and any
caller-specified generation constraint. Machine identity is stored on `jobs`.
Transport version and outer correlation ID are gateway-owned; the actual dispatch
generation is stored on `jobs` before sending.

Insert job and request together before returning `202`. Keep input for all
nonterminal states. The agreed starting retention is configurable, initially
seven days after terminal completion. Set expiry in the completion transaction;
cleanup checks terminal state before deleting payloads.

Removing input does not remove the job, fingerprint, response, or notification.
Job detail must distinguish expired input from an empty request. Response,
notification, and idempotency-history retention remain separate policy decisions;
no automatic purge is implied by the input retention window.

No separate batch tables are necessary: the input and result arrays are bounded
by the existing protocol, and items share one submission and lifecycle.

## 7. `webhook_deliveries`

This table is the durable terminal-job notification outbox as well as delivery state.

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key; stable delivery/event ID |
| `job_id` | uuid | Completed job |
| `user_id` | uuid | Owning user |
| `event_type` | text | `job.succeeded`, `job.failed`, or `job.unknown` |
| `callback_url` | text | Destination captured for this event |
| `payload` | json | Immutable notification payload |
| `status` | text | `pending`, `delivering`, `retry_wait`, `delivered`, or `failed` |
| `next_attempt_at` | timestamptz | Next delivery eligibility |
| `lease_token` | uuid, nullable | Current worker claim |
| `lease_expires_at` | timestamptz, nullable | Claim expiry |
| `retry_from_attempt` | integer | First lifetime attempt number in the current retry cycle |
| `retry_started_at` | timestamptz | Current retry window start |
| `created_at` | timestamptz | Event creation time |
| `delivered_at` | timestamptz, nullable | Last successful delivery |

Enforce unique `job_id`: one terminal event per job, including batches. Add unique
`jobs(user_id, id)` and an ownership-enforcing composite foreign key from
`(user_id, job_id)`. Lease fields are paired; retry/attempt numbers are positive.

Save terminal job state, request expiry, and this event in one transaction.
The current daemon has no acknowledgement message, so none is sent. The planned
receipt extension must acknowledge only after that commit and allow repeated
acknowledgement if the gateway crashes between commit and acknowledgement.

Delivery claims use short transactions and lease fencing. Do not hold a database
transaction during callback I/O. Retry delivery independently of execution.
Manual redelivery starts a new retry cycle for a delivered/failed event while
preserving ID, payload, captured destination, previous successful delivery time,
and attempt history. It does not rerun the job. A later success updates `delivered_at`.

Use the current user signing secret when preparing a delivery. Disabled users
receive no new delivery claims; already dispatched HTTP calls may finish.
Callback failure never changes the stored job outcome. There are no automatic
command-exit events.

## 8. `webhook_delivery_attempts`

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | uuid | Primary key |
| `delivery_id` | uuid | Foreign key to `webhook_deliveries.id` |
| `attempt_number` | integer | Increasing lifetime attempt number |
| `started_at` | timestamptz | Attempt start |
| `finished_at` | timestamptz, nullable | Attempt completion |
| `http_status` | integer, nullable | Observed HTTP status |
| `error` | json, nullable | Safe delivery error |

Enforce unique `(delivery_id, attempt_number)` and positive attempt numbers.
Do not store arbitrary callback response bodies. A lost acknowledgement remains
an ambiguous delivery attempt, not evidence that the receiver rejected the event.

## Initial indexes

In addition to primary keys and unique constraints, start with indexes supporting:

- Users and user keys by creation time/ID; keys scoped by user.
- Live machines by user and creation time/ID.
- Registration tokens by machine; token lookup is covered by the unique hash.
- Jobs by user and creation time/ID, and by user/machine and creation time/ID.
- Nonterminal jobs by status for dispatch and recovery.
- Job input payloads by non-null expiry for bounded cleanup.
- Deliveries by user and creation time/ID, and by job through the unique constraint.
- Runnable deliveries by next attempt time, and claimed deliveries by lease expiry.

Attempt lookups use their parent/attempt-number unique index. Add further indexes
only when concrete query plans justify them; no JSON-field indexes are required.

## Transactions and recovery boundaries

1. Authenticate and resolve user-scoped idempotency. For new work, validate machine
   ownership/usability and input. Atomically insert job and request before acceptance.
2. Recheck authorization and pin the live runtime generation before first dispatch.
   Record the dispatch transition before sending bytes to the daemon.
3. Await the response or recover its receipt after reconnect. Validate request ID,
   machine, protocol version, and original runtime generation. Recovery never dispatches work.
4. On a terminal result, save job response/state, input expiry, and the outbox event
   atomically, then acknowledge persistence. Duplicate and late responses for already
   terminal jobs are acknowledged without altering the result or creating another event.
5. Claim/send/finalize callbacks independently using fenced delivery leases.
6. Delete only eligible expired input payloads; do not delete their parent jobs.

The daemon retains accepted request receipts and complete results across network loss.
These are operation/batch responses, not automatic command tracking. A daemon restart
changes generation and loses its in-memory records. Unrecoverable requests become
explicitly unknown rather than automatically executing again. Startup sets a 24-hour
recovery deadline only if one is not already set; reconnect acceptance clears it.
Migration 0002 adds these columns without changing existing job records' legacy behavior.

## Deployment assumptions and remaining decisions

The first deployment has one Rust gateway process owning machine connections,
dispatch/reconciliation tasks, and independent webhook tasks. A session advisory
lock held through serving and shutdown enforces single-instance ownership. No
gateway-instance, connection-lease, or routing tables are part of this schema. Multiple replicas
require an explicit connection ownership/routing design; a generic job lease or
stored online flag alone is insufficient.

Registration TTL is 15 minutes, queued dispatch deadline is 30 seconds, and
input retention defaults to seven days after completion. The gateway admits at
most 256 outstanding jobs per user and dispatches at most 32 per connection.
Delivery uses 60-second leases, ten-second HTTP calls, and eight attempts per
24-hour retry cycle; see the API document for signing and retry details.

Runtime database connections use five-second acquisition/statement limits,
three-second lock waits, and a 30-second idle-transaction limit. Database operations
release connections while awaiting machine or callback I/O.

Daemon receipts are bounded to 128 requests and 256 MiB, with 8 MiB reserved per
pending response, and remain until acknowledged or the runtime exits. Longer-term
result/history retention and credential-history cleanup remain separate policies;
this release does not automatically purge those records. Secret values are not
logged, and raw database or callback error bodies are not public API errors.
