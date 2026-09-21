# process-execution-protocol

Shared version 5 execution/filesystem RPC types, dispatch, JSON byte/cursor encoding, and
batching. Both `process-execution` and `process-execution-host-daemon` use this crate.
It embeds no network transport and owns no processes independently of the supplied core.

The [execution API reference](../../apps/process-execution/README.md#protocol-version-4)
documents the single-operation requests and responses. Version 4 adds `execution.run`,
`execution.terminate_run`, timed-out results, and complete machine-local output artifacts.
`filesystem.write_file` also accepts an additive `mode: "overwrite"` without a
precondition; omitted mode retains the required conditional precondition. The host
advertises this extension through `runtime.filesystem.overwrite`.
`runtime_config::RuntimeConfig` provides the common JSON configuration
for an embedded core.

## Dispatch

Construct `Dispatcher::local(core, binary_info)` for private local IPC, or
`Dispatcher::gateway(core, binary_info)` for the host's authenticated gateway session.
The gateway dispatcher rejects `runtime.shutdown`. The dispatcher is a policy layer;
the caller is responsible for authenticating the transport and authorizing host access.

`dispatch(Request)` returns a `Response`. `is_ok()` reports whether the envelope was
accepted. `succeeded()` also accounts for errors/skips within a valid batch.
`encode_response` applies the shared 8 MiB response limit. Transport envelope overhead
must also fit within its transport's limit.

## Batches

Send `operations` instead of `operation`/`params`:

```json
{
  "protocol_version": 5,
  "request_id": "batch-1",
  "mode": "parallel",
  "accepted_error_codes": ["not_found"],
  "operations": [
    {"request_id": "info", "operation": "runtime.info"},
    {"request_id": "list", "operation": "execution.list", "params": {}}
  ]
}
```

An optional outer `expected_generation_id` fences the entire batch. A malformed batch,
duplicate request IDs, or a generation mismatch rejects the envelope before dispatch.
There must be 1–32 operations. Request IDs contain 1–256 bytes and must be unique within
the batch. Nested batches, mixed single/batch envelopes, batched shutdown, and batched
run termination are rejected.

- `sequential` (default): dispatch in input order, waiting for each operation response.
  On the first operation error, remaining operations are marked `skipped`.
- `parallel`: run up to eight operations at once. Errors do not skip other operations.
  Results are returned in input order, regardless of completion order.

`accepted_error_codes` is optional and defaults to an empty list. An operation error
whose code is listed remains an `error` item in the response, but it counts as successful
for the outer batch. In sequential mode it also does not skip later operations. This is
intended for expected outcomes such as probing for an optional file with `not_found`;
unlisted errors and all skipped operations still fail the batch.

A valid batch returns `status: "ok"` and this result structure:

```json
{
  "succeeded": false,
  "results": [
    {"request_id": "one", "status": "ok", "result": {}},
    {"request_id": "two", "status": "error", "error": {"code": "invalid_argument", "message": "..."}},
    {"request_id": "three", "status": "skipped", "reason": "a previous operation failed"}
  ]
}
```

`succeeded` is true only when every operation succeeded or returned an explicitly
accepted error code. The local `rpc` command exits with code 1 for a batch containing an
unaccepted error or skip, while still printing its complete response. A failed command
launch or nonzero command exit remains an execution result, not an operation error, as
with single requests.

Sequential dispatch orders **operation responses**, not process completion. A start
can return while its command is still running. Use a shell script such as `build && test`
when one command must finish before the next begins. References to earlier batch results
are not supported; newly returned handles require a subsequent request.

No rollback occurs. Each operation retains its existing retry semantics: `start_id`,
`run_id`, `input_id`, interrupt `operation_id`, and filesystem `mutation_id` still
matter. `request_id` is correlation in the local RPC transport. Negotiated gateway
recovery also uses the outer ID for in-memory deduplication until acknowledgement. An accepted batch continues if its client disconnects while its
host remains running. Lost responses can be recovered through observation/listing and
appropriate per-operation retries. `execution.run` can keep a batch open until its
process finishes. `execution.terminate_run` is a separate envelope so reserved control
capacity can deliver it. Daemon shutdown can interrupt unfinished batch work.

A future sandbox adapter can hold one activity lease around the whole batch. Releasing
that lease does not imply that the sandbox is idle: active commands and other requests
must still be considered before pausing it.

## Gateway transport contract

`gateway::{HostMessage, GatewayMessage}` defines the version 5 handshake:

1. The daemon connects to `wss://GATEWAY/BASE/v1/machines/MACHINE_ID/connect`, authenticating
   with an `Authorization: Bearer ...` header. It sends a `hello` containing protocol
   version, stable installation ID, gateway-issued host ID, runtime info, binary info, and optional `request_recovery: true`.
   The wire field `host_id` contains the machine ID for compatibility.
2. The gateway sends `welcome` with `protocol_version`, a UUID `connection_id`, and
   `heartbeat_interval_ms` (50–60,000; normally 15,000), and `request_recovery`
   indicating whether it supports the advertised feature. Missing flags default to false.
3. The gateway sends `{"type":"request","request":RPC_REQUEST}`. The host returns
   `{"type":"response","response":RPC_RESPONSE}`. Requests may complete out of order;
   correlate their request IDs. Each WebSocket text message contains one JSON envelope.
4. WebSocket Ping/Pong maintains liveness. Both peers must respond to Ping. The daemon
   reconnects after three heartbeat intervals without valid incoming traffic.
5. `{"type":"disconnect","code":"reconnect"}` requests a fresh connection.
   `revoked`, `replaced`, and `incompatible_protocol` stop the daemon and require local
   attention; they do not cause an authentication retry loop.

With recovery negotiated, additional envelopes carry `request_id` and `generation_id`:

| Direction | Type | Meaning |
|---|---|---|
| Host → gateway | `accepted` | Receipt recorded; operation or batch may still be running |
| Gateway → host | `recover` | Query an existing receipt, without dispatching work |
| Host → gateway | `missing` | No receipt in this runtime; outcome cannot be recovered |
| Gateway → host | `acknowledge` | Job durably settled; release its retained response |

The existing `response` envelope carries completed results. The daemon proactively resends
unacknowledged results after reconnecting. The gateway persists the response and outbox event
before acknowledging, and acknowledges duplicates without changing terminal results or
creating another event. A terminal `unknown` job remains immutable if its response arrives
later; acknowledging it releases the daemon's receipt.

The daemon reserves response capacity before accepting work. Repeated unacknowledged IDs
with the same normalized request recover their receipt; changed input is a protocol error.
The gateway uses a unique job UUID, never reuses an acknowledged ID, and never automatically
resends an operation. A receipt lookup does not observe the underlying command.

Reconnects preserve runtime generation; daemon restarts do not. Every gateway dispatch pins
`expected_generation_id`; recovery only queries the original generation. The daemon retains
at most 128 receipts/256 MiB, reserving 8 MiB for each pending response, until acknowledged
or the runtime exits. Peers without recovery negotiation use connection-scoped responses.

`RegistrationRequest` and `RegistrationResponse` share the HTTP enrollment contract:
`POST /v1/machines/:machineId/register`, with a registration token in the Authorization
header and `{ "installationId": "UUID" }`, returns `{ "machineId": "UUID", "credential": "..." }`.
User-to-machine authorization is enforced by the gateway. No gateway deployment URL is
built into the daemon.
