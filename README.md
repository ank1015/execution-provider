# Execution Providers

This repository is a Cargo workspace for multiple Rust applications and reusable packages.

## Layout

- `apps/` — executable crates
- `packages/` — reusable library crates

## Packages

- [process-execution-core](packages/process-execution-core/README.md) — local process
  supervision for Linux, macOS, and Windows, with pipes, PTYs, retained output,
  input, termination, and execution listing.
- [process-execution-protocol](packages/process-execution-protocol/README.md) — shared
  RPC types, dispatch, batching, runtime configuration, and gateway connection messages.

## Applications

- [process-execution](apps/process-execution/README.md) — a local supervisor binary
  with `serve`, `rpc`, `health`, and `version`; Unix sockets on Linux/macOS and
  named pipes on Windows.
- [process-execution-host-daemon](apps/process-execution-host-daemon/README.md) — an
  installed user-account host with an authenticated outbound gateway connection,
  reconnects, and an embedded execution runtime.
- [execution-gateway](apps/execution-gateway/README.md) — authenticated machine
  registration and command routing, durable operation/batch jobs, PostgreSQL
  persistence, and signed job-result webhooks.

## Binary releases

Changes to either execution binary on `main` publish Linux x86-64, Windows x86-64,
and universal macOS archives to
`gs://execution-provider-releases-361197090477`. Releases are private and available
through authenticated Cloud Storage access. Each release includes SHA-256 checksums
and a JSON manifest under both an immutable `releases/COMMIT_SHA/` path and `latest/`.

Create a new application:

```sh
cargo new apps/my-app --bin
```

Create a new package:

```sh
cargo new packages/my-package --lib
```

Run checks for the entire workspace:

```sh
cargo check --workspace
```
