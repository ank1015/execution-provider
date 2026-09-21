# process-execution-core

A Rust library that owns local command executions, pipes and terminals, input queues,
output history, complete run-output files, and completion results. Linux, macOS, and
Windows share the same API.

The embedding application handles remote access, owner routing, execution expiration,
and sandbox pause/resume. This crate has no server, wire protocol, or execution-expiry timer.

## Quick start

Create the core inside a Tokio runtime and call `shutdown()` before stopping that runtime.

```rust,no_run
use process_execution_core::{Command, Config, ProcessExecutionCore, StartRequest};

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let core = ProcessExecutionCore::new(Config::new(std::env::current_dir()?))?;
let mut request = StartRequest::new("build-001", Command::program("cargo", ["build"]));
request.wait_ms = 1000;

let observation = core.start_execution(request).await?;
println!("{:?}", observation.execution.state);

// Use observation.execution.handle and observation.next_cursor to observe later.
// Shutdown stops active commands; do this when the host is actually shutting down.
core.shutdown().await?;
# Ok(())
# }
```

The [complete example](examples/run.rs) prints command output through completion:

```sh
cargo run -p process-execution-core --example run
```

## Operations

`ProcessExecutionCore` is cloneable. All clones address the same runtime generation.
An `ExecutionHandle` contains the execution ID and generation ID. Handles from another
generation are rejected before any operation occurs.

| Method | Input | Return |
|---|---|---|
| `new` | `Config` | Core runtime |
| `runtime_info` | — | Generation, resolved default shell, capabilities |
| `start_execution` | `StartRequest` | Execution snapshot and initial output |
| `run_execution` | `RunRequest` | Final execution, complete output-file metadata, and bounded tail output |
| `terminate_run` | Run ID, optional grace duration | Idempotent pending/terminating/finished receipt |
| `get_execution` | Handle | Execution snapshot |
| `observe_execution` | `ObserveRequest` | Snapshot, output, cursor, gap/more indicators |
| `write_input` | Handle, input ID, bytes | Accepted-byte receipt |
| `close_input` | Handle | Piped stdin closure state |
| `interrupt_execution` | Handle, operation ID | Snapshot acknowledging the interrupt request |
| `terminate_execution` | Handle, optional grace duration | Snapshot acknowledging termination |
| `resize_terminal` | Handle, rows, columns | Snapshot with applied dimensions |
| `list_executions` | `ListRequest` | Filtered page and next-page cursor |
| `shutdown` | — | Waits for accepted executions to finish cleanup |

All methods except `new` and `runtime_info` are async. Public request/result types
are serializable for adapters; the crate itself does not prescribe an encoding.

## Commands and shells

`Command::Program` launches an executable with an argument array. Shell expressions,
variables, pipes, and redirects are not interpreted. Windows batch files require
shell execution with `cmd.exe`.

`Command::Shell` executes a script. Selection order is the request's explicit `Shell`,
the configured default, then automatic discovery:

| Platform | Automatic shell selection |
|---|---|
| Linux | Supported user login shell, bash, zsh, `/bin/sh` |
| macOS | Supported user login shell, zsh, bash, `/bin/sh` |
| Windows | `pwsh`, `powershell`, `cmd.exe` |

An explicit `Shell` supplies both its executable path/name and `ShellKind`; its path
is honored and an unavailable shell is an error. Execution snapshots include the
resolved shell. `login` defaults to false through `Command::shell`: Unix shells use
`-c` (`-lc` when true); PowerShell uses `-NoProfile -Command` (omits `-NoProfile` when
true); cmd uses `/d /s /c`. Supporting several operating systems does not translate
scripts between their shell languages.

The runtime captures its environment at construction. A start may additionally request
an interactive shell snapshot using a stable `ShellSnapshotRequest.scope_id`. On Unix,
the runtime starts the selected login shell, explicitly sources `.zshrc`, `.bashrc`, or
`$ENV` as appropriate, and captures its exported environment, functions, options, and
aliases. Shell state is restored before parsing the requested script; direct programs
receive the captured environment. This makes tools installed by user profile managers
available even when the host daemon itself was launched non-interactively.

Snapshots are cached in memory by scope, selected shell, and cwd. Capture is time- and
size-bounded, successful entries are LRU-bounded, and failed captures back off before
retrying. Failure is fail-open: the requested command still starts with the ordinary
runtime environment. PowerShell and cmd snapshots are currently unsupported. Captured
values never appear in runtime information, execution snapshots, or gateway responses.

Environment precedence is daemon environment, captured profile, explicit `Config.env`,
then execution-specific overrides. Overrides use case-insensitive names on Windows.
Relative execution working directories resolve against the configured working directory.

## Lifecycle and output

Executions move through `Starting`, `Running`, `Stopping`, and `Finished`. `Stopping`
also covers cleanup and output draining after the direct child exits. The final
result distinguishes normal exit (including nonzero codes), timeout, requested
termination, launch failure, and lost outcome. A command failing to spawn remains a queryable
`StartFailed` result; invalid requests are function errors.

Accepted work belongs to the runtime. Dropping a start/observe future, timing out an
external request, or disconnecting a client does not terminate that work. The core
starts background supervision before its first await after accepting a start.

`start_execution` waits for the launch attempt, then optionally collects initial output.
`wait_ms` controls observation time; it never limits process lifetime. `observe_execution`
supports two wait modes:

- `Activity`: returns when new output or an execution change is available.
- `FinishedOrTimeout`: collects until completion, the response fills, or the wait ends.

`run_execution` is the non-interactive completion primitive. It always uses closed-stdin
pipes, optionally enforces `timeout_ms` after a successful launch, and returns only after
process cleanup and output draining finish. Every captured byte is streamed to a private
machine-local file while the result includes only a bounded tail preview (64 KiB by
default and at most 1 MiB). The file metadata includes an artifact ID, absolute path,
size, SHA-256, completeness, and expiry. Stdout and stderr identities are retained in
the preview; the file itself contains their raw bytes in capture order.

`terminate_run` addresses this work by caller-stable `run_id`. It is safe to repeat. A
termination racing ahead of run admission is retained, so the corresponding run is
completed as terminated without launching. The first timeout or explicit termination
cause wins.

Output is raw bytes in a bounded **in-memory journal**. Pipes retain separate stdout
and stderr streams; PTYs produce one terminal stream. Pipe stream ordering reflects
capture order, not a guaranteed ordering of writes in the child.

Each reader maintains its own cursor. Reading never consumes another reader's data.
Omitting a cursor reads from the beginning of retained history. `max_output_bytes`
limits raw output bytes in a response, excluding metadata. Large chunks can be split
across responses without loss. A finished execution can still have `has_more: true`.

Oldest bytes are discarded when retention capacity is exceeded. `output_gap` reports
that a requested position is no longer retained. `output_incomplete` reports an I/O
failure or output-drain deadline that prevented complete capture. Polling does not
renew retention or extend a command's lifetime.

`Finished` is published after output collection is finalized. The direct child defines
the execution lifetime: when it exits, remaining owned descendants are cleaned up.
Launch development servers in the foreground; keep an interactive shell alive when
you want multiple commands within the same shell state.

## Input, retries, and limits

`IoMode::Pipes { stdin: false }` is the default. Set stdin to true for writable pipes,
or use `IoMode::Pty { rows, cols }` for a terminal.

Input writes are ordered, bounded, and deduplicated by input ID. A receipt means bytes
were accepted into the queue, not that the application consumed them. No newline is
added. Empty input is a no-op. A delivery error is exposed as `execution.input_error`.
`close_input` rejects new writes, drains accepted writes, and closes piped stdin.
Repeated closure is safe. PTYs do not support pipe-style EOF; send appropriate terminal
input instead. Windows terminal line input commonly needs `\r` for Enter.

Reusing a start ID with identical arguments returns the same execution, without
repeating the initial collection wait. Different arguments produce an idempotency
conflict. Run IDs retain the same final execution and artifact for
`run_output_retention`; changed arguments conflict. Input IDs similarly reject different bytes. Interrupt operation IDs retain
their first receipt. Repeated termination preserves the first termination deadline.
Termination acknowledgement does not mean cleanup is complete; observe the execution.

Start execution and retry records share `finished_retention`. Run records and their
files use `run_output_retention`; the output file is removed when that record expires.
After retention ends, IDs no longer deduplicate requests. The caller must use new unique
IDs for new work. Input/interrupt receipt counts are bounded; reaching a limit rejects
new operations rather than forgetting existing receipts.

Listing supports active/finished/all state, exact label matches, and pagination. A page
cursor fixes the upper creation sequence, excluding later starts. States can change
and records can expire between pages. Count `Starting` and `Stopping` as active when
making outer sandbox-idle decisions, and coordinate those decisions with new starts.

Default limits are 64 active executions, 1,024 retained records, 1 MiB retained output
per execution, 64 KiB output per response, a 1 MiB input queue per execution, 15 minutes
of finished-start retention, 24 hours of run-output retention, a 2-second termination
grace period, and a 1-second output drain.
`Limits` also bounds observation waits and retry receipt counts. Capacity exhaustion
rejects new starts without evicting active work.

## Filesystem operations

The core also provides bounded whole-file metadata/read operations, conditional and
overwrite writes, and conditional removal for trusted callers. Relative paths resolve
against the requested cwd and then the configured runtime cwd. Reads return binary data and SHA-256; protocol
adapters choose the wire encoding. The default read and write limit is 5 MiB.

Writes use a temporary file in the destination directory followed by atomic replacement.
They can create missing parent directories. Conditional writes and removals require either
a missing-file or SHA-256 precondition. Overwrite writes require no old hash; only the new
bytes must fit the write limit, so an old file larger than 5 MiB can be replaced.
Overwrite follows a final symlink, including a dangling link, and leaves it in place;
conditional writes retain their existing behavior of replacing the link after reading
through it. A replacement detaches the destination from any other hard links, which keep
the old contents. Matching content within the read limit is an `already_applied` no-op.
Atomic replacement concerns reader visibility, not power-loss durability or external
compare-and-swap.

Mutations affect regular files only and use a stable mutation ID. Identical retries
return the retained receipt; changing the mode or any other request input under the
same ID conflicts. The core retains 4,096 mutation receipts by default, and can recognize
an already achieved final state after receipt loss or a runtime restart.

## Platform behavior

Linux and macOS use process groups, native signals, and nonblocking Unix PTYs.
Interrupt targets a terminal's foreground process group when available. Termination
requests SIGTERM and escalates to SIGKILL. Programs that create another process group
or session can leave the managed group; this core is not an OS isolation boundary.
Inherited output handles cannot keep finalization open beyond the configured drain
deadline.

Windows requires Windows 10 version 1809 or newer, or Windows Server 2019 or newer.
It uses ConPTY for terminals and Job Objects for process ownership. The job is assigned
in `CreateProcessW` before user code executes. Pipes use an explicit inherited handle
list. Terminal interrupt sends Ctrl-C through the input queue; applications may handle
or ignore it. Terminal termination attempts Ctrl-C, then terminates the job after its
grace period. Piped Windows executions have no universal graceful interrupt: interrupt
returns `UnsupportedOperation`, and termination directly terminates the job.

`shutdown()` rejects new starts, terminates accepted executions, and waits for their
results/output to finalize. Dropping the last core clone requests cleanup while its
Tokio runtime remains alive; explicit shutdown is the supported way to await it.
Commands, execution state, and retry records are not restored after a runtime restart.
Run files already written can remain on disk, but their in-memory artifact records are
lost. A new runtime has a new generation ID.

## Development

```sh
cargo fmt --all -- --check
cargo clippy -p process-execution-core --all-targets -- -D warnings
cargo test -p process-execution-core
```

Integration tests compile a small Rust child program and exercise real processes:
binary output, stream separation, argument/context handling, cursor replay, retention
gaps, both wait modes, input deduplication/EOF, PTYs, resizing, concurrent retries,
cancellation, termination, descendant cleanup, listing, and resource limits.

The workspace CI runs checks and tests natively on Linux, macOS, and Windows.
Cross-compilation alone does not verify runtime behavior on another operating system.
