# v0.2 compatibility and lifecycle contract

This document describes the implemented correctness phase, not a completed v1.0 release.

## Existing commands

The repository root remains buildable with `go build .`. Existing `-l`/`-listen`, `-t`/`-target`, `-unix-mode`, `-dial-timeout`, `-idle-timeout`, `-v`, `connect`, target shorthand and `socks-stdio` remain supported. In `connect`, options precede the target, as required by Go's flag parser:

```sh
tcp2socket connect -dial-timeout 5s -v 127.0.0.1:8999
```

TCP and absolute Unix endpoint syntax is unchanged. STDIO stdout contains only application/protocol bytes; help, logs and errors go to stderr. SOCKS5 remains a single no-auth CONNECT session per process. There is no new network SOCKS listener.

Exit codes remain: 0 for completion, help or handled shutdown; 2 for flag syntax/missing arguments; 1 for invalid endpoint/value or runtime failure. New negative timeout/resource values and Unix modes outside 0000–0777 are rejected. Signal cancellation is reported in verbose terminal events but retains legacy success status; a future versioned CLI may introduce distinct signal codes.

## Correctness changes

- stdin EOF half-closes the target writer and continues draining its response without the old 200ms cutoff. An indefinitely silent target can still require external cancellation; no implicit drain timeout is introduced.
- In STDIO modes, target EOF ends the process even if stdin remains open. STDIO cannot independently signal output EOF while preserving an indefinitely open input direction. This policy is explicit and differs from full-duplex socket proxy mode, which waits for both EOFs.
- Fatal copy/half-close errors close both directions and surface an error instead of silently succeeding.
- Context cancellation closes both owned streams and joins copy goroutines. STDIO handles are owned and closed by the adapter; it does not claim deadline support.
- Idle timeout measures successful read/write activity across both directions, including after a half-close. It does not limit total connection lifetime. Default 0 remains disabled.
- On shutdown the listener closes immediately. Existing work may drain until `-shutdown-timeout` (default 10s), then is canceled and closed. Queued work is discarded; no new target dials begin for queued connections after shutdown is observed. A timeout of 0 forces immediate cancellation.

## Resource bounds

Proxy mode defaults to 128 active connection workers and an accepted-connection queue of 64. Overflow is closed immediately; verbose logging reports `reason=capacity`. `-max-pending-accept 0` disables userspace queuing. The OS listener backlog is separate. Queued clients are not target-connected until a worker is available. The limits also bound dial concurrency.

Each ordinary relay uses two copy goroutines and bounded copy buffers. Idle tracking uses one timer per active relay, not recurring goroutines. Slow destinations apply normal socket backpressure; there are no unbounded payload queues. Limits apply to this process, not to other applications or OS kernel socket buffers.

## Unix socket safety

The legacy permission default is still 0660. Prefer a private 0700 parent directory and an explicit 0600 socket for sensitive services. Existing regular files and symlinks are never intentionally removed. An existing socket must belong to the current UID and return ECONNREFUSED before it is treated as stale. Active, inaccessible and indeterminate sockets cause startup failure. This liveness check creates and immediately closes a local connection but sends no data.

Cleanup compares file identity and disables Go's unconditional unlink-on-close behavior, so an already observed replacement path is preserved. Lstat/check/unlink is not an atomic filesystem transaction: a hostile process able to mutate the parent directory can still race it. A private parent directory remains a security requirement; this code is not a defense against a malicious same-UID process.

## Rollback

Keep the previous binary separately and stop the upgraded listener before replacing it. No persistent configuration or protocol migration is introduced. Rolling back restores the previous lifecycle bugs and unbounded accept behavior, so use only in a controlled environment.
