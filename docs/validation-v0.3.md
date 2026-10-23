# v0.3 validation report (in progress)

Per-PR records toward the v0.3 gate (plan §7). Nothing here claims v0.3 acceptance yet; the gate section at the bottom lists outstanding work.

## PR-05A — control frame codec (2026-10-23)

Scope: `internal/control` frame encode/parse/streaming-read with strict limits and a fuzz target. No handshake, no heartbeat, no session wiring, no CLI or proxy changes.

Decisions frozen by `TestHeaderLayout` and `TestHeaderRejections`:

- Header: magic `T2S1` (4) + version (1) + type (1) + flags (2, must be zero) + payload length (4, big-endian) + session ID (16) + sequence (4) = 32 bytes, network byte order.
- Accepted types: `0x01`–`0x06` (HELLO, HELLO_ACK, PING, PONG, ERROR, CLOSE); other values rejected at decode and encode.
- Version must equal 1; nonzero reserved flags rejected; declared payload length above 4096 rejected before any allocation or payload read; `Parse` rejects trailing bytes to surface stream desync.
- SessionID content and sequence semantics are not interpreted at this layer; old-session rejection and PING/PONG pairing belong to PR-05B/05C.

Local evidence (macOS 27.0.1 arm64, Apple M5, Go 1.26.1):

| Check | Result |
| --- | --- |
| `go test -race -count=2 -timeout 60s ./internal/control` | PASS |
| `go test ./internal/control -run '^$' -fuzz '^FuzzDecodeFrame$' -fuzztime 10s -parallel 2` | PASS, 1,902,020 executions |
| `go vet`, `gofmt -l` | clean |

Not covered by this PR: HELLO/ACK state machine, heartbeat timeouts, doctor, and the v0.3 gate (full-matrix CI rerun,故障注入汇总, `session`/`control` 互通)。

## PR-05B — HELLO/ACK handshake (2026-10-23)

Scope: `internal/control/handshake.go` — initiator/responder handshake over the frozen frame layer, capability payload, structured ERROR payload, session-ID isolation helper. No heartbeat, no CLI or proxy changes.

Decisions frozen by tests:

- Initiator sends HELLO with a caller-generated fresh session ID; the responder validates structure (frame layer) then permission (capability subset) and answers HELLO_ACK with the identical session ID and the accepted capability list (must equal the requested list).
- Capability payload = one byte per capability; zero codes and duplicates are protocol errors (ERROR 0x02); well-formed codes outside the responder's permission are rejected with ERROR 0x01; v0.3 defines only `heartbeat` (0x01).
- ERROR payload = code byte + detail text; codes 0x01 unsupported_capability, 0x02 protocol_error, 0x03 unauthorized. Detail is peer-controlled text and must never carry credentials.
- Both sides reach READY only after the initiator verified the ACK and the responder fully wrote it; any EOF, timeout or mismatch stays non-READY.
- Handshake timeouts require deadline support: a stream without `SetDeadline` fails closed instead of allowing an unbounded handshake; deadlines are cleared on success.
- `CheckSession` rejects any post-handshake frame whose session ID differs from the negotiated one — the primitive stale-session isolation (PR-05C heartbeat) builds on.

Required cases from the plan, now passing: `TestHandshakeSuccess`, `TestUnsupportedVersion` (both roles), `TestUnknownRequiredCapability`, `TestOldSessionID`, `TestHandshakePeerEOF`, plus timeout, wrong-first-frame, capability-structure and ERROR-payload cases. `TestTruncatedFrame` / `TestOversizedPayload` were covered by PR-05A. Two additional fuzz targets (`FuzzParseErrorPayload`, `FuzzParseCapabilityList`) guard the new payload parsers.

Local evidence (macOS 27.0.1 arm64, Go 1.26.1):

| Check | Result |
| --- | --- |
| `go test -race -count=2 -timeout 90s ./internal/control` | PASS |
| Fuzz `ParseErrorPayload` / `ParseCapabilityList` / `DecodeFrame`, 5s each | PASS, ~3.0M executions total |
| `go vet`, `gofmt -l` | clean |

An earlier draft pre-validated the capability list in the initiator and deadlocked the failure path on synchronous pipes; structure validation now belongs to the responder, whose best-effort ERROR gives both sides a fast, structured failure. Error wrapping uses multi-`%w` so callers can `errors.Is` both the handshake sentinel and the underlying cause (deadline exceeded, version mismatch, session mismatch).

Not covered by this PR: heartbeat/DEGRADED (PR-05C), doctor (PR-05D), gate (full CI rerun, fault-injection summary, session/control interop).

## PR-05C — heartbeat / degraded failure detection (2026-10-23)

Scope: `internal/control/heartbeat.go` — PING/PONG exchange over an established READY session, consecutive-miss budget, single-writer goroutine, fail-once callback contract. Session integration is test-verified only; no CLI or proxy changes and no heartbeat is forced onto ordinary TCP forwarding.

Decisions frozen by tests:

- Config `Interval/Timeout/MaxMissed`, defaults 10s/5s/3 (plan values); detection latency bounded by roughly `Interval*MaxMissed`. First PING is sent immediately; invalid config fails `StartHeartbeat` before any goroutine starts.
- PONG must echo the session ID and reference an outstanding sequence; a duplicate, stale or unsolicited PONG fails the heartbeat immediately (`ErrStalePong`), as does any foreign session ID (`CheckSession` from PR-05B). `missed` resets only on a valid PONG; `missed >= MaxMissed` fails with `ErrHeartbeatTimeout`.
- The plan's "single writer goroutine" is implemented literally: all frames toward the peer go through one bounded queue (depth 16) owned by one writer goroutine, so PINGs and PONG replies never interleave and a blocked write can never stall the read loop. This is what keeps synchronous transports (and the future mux) deadlock-free: the first draft answered PINGs from the read-loop goroutine and deadlocked two net.Pipe peers where every goroutine was simultaneously a blocked writer.
- Failure contract: `fail(err)` runs exactly once — it stops the writer, then invokes `onFail(err)`. `Close()` stops without invoking `onFail`. Neither waits for a write blocked on a stalled transport; the caller closes the transport first. Marking the session DEGRADED, closing the transport and finishing the session are the caller's job; there is no automatic re-auth or reconnect.
- A stalled peer (stops reading, never answers) is detected by the missed-pong budget even while a write is blocked, because only the writer goroutine is stranded. This is stronger than the intermediate serialized-mutex design, in which one blocked write froze the whole state machine.

Required cases from the plan, now passing: timeout budget (`TestHeartbeatTimeout`), duplicate/stale PONG (`TestDuplicatePong`, `TestUnsolicitedPong`), foreign session ID (`TestHeartbeatForeignSession`), peer EOF (`TestHeartbeatPeerEOF`), simultaneous close of two live heartbeats with a goroutine-leak check (`TestSimultaneousClose`), write stall without state-machine stall (`TestWriteStall`), mutual echo loop with clean shutdown (`TestHeartbeatEchoLoop`), and the session contract (`TestHeartbeatMarksSessionDegraded`): on failure the callback marks READY→DEGRADED (observed as a permanent state event; the snapshot advances to CLOSING immediately by design), the session refuses new work, explicit Finish closes it exactly once, and no path returns it to READY.

Local evidence (macOS 27.0.1 arm64, Go 1.26.1):

| Check | Result |
| --- | --- |
| `go test -race -count=2 -timeout 90s ./internal/control` | PASS |
| Fuzz `DecodeFrame` / `ParseErrorPayload` / `ParseCapabilityList`, 10s each | PASS |
| `TCP2SOCKET_TEST_RACE=1 go test -race -count=1 -timeout 240s ./...` | PASS (all 8 packages incl. integration) |
| `go vet`, `gofmt -l` | clean |

Not covered by this PR: wiring the heartbeat into a live CLI session (v0.3 keeps session control as a foundation layer), doctor (PR-05D), gate (full CI rerun, fault-injection summary, session/control interop).

## PR-05D — doctor / version CLI and stable event logs (2026-10-23)

Scope: `internal/doctor`, `internal/buildinfo`, `internal/logging`, CLI wiring (`doctor`, `version`, `--version`), `proxy.Event.SessionID`. No change to forwarding paths, no handshake forced onto ordinary TCP, no persistent management port, no new third-party dependency.

Spec compliance (plan §5):

- `doctor` runs bounded read-only checks, each reported PASS/WARN/FAIL/UNAVAILABLE under a per-check timeout (default 3s, hard cap 30s). Exit code 1 only when a check FAILs; WARN and UNAVAILABLE keep exit 0. Default checks: `build_info` (WARN while ldflags metadata is dev/unknown), `loopback_tcp` (127.0.0.1 echo probe verifying the accepted source stays loopback — the dubhe-vpn unconditional-SNAT signature, surfaced without privileges or tcpdump), `unix_socket` (temp-dir listen/dial/echo/cleanup, files removed afterwards), `fd_limit` (UNAVAILABLE where the platform does not expose rlimits).
- `--inspect-nat` is opt-in and Linux-gated; it runs `iptables-save -t nat` read-only and flags MASQUERADE rules lacking a `-d`/`-o` condition (the loopback-rewrite shape) with a suggestion to add a loopback exception. Nothing is executed, modified or repaired; doctor never touches iptables/nft/container networking anywhere.
- `version` prints `version`/`commit`/`go`/`os-arch`, one key=value per line; ldflags targets `internal/buildinfo`, defaults stay `dev`/`unknown`; the additive proxy flag `--version` prints the same block.
- Event logs are unified through `internal/logging`: stable key=value lines, values quoted only when needed so field parsing survives. `closed`/`rejected` events carry `connection_id`, `source`, `reason`, `s2t`/`t2s`, `elapsed_ms`, and `session_id` only when a control session exists (`proxy.Event.SessionID` added; ordinary direct forwarding leaves it empty). STDIO modes keep every log on stderr; no event carries business payload or identity credentials. `session list/status/stop` is intentionally absent — no fake management commands without a designed local IPC and access model.
- Legacy compatibility: new subcommands and flags only; all existing invocations, exit codes and stderr notices parse exactly as before (integration `TestCLIHelpAndExitCodes`, `TestExpectedNewProxyFlags` still pass unchanged).

Tests: `internal/doctor` (exit codes via injected checks, status validation, deadline-bounded loopback probe, dubhe-vpn source classification, MASQUERADE parsing incl. long-form flags), `internal/logging` (field order, quoting, odd pairs, nil safety, concurrent line integrity), integration `TestVersionSubcommand`, `TestProxyVersionFlag`, `TestDoctorSubcommand`, `TestDoctorInspectNATPlatformGate`, `TestDoctorHelpFlags`.

Local evidence (macOS 27.0.1 arm64, Go 1.26.1):

| Check | Result |
| --- | --- |
| `go test -race -count=2 ./internal/doctor ./internal/logging` | PASS |
| Smoke: `version`, `doctor`, `doctor --inspect-nat`, `--version` | outputs as documented; doctor exit 0 (build_info WARN, nat UNAVAILABLE off-Linux) |
| Full matrix below | PASS |

## v0.3 Gate

### Fault injection summary

Plan §7 requires deterministic terminal states for: control-channel disconnect, PING/PONG loss, half frames with EOF, and simultaneous close on both ends. Evidence (all passing locally, see tables above):

| Fault | Tests | Outcome |
| --- | --- | --- |
| Transport EOF mid-handshake | `TestHandshakePeerEOF`, `TestHandshakeWrongFirstFrame`, `TestHandshakeTimeout` | structured failure, never READY, bounded by deadline |
| Half frames / truncated stream | `TestTruncatedFrame`, `TestReadFramePartialWrites`, `TestReadFrameTruncatedStream`, `FuzzDecodeFrame` (~1.9M execs) | rejected before allocation, no panic, no hang |
| Wrong version / oversized length / trailing data | `TestUnsupportedVersion`, `TestOversizedPayload`, `TestHeaderRejections`, `TestTrailingData` | rejected before payload read; 4096 limit proven |
| Stale / foreign session ID | `TestOldSessionID`, `TestHeartbeatForeignSession` | foreign frames terminate the heartbeat immediately; no cross-session READY |
| PING/PONG loss | `TestHeartbeatTimeout`, `TestWriteStall` | detection bounded by `Interval*MaxMissed`; `fail` exactly once even while a write is blocked |
| Duplicate / unsolicited PONG | `TestDuplicatePong`, `TestUnsolicitedPong` | immediate `ErrStalePong` termination |
| Simultaneous close on both ends | `TestSimultaneousClose` (incl. goroutine-leak check), session `Close`/`CloseAll`/cancel tests | both sides reach `Done`, no failure reported, no goroutines left |
| Transport stall (peer stops reading) | `TestWriteStall` | only the writer goroutine is stranded; missed-pong detection continues and bounds the failure |
| Signals / delayed EOF (legacy faults) | `TestProxySIGTERMBoundedShutdown`, `TestSocksHandshakeWaitingSIGTERM`, `TestDelayedResponseNoTruncation`, `TestTargetEOFStdinOpenProcessExit` | exit bounded, v0.2 behavior preserved |
| stdout discipline | `TestConnectStdoutNoPollution` + STDIO integration | proxy/connect/socks-stdio stdout carries protocol bytes only; only `version`/`doctor` (non-STDIO modes) write stdout reports |

Resource observability: `TestProxyResourceBaseline` samples the proxy **subprocess** (lsof on macOS, /proc/<pid>/fd on Linux) against a baseline; 100MiB hash, 32-way concurrency and 8-stream mixed scenarios pass with bounded FD growth. 64-way concurrency and 24h/72h soak are NOT claimed here (plan §7 lists them as further evidence, not a v0.3 gate item).

### Gate checklist

- [x] PR-05B handshake + session ID isolation tests
- [x] PR-05C heartbeat + degraded handling tests
- [x] PR-05D doctor/version CLI + stable event logs
- [x] Full CI matrix green on the gate commit `444dd46` (run 39909351577: ubuntu-latest/macos-15 Go 1.22.x + ubuntu-latest/macos-latest stable, incl. per-package fuzz steps)
- [x] Fault injection summary (EOF, half frames, simultaneous close, stale PONG)

Deferred to later stages per plan: v0.4 Mux design, v0.5 SOCKS listener/SSH transport, experimental PTY. The control layer remains decoupled from ordinary TCP forwarding.
