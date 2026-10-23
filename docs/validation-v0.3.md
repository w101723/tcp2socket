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

## Gate status (pending)

- [x] PR-05B handshake + session ID isolation tests
- [x] PR-05C heartbeat + degraded handling tests
- [ ] PR-05D doctor/version CLI
- [ ] Full CI matrix green on the gate commit
- [ ] Fault injection summary (EOF, half frames, simultaneous close, stale PONG)
