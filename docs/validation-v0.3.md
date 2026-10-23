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

## Gate status (pending)

- [ ] PR-05B handshake + session ID isolation tests
- [ ] PR-05C heartbeat + degraded handling tests
- [ ] PR-05D doctor/version CLI
- [ ] Full CI matrix green on the gate commit
- [ ] Fault injection summary (EOF, half frames, simultaneous close, stale PONG)
