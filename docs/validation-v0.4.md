# v0.4 validation report (in progress)

Per-PR records toward the v0.4 mux milestone. Nothing here claims v0.4 acceptance yet. v0.3 remains the last gated baseline (`docs/validation-v0.3.md`).

## PR-06A/06B — T2S2 protocol freeze + frame codec (2026-10-24, `f2d6afe`)

Scope: `docs/protocol.md` (T2S2 v1 specification) and `internal/mux/codec.go` — 38-byte header encode/parse, 12 frame types `0x01`–`0x0C`, CRC32C (Castagnoli) trailer, capability/target/error/window-update payloads. No session, no registry, no CLI changes.

Decisions frozen by `TestGoldenVectors` (byte-exact) and `TestHeaderRejections`:

- Header: magic `T2S2` (4) + version (1, =1) + type (1) + flags (2, must be 0) + reserved (2, must be 0) + payload length (4, BE, max 32768) + epoch (16) + stream ID (4) + sequence (4) = 38 bytes + 4-byte CRC32C trailer.
- `parseHeader` validates length before any allocation (oversized declared length rejected without reading payload); `ReadFrame` re-verifies the CRC over the full frame; `Parse` rejects trailing bytes.
- Session-scoped frame types (HELLO/HELLO_ACK/PING/PONG/GOAWAY) must carry stream ID 0.
- Golden vectors generated from the document alone; mismatches are interoperability failures.

Evidence (macOS 27.0.1 arm64, Go 1.26.1): `go test -race ./internal/mux` PASS; `FuzzDecodeFrame` 2.4M+ executions; `go vet`, `gofmt -l` clean. CI green on `f2d6afe`.

## PR-06C — stream registry and state machine (2026-10-24, `8972bd4`)

Scope: `internal/mux/stream.go` — registry with client-odd/server-even ID parity, `StreamOpen → HalfClosedLocal/Remote → Closed` transitions, per-frame-type sequence validation, receive-window ledger, stream-limit accounting. No session wiring.

Frozen behaviors: duplicate FIN/RESET rejected; DATA after FIN (state `closed`/`half_closed_remote`) rejected as `ErrDataAfterFin`; ghost frames reported as `ErrGhostStream`; sequence violations as `ErrSequence`; window overrun detected via the single receive ledger.

Evidence: `go test -race ./internal/mux` PASS, CI green on `8972bd4`.

## PR-06D — flow control and fair scheduling (2026-10-24, `77fa811`)

Scope: `internal/mux/flow.go` — send-side credit (`SendCredit`), session aggregate buffer (1 MiB, §5), round-robin scheduler with bounded per-stream queues (depth 16), control-frame priority, per-turn byte budget.

Frozen behaviors: `Acquire`/`Grant` never exceed 2^31−1 window; `ErrQueueFull` is backpressure, not an error path; scheduler `Next()` drains closed-but-nonempty queues before reporting `ErrSchedulerClosed`.

Evidence: scheduler fairness, control priority, bounded-queue and concurrency tests PASS under `-race -count=4`; CI green on `77fa811`.

## PR-06E — session assembly (2026-10-24, `f79ab5b`)

Scope: `internal/mux/session.go` — synchronous handshake (epoch negotiation, `serving` guard), single reader goroutine, single scheduler-fed writer goroutine, `Accept`/`Open`, `net.Conn`-style streams with half-close.

Key decisions (each backed by a regression test, defect re-injection verified where noted):

- **Close retires, never drops, the scheduler queue.** `RemoveStream` originally deleted the stream's outbound queue, losing up to a full window (64 KiB) of frames that a successful `Write` had already accepted — the peer saw a short transfer then RESET. The queue now stays until drained (`gcRetired` lazily reclaims it) and the close RESET is enqueued behind those frames. `TestMuxCloseFlushesWrittenData` fails deterministically (229376/262144 bytes) with the old semantics re-injected.
- **`fail()` records the peer-termination fact even when the stream already died locally**, so a later local `Close()` suppresses the answering RESET (§7.2: never answer a RESET with a RESET).
- **Read never returns `(0, nil)`**: a clean FIN surfaces as `io.EOF`.
- **Scheduler wakes the writer via a `Ready` channel** (`signalReady` never drops a wakeup; the writer parks instead of spinning). `TestSchedulerSignalNoLoss` / `TestSchedulerSignalAfterEmptyNext`.
- **`Registry.Consume` replaced by `GrowReceiveWindow`**: `CheckData`'s ledger is the single receive-window authority (the earlier two-ledger design double-counted in-flight credit and misjudged window overruns).

A `t.Logf`-in-`Serve`-goroutine data race (log after the subtest finished) was found by `-race -count=3` and removed — a test bug, not a product one.

Evidence: `go test -race -count=5 ./internal/mux` PASS ×2 rounds; full repo `go test -race ./...` (11 packages) PASS; CI green on `f79ab5b` (run 40058240379) and `a3526c5` (run 40058308102, which added the three mux fuzz targets to the CI fuzz step: `FuzzDecodeFrame`, `FuzzTargetDescriptor`, `FuzzErrorPayload`).

## PR-07 — stress and fault injection (2026-10-24, `53a23bd`)

Scope: `internal/mux/stress_test.go` plus four product fixes the stress runs exposed.

Stress coverage (all `-short`-skippable, full runs in CI):

| Scenario | Test | Outcome |
| --- | --- | --- |
| 1/8/32/64 concurrent streams, 64 KiB echo each | `TestMuxStressStreamCounts` | PASS; 64 deliberately exceeds the 16-deep accept backlog |
| 8 MiB single stream, byte-exact + trailing EOF | `TestMuxLargeTransfer` | PASS |
| 8 streams under 1 ms/direction injected latency | `TestMuxHighLatency` | PASS |
| 16 streams, per-stream pattern hash (isolation) | `TestMuxStreamIsolationUnderLoad` | PASS |
| Transport killed mid-transfer | `TestMuxTransportLossMidTransfer` | reader/writer unblock with errors; both sessions end; no hang |
| RESET aborting a blocked Read | `TestMuxResetAbortsBlockedRead` | Read returns the structured `ERR_DIAL` error promptly |
| Clean transport EOF with open streams | `TestMuxEOFInjection` | pending Read errors; session `Done` fires |
| Transport freeze then release | `TestMuxBlockedTransportKeepsQueuesBounded` | write backpressures while frozen (never buffers unbounded); drains fully after release |

Four product bugs surfaced by these runs, each fixed and re-injection-verified:

1. **`handleOpen` answered OPEN_OK before the accept handoff.** With more than 16 concurrent opens, overflowed streams received OPEN_OK and then an OPEN_ERR-plus-drop; the peer's in-flight DATA hit a closed registry entry (`DATA after FIN (state closed)`) and was RESET. The handoff now precedes the answer: overflow sends OPEN_ERR only, never a lying OK.
2. **`SessionBuffer` had no per-stream ledger.** Bytes delivered into a dead stream's receive queue were reserved forever — 63 × 16 KiB leaked the whole 1 MiB budget and wedged the reader in the `Reserve` loop (the streams=64 hang). Reserve/Release now track per stream, `dropStream` releases the remainder via `ReleaseStream`, and the dead-deliver path releases its frame.
3. **`Stream.Read` released the session budget only when a block's tail drained.** A 32 KiB block split across two 16 KiB Reads stranded half its reservation, every time — with 64 streams that alone exhausted the session budget. Every copied byte is now consumed at the moment it is copied; multiple small WINDOW_UPDATEs per block are protocol-legal. Re-injecting the old tail-only release makes `TestMuxStressStreamCounts/streams=64` fail deterministically.
4. **`Stream.Close` after our own FIN sent a RESET**, turning the peer's clean post-FIN drain into `ERR_SHUTDOWN` noise. Close now stays silent once our FIN is on the wire (`Registry.FinSent` dedicated flag — deliberately not derived from stream state, because a RESET-closed registry entry also lands in `StreamClosed`; deriving it from state regressed `TestMuxCloseFlushesWrittenData` into a hang before the flag was introduced).

Two test-harness defects found by `-race` were also fixed (a `t.Logf` after subtest end; a data race in the blocked-transport test double's token bucket). Neither is product code.

Evidence: `go test -race -count=3 ./internal/mux` PASS (zero DATA RACE reports); full repo `go test -race ./...` (11 packages) PASS; `go vet` clean.

## v0.4 status (not a gate claim yet)

- [x] PR-06A/06B protocol freeze + codec with golden vectors and fuzz targets
- [x] PR-06C registry + state machine
- [x] PR-06D flow control + fair scheduler
- [x] PR-06E session assembly (handshake, single reader/writer, accept/open)
- [x] PR-07 stress + fault injection with the four product closures above
- [x] Full CI matrix green on `53a23bd` follow-up `91fe42c` (run 40062048232); the first PR-07 run (40061733769) failed on the 32-stream sweep under runner load and produced the accept-backlog retry fix recorded above
- [ ] Gate decision: whether v0.4 closes here (library-complete mux) or also includes CLI wiring (explicit opt-in, no old-CLI breakage — a separate PR-08 decision)
- [ ] Validation report gate checklist finalized

The mux layer remains unwired into any CLI path: `proxy`, `connect`, and `socks-stdio` behave exactly as v0.3 left them. Wiring is a deliberate, separately-reviewed step.
