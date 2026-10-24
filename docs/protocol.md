# T2S2 stream multiplexing protocol — version 1 (frozen)

Status: frozen for v0.4 implementation. Any byte-level change requires a
version bump plus updated golden vectors. This document is normative: two
independent implementations reading only this file must interoperate.

T2S2 is a **separate protocol** from the frozen T2S1 v1 control framing
(`docs/validation-v0.3.md`, PR-05A). T2S1 stays untouched; T2S2 is the
versioned binary mux the upgrade plan (§7.2) requires. There is no
compatibility with the experimental Python `#PBR1|...` text framing — that
belongs to a future PTY adapter only.

All integers are unsigned, network byte order (big-endian). All multi-byte
fields are exactly the width stated; no varints.

## 1. Security model (non-negotiable)

CRC32C detects accidental corruption only. It is **not** authentication and
provides **no confidentiality.** T2S2 must run only over a channel that
already provides authentication and encryption (SSH, TLS). The epoch field
rejects stale frames from a previous incarnation of the underlying
connection; it is not an anti-tamper mechanism. A listener must never accept
a T2S2 session from an untrusted network.

## 2. Frame layout

```
offset  size  field
0       4     magic "T2S2"
4       1     version (=1)
5       1     type
6       2     flags (=0)
8       2     reserved (=0)
10      4     length (payload bytes, 0..MaxFramePayload)
14      16    epoch
30      4     stream_id
34      4     sequence
38      ...   payload (length bytes)
38+len  4     crc32c over all preceding bytes of this frame
```

Total header size: **38 bytes.** Trailer: **4 bytes CRC32C (Castagnoli)**.
`length` counts payload bytes only; the CRC is not included in `length`.

Field rules:

- `magic` — the four ASCII bytes `T2S2` (0x54 0x32 0x53 0x32). Any other
  value is a framing error; the session is torn down (see §8).
- `version` — exactly `1`. Any other value: reject with `GOAWAY(protocol)`
  if possible, then close.
- `type` — one of the eleven types in §3. Unknown type on decode → framing
  error.
- `flags` — `0` for all v1 frame types. A nonzero value is a protocol error
  (reserved for future versions; rejecting now keeps the format closed).
- `reserved` — `0`; same rejection rule as flags.
- `length` — `0..MaxFramePayload` (§5). `length > MaxFramePayload` is a
  framing error and MUST be detected **before** any payload allocation.
- `epoch` — 16 random bytes (the T2S1 session ID of the establishing
  handshake reused verbatim is acceptable). Both directions use the same
  epoch. Every frame must carry the epoch negotiated at session start; a
  mismatched epoch is a stale frame and the session tears down.
- `stream_id` — `0` for session-scoped frames (HELLO, HELLO_ACK, GOAWAY,
  PING, PONG); the target stream otherwise. Odd IDs are client-initiated,
  even IDs are server-initiated (§6).
- `sequence` — per-stream, monotonically increasing by 1 for DATA and
  WINDOW_UPDATE frames; `0` otherwise. Out-of-order/duplicate sequences are
  protocol errors (§7.3).

## 3. Frame types

| Type | Value | stream_id | Payload |
| --- | --- | --- | --- |
| HELLO | 0x01 | 0 | capability list (§4) |
| HELLO_ACK | 0x02 | 0 | accepted capability list (§4) |
| OPEN | 0x03 | new (odd client / even server) | target descriptor (§4.2) |
| OPEN_OK | 0x04 | the opened stream | empty |
| OPEN_ERR | 0x05 | the opened stream | error code (1) + detail |
| DATA | 0x06 | the stream | raw bytes |
| WINDOW_UPDATE | 0x07 | the stream | granted delta, uint32 |
| FIN | 0x08 | the stream | empty |
| RESET | 0x09 | the stream | error code (1) + detail |
| PING | 0x0A | 0 | empty |
| PONG | 0x0B | 0 | empty |
| GOAWAY | 0x0C | 0 | error code (1) + detail |

The lowest set is exactly the plan's §7.1 requirement plus GOAWAY for
session-level shutdown. OPEN_OK/OPEN_ERR are explicit; an OPEN is never
answered by DATA.

## 4. Payload formats

### 4.1 Capabilities (HELLO / HELLO_ACK)

One byte per capability, ascending, no duplicates, no `0x00`. v1 defines:

- `0x01` — `stream.mux` (required; a peer not offering it cannot multiplex)
- `0x02` — `heartbeat` (optional; PING/PONG as in T2S1's semantics)

HELLO_ACK must echo exactly the accepted subset; requesting {0x01} and being
answered {0x01, 0x02} is a protocol error.

### 4.2 Target descriptor (OPEN payload)

```
network (1)  — 0x01 TCP, 0x02 Unix
length (1)   — address byte length, 1..200
address (length bytes)
```

v1 supports TCP (`host:port` ASCII) and Unix socket paths. Whether a target
is permitted is the **responder's** policy decision (server allow-list);
refusal must answer OPEN_ERR(ERR_TARGET_DENIED), never silence.

### 4.3 Error payloads (OPEN_ERR / RESET / GOAWAY)

```
code (1)
detail (rest of payload, UTF-8, human-readable, max 255 bytes)
```

Codes (plan §7.5 superset):

| Code | Name | Meaning |
| --- | --- | --- |
| 0x01 | ERR_DIAL | target unreachable |
| 0x02 | ERR_TARGET_DENIED | target not permitted by policy |
| 0x03 | ERR_HANDSHAKE | session handshake failure |
| 0x04 | ERR_VERSION | version mismatch |
| 0x05 | ERR_TIMEOUT | timeout |
| 0x06 | ERR_IDLE | idle timeout |
| 0x07 | ERR_FLOW_CONTROL | window violation |
| 0x08 | ERR_FRAME | framing error |
| 0x09 | ERR_TRANSPORT_LOST | underlying transport lost |
| 0x0A | ERR_AUTH_REQUIRED | re-authentication needed |
| 0x0B | ERR_SHUTDOWN | orderly shutdown |
| 0x0C | ERR_STREAM_STATE | stream state violation |

Detail text is peer-controlled: implementations must treat it as untrusted
display text, never log raw frames, never echo credentials.

## 5. Limits

| Limit | Value |
| --- | --- |
| MaxFramePayload (DATA/WINDOW_UPDATE et al.) | 32768 (32 KiB) |
| Per-stream receive buffer | 64 KiB (2 × MaxFramePayload) |
| Session aggregate receive buffer | 1 MiB |
| Initial per-stream window | 64 KiB |
| Max concurrent streams per session | 256 |
| MaxWindowDelta (single WINDOW_UPDATE) | 2^31-1 |
| Error detail length | 255 bytes |
| Target address length | 200 bytes |

A window violation (sender exceeds granted credit) is `ERR_FLOW_CONTROL`:
the violating stream is RESET; a second violation tears down the session.
Session aggregate overrun: apply backpressure first; only a stream that
keeps growing while its consumer is not reading is RESET with
`ERR_FLOW_CONTROL`.

## 6. Stream IDs

- Client-initiated streams use odd IDs `1, 3, 5, …`; server-initiated use
  even IDs `2, 4, …` (a server that will not initiate streams simply never
  sends OPEN).
- An OPEN with a stream ID of the wrong parity, ID 0, or an ID already in
  use is `ERR_STREAM_STATE`.
- IDs are not reused within an epoch. (v1 has no ID recycling.)
- DATA/WINDOW_UPDATE/FIN/RESET on an unknown or already-closed stream is a
  ghost frame: `ERR_STREAM_STATE` RESET for unknown DATA/FIN, session
  teardown if persistent (see §7.3).

## 7. State machines

### 7.1 Session

```
INIT → (send HELLO / receive HELLO_ACK) → READY
INIT → (receive HELLO / send HELLO_ACK) → READY
READY → (GOAWAY sent or received, or transport EOF/error) → CLOSING → CLOSED
```

Both directions must complete HELLO↔HELLO_ACK before any OPEN. READY is
bidirectional confirmation only — it is not a statement about peer identity;
identity is the enclosing channel's job (§1).

### 7.2 Stream

```
IDLE → OPEN → OPENING → OPEN_OK → OPEN
                  └→ OPEN_ERR → CLOSED
OPEN → FIN(local) → HALF_CLOSED_LOCAL
OPEN → FIN(remote) → HALF_CLOSED_REMOTE
HALF_CLOSED_LOCAL/FIN(remote) → CLOSED     (both FINs observed)
any → RESET / transport failure → CLOSED
```

- FIN is a one-direction EOF, exactly like a TCP half-close.
- Duplicate FIN or RESET on the same stream: `ERR_STREAM_STATE`, stream is
  already closed — ignore-and-log at most once, second offense tears down
  the session.
- DATA after the sender's FIN (late data): `ERR_STREAM_STATE` RESET.
- An OPEN for a stream the peer never acknowledged (no OPEN_OK/OPEN_ERR
  yet) may be retried once by the opener after a 5s quiet period, with a
  **fresh** stream ID; the stale ID is then ghost.

### 7.3 Sequence rules

DATA and WINDOW_UPDATE carry per-stream sequences starting at 1, +1 per
frame, independent counters per frame type. A duplicate or out-of-order
sequence is a protocol error: RESET the stream (`ERR_STREAM_STATE`). Frames
arriving for a CLOSED stream (within a grace window of the closing event)
may be silently dropped; anything after the grace window is ghost (§6).

## 8. Error handling and teardown

Framing errors (bad magic, bad version, nonzero flags/reserved, oversized
length, CRC mismatch) are fatal to the session: attempt a best-effort
`GOAWAY(ERR_FRAME)` and close the transport. All open streams observe
`ERR_TRANSPORT_LOST` (or `ERR_FRAME`) — never a silent hang. Stream-level
errors RESET that stream only; the session continues. No frame is ever
retransmitted at this layer; loss recovery belongs to the enclosing
transport (SSH/TLS/TCP).

## 9. Scheduling and backpressure (implementation contract)

- One writer goroutine per session direction (the proven T2S1
  single-writer model); producers enqueue into bounded per-stream queues.
- Round-robin across streams with a per-turn byte budget; session-scoped
  control frames (GOAWAY, PING/PONG) take priority over DATA.
- Writers must not hold the stream-table lock while writing to the
  transport.
- A stalled consumer's credit is simply not replenished: WINDOW_UPDATE is
  sent only as the application reads. Nothing is buffered beyond §5 limits.

## 10. Golden vectors

Implementations must reproduce the exact bytes in `internal/mux/golden_test.go`
(TestGoldenVectors), generated from this document alone:

1. HELLO with capabilities {stream.mux}, epoch
   `000102…0f`, sequence 0 — 43 bytes total (38 header + 1 payload + 4 CRC).
2. DATA stream 3, sequence 7, payload `t2s2 golden` — 53 bytes total (38 + 11 + 4).
3. FIN stream 3, sequence 0 — 42 bytes total (38 + 0 + 4).
4. GOAWAY code ERR_FRAME, detail "bad crc" — 50 bytes total (38 + 8 + 4).

Byte-level mismatches against these vectors are interoperability failures.
