# Architecture: correctness phase

The root main is an exit-code adapter to `internal/cli`. No new wire protocol or persistent configuration is introduced.

```text
cli ── endpoint
 ├── proxy listener ── DirectDialer ── TCP / Unix
 │       └── Relay
 ├── connect ── Stdio + DirectDialer ── Relay
 └── socks-stdio ── socks5 protocol ── DirectDialer ── Relay
```

`transport.Stream` is an owned read/write/close stream. Its Close must unblock pending I/O, must be concurrency-safe and must permit repeated calls. Optional half-close and deadline interfaces are separate: STDIO does not provide fake deadline methods. The current STDIO implementation is intended for owned OS stdin/stdout pipes; arbitrary caller-provided blocking readers must satisfy the same cancellation contract.

`proxy.Relay` has two bounded copy tasks and one coordinator. EOF propagates CloseWrite when available. Fatal I/O errors, context cancellation and optional bidirectional idle timeout close both streams and join both tasks. Socket proxies retain both directions until completion; STDIO explicitly ends when the target sends EOF. Every completed relay returns byte counts, elapsed time, reason and error. The idle timestamp uses monotonic elapsed time.

`proxy.ListenAndServe` has a fixed worker pool, a bounded accepted-connection queue, and a serialized event callback. Worker count bounds both target dials and live relays. ServerConfig requires explicit limits, Dialer and UnixMode; zero UnixMode intentionally means 0000 (matching the explicit CLI permission value), not an implicit default. Applications embedding it must provide nonblocking/fast event callbacks and Dialers that honor context cancellation. Listener shutdown stops acceptance; active connections drain for the configured period, then their work context is canceled. Queued connections are closed without dialing once shutdown is observed.

`socks5` owns method negotiation, address parsing and reply mapping, accepts a Dialer and delegates forwarding to Relay. Cancellation begins before greeting reads, not only after dialing. Its parser fuzz targets operate on in-memory input and never open network destinations.

Session, mux, doctor, IPC, authorized SSH subprocess transport and experimental PTY are deliberately absent from this phase. No speculative empty packages or partial Session API are advertised. Future implementations must first freeze their protocol and trust model.
