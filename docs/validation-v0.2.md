# Correctness-phase validation report

Date: 2026-10-22. Scope: the first implementation batch of upgrade-spec PR-01–PR-03, on `dev`. This is not v1.0 acceptance and no release was published.

## Local evidence

Host: macOS / arm64 (Apple M5), Go 1.26.1.

| Check | Result |
| --- | --- |
| `go test -count=1 -timeout 45s ./...` | PASS |
| `TCP2SOCKET_TEST_RACE=1 go test -race -count=1 -timeout 180s ./...` | PASS; integration child binary also race-instrumented |
| `go vet ./...` and `git diff --check` | PASS |
| Proxy + integration tests, `-count=5` | PASS |
| SOCKS5 + transport + proxy, `-race -count=10` | PASS |
| Request parser fuzz, 5s / 2 workers | PASS, 698,591 executions |
| Greeting parser fuzz, 5s / 2 workers | PASS, 744,041 executions |
| Endpoint parser fuzz, 5s / 2 workers | PASS, 490,905 executions |
| CGO-disabled darwin/{amd64,arm64}, linux/{amd64,arm64} builds | PASS |

Package coverage from local unit tests before final extra half-close tests: endpoint 100.0%, proxy 88.5%, SOCKS5 74.3%, transport 73.3%, CLI 59.7%. Black-box tests separately exercise the built executable and are not included in these package percentages. These numbers are not directly comparable with the old monolithic repository's 20.9% total.

The benchmark `go test ./internal/proxy -run '^$' -bench '^BenchmarkRelay$' -benchtime 1s -benchmem` measured 1MiB local TCP relay iterations including connection setup: 772,262 ns/op, 1,357.80 MB/s, 70,561 B/op, 84 allocs/op. This is a post-change local observation, not a before/after performance claim or production throughput guarantee.

## Regression and fault scenarios

- Legacy help/exit codes, flags, explicit `connect`, TCP URI/host:port/Unix shorthand, all four TCP/Unix proxy combinations, stdout byte purity.
- SOCKS5 greeting, IPv4/IPv6/domain parsing, malformed/truncated packets, refused dials, cancel during greeting, parser fuzzing.
- Real inherited process pipes: SOCKS handshake and payload, target EOF with stdin kept open in both STDIO modes, SIGTERM during handshake.
- Delayed 500ms response after client input EOF is retained, replacing the old cutoff behavior.
- Socket-only reverse half-close preserves subsequent upload; deadline failure, blocked write cancellation, repeated cancel and joined tasks.
- Active-connection SIGTERM with 100ms drain deadline, capacity overflow and queued-work disposal.
- Active Unix listener protection, stale listener cleanup, ordinary file/symlink rejection, configured permissions and cleanup preserving a replaced path.
- 1MiB streaming SHA-256 verification and eight concurrent isolated transfers.

The first end-to-end run correctly caught that closing inherited blocking stdin did not interrupt macOS reads. The adapter now duplicates/registers file descriptors with Go's poller before copying. Final full standard and race runs passed after that fix. The test process helper also uses completion broadcasts so repeated cleanup cannot consume a result twice and deadlock.

## Compatibility and resource impact

Two bounded copy tasks per active relay; a fixed 128-worker default, a 64-slot accepted queue and immediate overflow rejection. Idle tracking adds one timer only when enabled. STDIO owns its handles and prepares duplicated file descriptors; original nonblocking flags are restored on cleanup. No payload logging, third-party module, new listening management endpoint or persistent configuration is added.

CLI error categories remain the old 0/1/2 categories. Input validation is stricter for negative limits/timeouts and out-of-range Unix permission bits. The 0660 Unix mode and zero idle timeout defaults remain unchanged. See [compatibility.md](compatibility.md) for precise EOF/shutdown policy and rollback instructions.

## Security boundary and remaining acceptance work

No authentication bypass, firewall modification, PTY backend or network SOCKS listener is implemented. Socket parent-directory mutation by an attacker is outside the filesystem safety guarantee; use a private directory. Stream implementations and custom Dialers must honor close/cancel contracts. Logs contain metadata, not payload or credentials.

New CI defines macOS/Linux Go 1.22 and stable runtime/race checks, but it has not run remotely in this work. Linux runtime, amd64 runtime and Go 1.22 execution are therefore **not locally verified**. Four-target cross-compilation is not runtime acceptance.

No 100MiB/32-/64-connection stress run, goroutine/FD soak baseline, 24h/72h soak or hostile-directory TOCTOU proof is claimed. Session/Mux, doctor/version CLI, local IPC, standalone SOCKS, authorized SSH backend and experimental PTY remain future stages. No license is selected on the owner's behalf. Changes remain uncommitted and unpublished.
