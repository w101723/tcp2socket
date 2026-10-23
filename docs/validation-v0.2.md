# Correctness-phase validation report

Date: 2026-10-22, updated 2026-10-23 after the first `dev` push. Scope: the first implementation batch of upgrade-spec PR-01–PR-03, on `dev`. This is not v1.0 acceptance and no release was published.

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
- 100MiB streaming SHA-256 round trip through a real proxy process and 32 concurrent isolated streams, each verified end to end.
- Proxy resource baseline: repeated round trips, SIGTERM stop, and post-stop goroutine sanity in the test binary.

The first end-to-end run correctly caught that closing inherited blocking stdin did not interrupt macOS reads. The adapter now duplicates/registers file descriptors with Go's poller before copying. Final full standard and race runs passed after that fix. The test process helper also uses completion broadcasts so repeated cleanup cannot consume a result twice and deadlock.

Writing the stress tests surfaced a wrong expectation worth recording: proxy mode is a long-running service, so it keeps accepting after all client streams close and must be stopped by signal — this matches legacy behavior, and the tests now stop it with SIGTERM and assert a bounded exit.

Resource sampling targets the proxy subprocess, not the test parent: `/proc/<pid>/fd` on Linux or `lsof -p` on macOS, with peak-after-traffic compared against the idle level. This run sampled the subprocess on macOS via `lsof` (6 descriptors idle, stable after traffic). The parent-process goroutine check from earlier revisions was removed as unrepresentative. CI run 39891982920 (2026-10-23) passed all four matrix cells: Ubuntu Go 1.22/stable and macOS 15 Go 1.22 / macOS latest Go stable; stress tests and the subprocess-resource test were not present in that run and have only been executed locally.

## Compatibility and resource impact

Two bounded copy tasks per active relay; a fixed 128-worker default, a 64-slot accepted queue and immediate overflow rejection. Idle tracking adds one timer only when enabled. STDIO owns its handles and prepares duplicated file descriptors; original nonblocking flags are restored on cleanup. No payload logging, third-party module, new listening management endpoint or persistent configuration is added.

CLI error categories remain the old 0/1/2 categories. Input validation is stricter for negative limits/timeouts and out-of-range Unix permission bits. The 0660 Unix mode and zero idle timeout defaults remain unchanged. See [compatibility.md](compatibility.md) for precise EOF/shutdown policy and rollback instructions.

## Security boundary and remaining acceptance work

No authentication bypass, firewall modification, PTY backend or network SOCKS listener is implemented. Socket parent-directory mutation by an attacker is outside the filesystem safety guarantee; use a private directory. Stream implementations and custom Dialers must honor close/cancel contracts. Logs contain metadata, not payload or credentials.

CI first ran on the pushed `dev` commit d9db1c2 (workflow run 39889259425): Ubuntu Go 1.22/stable and macOS stable passed; **macOS `macos-latest` (macOS 26) + Go 1.22.12 failed** at dynamic-loader startup (`dyld: missing LC_UUID load command`, abort trap) before executing any assertion, for the proxy/socks5/transport/integration test binaries. The same commit passed all four target matrix cells on Ubuntu and the Go-stable macOS runner, so no test failure implicates the relay or SOCKS logic. The matrix now pins the Go 1.22 macOS cell to `macos-15`; that cell has not yet re-run remotely.

New CI defines macOS/Linux Go 1.22 and stable runtime/race checks. Locally verified here: macOS arm64 runtime and test execution. Remotely observed as passing: Ubuntu Go 1.22/stable and macOS stable. Not yet verified: the re-pinned macOS 15 + Go 1.22 cell and linux/amd64 runtime; four-target cross-compilation is not runtime acceptance.

No 64-connection stress run, long-duration FD/goroutine soak, 24h/72h soak or hostile-directory TOCTOU proof is claimed. The subprocess FD sampling has not yet run on a Linux CI host. Session/Mux, doctor/version CLI, local IPC, standalone SOCKS, authorized SSH backend and experimental PTY remain future stages. No license is selected on the owner's behalf.
