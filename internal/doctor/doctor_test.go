package doctor

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"tcp2socket/internal/logging"
)

func captureReport(t *testing.T, opts Options) (int, string) {
	t.Helper()
	var buf bytes.Buffer
	opts.Out = &buf
	code := Run(opts)
	return code, buf.String()
}

func requireFields(t *testing.T, out string, substrings ...string) {
	t.Helper()
	for _, s := range substrings {
		if !strings.Contains(out, s) {
			t.Fatalf("report missing %q:\n%s", s, out)
		}
	}
}

// TestDoctorDefaultChecksPass runs the real local checks: on a healthy
// machine everything must be green (build_info may WARN on dev metadata) and
// the exit code is 0.
func TestDoctorDefaultChecksPass(t *testing.T) {
	code, out := captureReport(t, Options{})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
	requireFields(t, out,
		`check=build_info status=`,
		`check=loopback_tcp status=PASS`,
		`check=unix_socket status=`,
		`check=fd_limit status=`,
		"event=doctor_summary",
		"result=ok",
	)
	if strings.Contains(out, "check=loopback_tcp status=FAIL") {
		t.Fatalf("loopback self-test failed on a healthy host:\n%s", out)
	}
}

// TestDoctorFailingCheckExitCode: a FAIL flips the exit code; WARN and
// UNAVAILABLE never do.
func TestDoctorFailingCheckExitCode(t *testing.T) {
	failCheck := func(ctx context.Context) Report { return Report{Status: Fail, Detail: "injected"} }
	code, out := captureReport(t, Options{ExtraChecks: []Check{failCheck}})
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	requireFields(t, out, "check=extra status=FAIL", "failed=1", "result=failed")

	warnCheck := func(ctx context.Context) Report { return Report{Status: Warn, Detail: "injected"} }
	code, _ = captureReport(t, Options{ExtraChecks: []Check{warnCheck}})
	if code != 0 {
		t.Fatalf("WARN must not fail the doctor, exit = %d", code)
	}
	unavailCheck := func(ctx context.Context) Report { return Report{Status: Unavailable, Detail: "injected"} }
	code, _ = captureReport(t, Options{ExtraChecks: []Check{unavailCheck}})
	if code != 0 {
		t.Fatalf("UNAVAILABLE must not fail the doctor, exit = %d", code)
	}
}

// TestRunCheckValidatesStatus: a check returning no status becomes FAIL, and
// the assigned check name always lands in the report.
func TestRunCheckValidatesStatus(t *testing.T) {
	r := runCheck(context.Background(), time.Second, "named", func(ctx context.Context) Report {
		return Report{Detail: "no status"}
	})
	if r.Check != "named" || r.Status != Fail {
		t.Fatalf("report = %+v, want named/FAIL", r)
	}
}

// TestLoopbackCheckBoundedByDeadline: an expired context must produce a
// bounded FAIL, never a hang.
func TestLoopbackCheckBoundedByDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	done := make(chan Report, 1)
	go func() { done <- checkLoopbackTCP(ctx) }()
	select {
	case r := <-done:
		if r.Status != Fail {
			t.Fatalf("status = %s, want FAIL on expired deadline", r.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loopback check did not respect its deadline")
	}
}

// TestLoopbackSourceAnomaly is the dubhe-vpn regression: a non-loopback
// source on a 127.0.0.1 connection must be classified as a NAT rewrite, with
// guidance that stays read-only.
func TestLoopbackSourceAnomaly(t *testing.T) {
	if err := loopbackSourceAnomaly(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 51000}); err != nil {
		t.Fatalf("loopback source flagged: %v", err)
	}
	err := loopbackSourceAnomaly(&net.TCPAddr{IP: net.IPv4(192, 168, 99, 198), Port: 51000})
	if err == nil {
		t.Fatal("rewritten loopback source not detected")
	}
	msg := err.Error()
	for _, want := range []string{"rewritten", "MASQUERADE", "does not modify"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("anomaly message missing %q: %s", want, msg)
		}
	}
	if err := loopbackSourceAnomaly(&net.IPAddr{IP: net.IPv4(127, 0, 0, 1)}); err == nil {
		t.Fatal("unexpected address type must be reported")
	}
}

// TestCheckFDLimitStatus: the fd check may PASS, WARN on a low soft limit or
// be UNAVAILABLE off-platform — never FAIL a healthy machine.
func TestCheckFDLimitStatus(t *testing.T) {
	r := checkFDLimit(context.Background())
	switch r.Status {
	case Pass, Warn, Unavailable:
	default:
		t.Fatalf("status = %s, want PASS/WARN/UNAVAILABLE (detail: %s)", r.Status, r.Detail)
	}
}

// TestMasqueradeFindings freezes the hint heuristic on a sample dump.
func TestMasqueradeFindings(t *testing.T) {
	sample := `-A POSTROUTING -j MASQUERADE
-A POSTROUTING -o eth0 -j MASQUERADE
-A POSTROUTING -s 10.0.0.0/8 -j MASQUERADE
-A OUTPUT -d 127.0.0.1 -j RETURN
COMMIT
`
	uncond, scoped := masqueradeFindings(sample)
	if len(uncond) != 1 || !strings.Contains(uncond[0], "-A POSTROUTING -j MASQUERADE") {
		t.Fatalf("unconditional = %v, want the bare POSTROUTING rule", uncond)
	}
	if scoped != 2 {
		t.Fatalf("scoped = %d, want 2", scoped)
	}
	if u, s := masqueradeFindings("*nat\nCOMMIT\n"); u != nil || s != 0 {
		t.Fatalf("empty table: %v %d, want none", u, s)
	}
	// --jump long form and --out-interface long form must parse too.
	u, _ := masqueradeFindings("-A POSTROUTING --out-interface eth1 --jump MASQUERADE\n")
	if len(u) != 0 {
		t.Fatalf("scoped long-form rule flagged unconditional: %v", u)
	}
}

// TestDoctorConcurrentReport: event lines never interleave.
func TestDoctorConcurrentReport(t *testing.T) {
	var buf bytes.Buffer
	log := logging.New(&buf)
	var sink Report
	_ = sink
	opts := Options{Log: log, ExtraChecks: []Check{
		func(ctx context.Context) Report { return Report{Status: Pass, Detail: strings.Repeat("x", 200)} },
	}}
	if code := Run(opts); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for i, line := range strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n") {
		if !strings.HasPrefix(line, "event=doctor_check ") && !strings.HasPrefix(line, "event=doctor_summary ") {
			t.Fatalf("line %d malformed: %q", i, line)
		}
	}
}
