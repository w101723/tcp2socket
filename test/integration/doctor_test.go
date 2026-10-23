package integration

import (
	"runtime"
	"strings"
	"testing"
)

// TestVersionSubcommand verifies `tcp2socket version` (plan §5): the four
// documented fields, one key=value per line, exit 0.
func TestVersionSubcommand(t *testing.T) {
	cmd := boundedCommand(t, "version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("version failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 4 {
		t.Fatalf("version printed %d lines, want 4: %q", len(lines), out)
	}
	for _, prefix := range []string{"version=", "commit=", "go=", "os/arch="} {
		if !strings.HasPrefix(lines[0], prefix) && !containsPrefix(lines, prefix) {
			t.Fatalf("version output missing %q: %q", prefix, out)
		}
	}
}

func containsPrefix(lines []string, prefix string) bool {
	for _, l := range lines {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

// TestProxyVersionFlag: the additive -version flag prints the same block.
func TestProxyVersionFlag(t *testing.T) {
	cmd := boundedCommand(t, "--version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("--version failed: %v", err)
	}
	if !strings.Contains(string(out), "version=") || !strings.Contains(string(out), "os/arch=") {
		t.Fatalf("--version output unexpected: %q", out)
	}
}

// TestDoctorSubcommand runs the read-only self-check end to end. On a healthy
// host it must exit 0 and report every default check plus the summary; WARN
// (dev build metadata) and UNAVAILABLE (platform-gated fd reporting) are
// allowed and must not fail the run.
func TestDoctorSubcommand(t *testing.T) {
	cmd := boundedCommand(t, "doctor")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("doctor exited with error: %v\n%s", err, out)
	}
	report := string(out)
	for _, want := range []string{
		"check=build_info status=",
		"check=loopback_tcp status=PASS",
		"check=unix_socket status=",
		"check=fd_limit status=",
		"event=doctor_summary",
		"result=ok",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("doctor report missing %q:\n%s", want, report)
		}
	}
}

// TestDoctorInspectNATPlatformGate: the opt-in NAT hint is UNAVAILABLE off
// Linux and must never change the exit code by itself.
func TestDoctorInspectNATPlatformGate(t *testing.T) {
	cmd := boundedCommand(t, "doctor", "--inspect-nat")
	out, err := cmd.Output()
	report := string(out)
	if runtime.GOOS != "linux" {
		if err != nil {
			t.Fatalf("doctor --inspect-nat must stay exit 0 off Linux: %v\n%s", err, report)
		}
		if !strings.Contains(report, "check=nat_inspect status=UNAVAILABLE") {
			t.Fatalf("expected UNAVAILABLE nat_inspect off Linux:\n%s", report)
		}
		return
	}
	// On Linux the check runs for real; only the line must be present.
	if !strings.Contains(report, "check=nat_inspect status=") {
		t.Fatalf("nat_inspect line missing:\n%s", report)
	}
}

// TestDoctorHelpFlags: both new subcommands support -h with exit 0, matching
// the legacy flag-error convention. Help goes to stderr like every other mode.
func TestDoctorHelpFlags(t *testing.T) {
	for _, args := range [][]string{{"doctor", "-h"}, {"version", "-h"}} {
		cmd := boundedCommand(t, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v failed: %v\n%s", args, err, out)
		}
		if len(out) == 0 {
			t.Fatalf("%v printed no help", args)
		}
	}
}
