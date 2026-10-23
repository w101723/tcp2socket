package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"tcp2socket/internal/doctor"
)

// runDoctorCLI implements `tcp2socket doctor` (plan §5): bounded, read-only
// local self-checks reported as PASS/WARN/FAIL/UNAVAILABLE. It never binds a
// persistent management port and never modifies firewall or container
// networking. Exit code 1 means at least one check FAILed; WARN and
// UNAVAILABLE keep exit 0 so scripts can treat them as informational.
func runDoctorCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		inspectNAT bool
		timeout    time.Duration
	)
	fs.BoolVar(&inspectNAT, "inspect-nat", false, "read-only NAT MASQUERADE hint (Linux only); prints suggestions, never modifies rules")
	fs.DurationVar(&timeout, "timeout", 3*time.Second, "per-check timeout budget")

	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage:
  tcp2socket doctor [options]

Read-only local self-checks, each reported as PASS/WARN/FAIL/UNAVAILABLE:
  build_info    version/commit/go/os-arch (WARN on dev/unknown metadata)
  loopback_tcp  127.0.0.1 echo probe; detects rewritten loopback sources
                (unconditional SNAT/MASQUERADE, e.g. the dubhe-vpn case)
  unix_socket   temporary user-owned unix socket listen/dial/echo/cleanup
  fd_limit      file descriptor soft/hard limits where the platform exposes them

Options:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return 2
	}
	if timeout <= 0 {
		fmt.Fprintf(stderr, "invalid --timeout %v: must be positive\n", timeout)
		return 1
	}

	return doctor.Run(doctor.Options{
		Out:        stdout,
		Timeout:    timeout,
		InspectNAT: inspectNAT,
	})
}
