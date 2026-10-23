// Package doctor implements the v0.3 read-only self-check CLI (plan §5).
//
// It runs bounded local checks and reports each as PASS/WARN/FAIL/UNAVAILABLE.
// It never binds a persistent management port, never modifies firewall or
// container networking, and never requires privileges: the loopback TCP probe
// exists precisely so a plain unprivileged run can surface NAT anomalies like
// the dubhe-vpn case (loopback SYN sources rewritten by unconditional
// MASQUERADE). Diagnosis only — suggested fixes are printed, never executed.
package doctor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"tcp2socket/internal/buildinfo"
	"tcp2socket/internal/logging"
)

type Status string

const (
	Pass        Status = "PASS"
	Warn        Status = "WARN"
	Fail        Status = "FAIL"
	Unavailable Status = "UNAVAILABLE"
)

type Report struct {
	Check   string
	Status  Status
	Detail  string
	Elapsed time.Duration
}

type Check func(ctx context.Context) Report

type Options struct {
	// Out receives the report lines (the CLI passes stdout; doctor is not a
	// STDIO data mode, so stdout is the primary output).
	Out io.Writer
	// Timeout bounds each check individually. Default 3s, hard cap 30s.
	Timeout time.Duration
	// InspectNAT enables the opt-in read-only NAT hint (Linux only).
	InspectNAT bool
	// ExtraChecks appends checks after the defaults; test hook.
	ExtraChecks []Check
	// Log overrides the event sink; defaults to a logging.Logger on Out.
	Log *logging.Logger
}

const (
	defaultCheckTimeout = 3 * time.Second
	maxCheckTimeout     = 30 * time.Second
)

// Run executes all enabled checks and returns the process exit code:
// 0 when no check FAILed (WARN and UNAVAILABLE do not fail), 1 otherwise.
func Run(opts Options) int {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultCheckTimeout
	}
	if timeout > maxCheckTimeout {
		timeout = maxCheckTimeout
	}
	log := opts.Log
	if log == nil {
		log = logging.New(opts.Out)
	}

	type named struct {
		name string
		fn   Check
	}
	checks := []named{
		{"build_info", checkBuildInfo},
		{"loopback_tcp", checkLoopbackTCP},
		{"unix_socket", checkUnixSocket},
		{"fd_limit", checkFDLimit},
	}
	if opts.InspectNAT {
		checks = append(checks, named{"nat_inspect", checkNAT})
	}

	var failed, warned, unavailable int
	emit := func(r Report) {
		log.Event("doctor_check",
			"check", r.Check,
			"status", string(r.Status),
			"detail", r.Detail,
			"elapsed_ms", logging.FormatInt(r.Elapsed.Milliseconds()))
		switch r.Status {
		case Fail:
			failed++
		case Warn:
			warned++
		case Unavailable:
			unavailable++
		}
	}
	for _, c := range checks {
		emit(runCheck(context.Background(), timeout, c.name, c.fn))
	}
	for _, fn := range opts.ExtraChecks {
		emit(runCheck(context.Background(), timeout, "extra", fn))
	}

	total := len(checks) + len(opts.ExtraChecks)
	result := "ok"
	if failed > 0 {
		result = "failed"
	}
	log.Event("doctor_summary",
		"checks", strconv.Itoa(total),
		"failed", strconv.Itoa(failed),
		"warned", strconv.Itoa(warned),
		"unavailable", strconv.Itoa(unavailable),
		"result", result)
	if failed > 0 {
		return 1
	}
	return 0
}

func runCheck(parent context.Context, timeout time.Duration, name string, fn Check) Report {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	start := time.Now()
	r := fn(ctx)
	r.Elapsed = time.Since(start)
	r.Check = name
	switch r.Status {
	case Pass, Warn, Fail, Unavailable:
	default:
		r.Status = Fail
		if r.Detail == "" {
			r.Detail = "check returned no status"
		}
	}
	return r
}

func checkBuildInfo(ctx context.Context) Report {
	detail := fmt.Sprintf("version=%s commit=%s go=%s os/arch=%s",
		buildinfo.Version, buildinfo.Commit, runtime.Version(), runtime.GOOS+"/"+runtime.GOARCH)
	if buildinfo.Version == "dev" || buildinfo.Commit == "unknown" {
		return Report{Status: Warn, Detail: detail + " (unreleased build metadata; set via -ldflags)"}
	}
	return Report{Status: Pass, Detail: detail}
}

// checkLoopbackTCP is the dubhe-vpn regression probe. It listens on an
// ephemeral 127.0.0.1 port, echoes a marker through it and verifies the
// accepted connection's source is still loopback. When a NAT rule rewrites
// loopback sources, the accepted address betrays it (e.g. 192.168.99.198
// instead of 127.0.0.1) — visible here without tcpdump or privileges.
func checkLoopbackTCP(ctx context.Context) Report {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultCheckTimeout)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("listen 127.0.0.1:0: %v", err)}
	}
	defer ln.Close()

	type accepted struct {
		remote net.Addr
		err    error
	}
	serverDone := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverDone <- accepted{err: err}
			return
		}
		_ = c.SetDeadline(deadline)
		buf := make([]byte, len(marker))
		_, rerr := io.ReadFull(c, buf)
		if rerr == nil {
			_, rerr = c.Write(buf)
		}
		remote := c.RemoteAddr()
		_ = c.Close()
		serverDone <- accepted{remote: remote, err: rerr}
	}()

	d, err := net.DialTimeout("tcp", ln.Addr().String(), time.Until(deadline))
	if err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("dial %s: %v", ln.Addr(), err)}
	}
	defer d.Close()
	_ = d.SetDeadline(deadline)
	start := time.Now()
	if _, err := d.Write(marker); err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("write marker: %v", err)}
	}
	got := make([]byte, len(marker))
	if _, err := io.ReadFull(d, got); err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("read echo: %v", err)}
	}
	rtt := time.Since(start)
	if !bytes.Equal(got, marker) {
		return Report{Status: Fail, Detail: "echo mismatch: loopback payload corrupted"}
	}

	res := <-serverDone
	base := fmt.Sprintf("listen=%s echo=%d bytes rtt=%s", ln.Addr(), len(marker), rtt)
	if res.err != nil {
		return Report{Status: Fail, Detail: base + "; server side: " + res.err.Error()}
	}
	if err := loopbackSourceAnomaly(res.remote); err != nil {
		return Report{Status: Fail, Detail: base + "; " + err.Error()}
	}
	return Report{Status: Pass, Detail: base + "; source=" + res.remote.String()}
}

var marker = []byte("t2s-doctor-loopback-probe")

// loopbackSourceAnomaly classifies the accepted address of a connection
// dialed to 127.0.0.1. Any non-loopback source is the dubhe-vpn signature of
// an unconditional SNAT/MASQUERADE rule touching loopback traffic.
func loopbackSourceAnomaly(addr net.Addr) error {
	ra, ok := addr.(*net.TCPAddr)
	if !ok || ra.IP == nil {
		return fmt.Errorf("unexpected remote address type %T", addr)
	}
	if !ra.IP.IsLoopback() {
		return fmt.Errorf("loopback connection source rewritten to %s — unconditional SNAT/MASQUERADE suspected; add a loopback exception in NAT; doctor does not modify NAT rules", ra.IP)
	}
	return nil
}

// checkUnixSocket exercises the same unix-socket path the proxy uses, inside
// a fresh temporary directory the current user owns, and verifies cleanup.
func checkUnixSocket(ctx context.Context) Report {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultCheckTimeout)
	}
	dir, err := os.MkdirTemp("", "t2s-doctor-")
	if err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("create temp dir: %v", err)}
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "doctor.sock")

	ln, err := net.Listen("unix", path)
	if err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("listen unix %s: %v", path, err)}
	}
	defer ln.Close()

	status := Pass
	modeDetail := ""
	if err := os.Chmod(path, 0o600); err != nil {
		status = Warn
		modeDetail = fmt.Sprintf("; chmod 0600 failed: %v", err)
	} else {
		modeDetail = "; mode=0600"
	}

	type accepted struct{ err error }
	serverDone := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			serverDone <- accepted{err: err}
			return
		}
		_ = c.SetDeadline(deadline)
		buf := make([]byte, len(marker))
		_, rerr := io.ReadFull(c, buf)
		if rerr == nil {
			_, rerr = c.Write(buf)
		}
		_ = c.Close()
		serverDone <- accepted{err: rerr}
	}()

	d, err := net.DialTimeout("unix", path, time.Until(deadline))
	if err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("dial unix %s: %v", path, err)}
	}
	defer d.Close()
	_ = d.SetDeadline(deadline)
	if _, err := d.Write(marker); err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("write marker: %v", err)}
	}
	got := make([]byte, len(marker))
	if _, err := io.ReadFull(d, got); err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("read echo: %v", err)}
	}
	if !bytes.Equal(got, marker) {
		return Report{Status: Fail, Detail: "echo mismatch: unix socket payload corrupted"}
	}
	if res := <-serverDone; res.err != nil {
		return Report{Status: Fail, Detail: "server side: " + res.err.Error()}
	}

	if err := ln.Close(); err != nil {
		return Report{Status: Fail, Detail: fmt.Sprintf("close listener: %v", err)}
	}
	cleanup := "socket file removed on close"
	if _, err := os.Stat(path); err == nil {
		if status == Pass {
			status = Warn
		}
		cleanup = "socket file survived listener close"
	}
	return Report{Status: status, Detail: fmt.Sprintf("path=%s echo=%d bytes; %s%s", path, len(marker), cleanup, modeDetail)}
}
