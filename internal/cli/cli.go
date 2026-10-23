package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"tcp2socket/internal/endpoint"
	"tcp2socket/internal/proxy"
	"tcp2socket/internal/socks5"
	"tcp2socket/internal/transport"
)

const (
	defaultDialTimeout     = 10 * time.Second
	defaultIdleTimeout     = 0 * time.Second
	defaultShutdownTimeout = 10 * time.Second
	defaultMaxConnections  = 128
	defaultMaxPending      = 64
	defaultUnixMode        = "0660"
)

func Run(args []string, stdin io.ReadCloser, stdout io.WriteCloser, stderr io.Writer) int {
	if len(args) == 0 {
		return runProxyCLI([]string{}, stderr)
	}

	sub := args[0]
	switch sub {
	case "socks-stdio":
		return runSocksStdioCLI(args[1:], stdin, stdout, stderr)
	case "connect":
		return runConnectCLI(args[1:], stdin, stdout, stderr)
	default:
		if !strings.HasPrefix(sub, "-") {
			return runConnectCLI(args, stdin, stdout, stderr)
		}
		return runProxyCLI(args, stderr)
	}
}

func printProxyHelp(out io.Writer, fs *flag.FlagSet) {
	fmt.Fprintf(out, `tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy

Proxy mode:
  tcp2socket [options] -l <listen> -t <target>

Connect mode:
  tcp2socket connect [options] <target>

SOCKS5 over STDIO mode:
  tcp2socket socks-stdio [options]

nc-compatible shorthand:
  tcp2socket <target>

Endpoint formats:
  tcp://0.0.0.0:8080
  tcp://127.0.0.1:1080
  unix:///tmp/proxy.sock

Shorthand formats:
  :8080
  127.0.0.1:1080
  /tmp/proxy.sock

Examples:
  TCP -> Unix:
    tcp2socket -l tcp://0.0.0.0:8080 -t unix:///tmp/backend.sock

  TCP -> TCP:
    tcp2socket -l :8080 -t 192.168.1.10:1080

  Unix -> TCP:
    tcp2socket -l /tmp/local.sock -t 192.168.1.10:1080

  Unix -> Unix:
    tcp2socket -l /tmp/a.sock -t /tmp/b.sock

  STDIO -> TCP:
    tcp2socket connect tcp://127.0.0.1:8999

  STDIO -> Unix:
    tcp2socket connect unix:///tmp/app.sock

  nc-compatible shorthand:
    tcp2socket 127.0.0.1:8999

Options:
`)
	fs.SetOutput(out)
	fs.PrintDefaults()
}

func runProxyCLI(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("tcp2socket", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		listenStr  string
		targetStr  string
		dialTO     time.Duration
		idleTO     time.Duration
		shutdownTO time.Duration
		maxConns   int
		maxPending int
		unixMode   string
		verbose    bool
	)

	fs.StringVar(&listenStr, "listen", "", "listen endpoint: tcp://0.0.0.0:8080 or unix:///tmp/in.sock")
	fs.StringVar(&listenStr, "l", "", "short form of --listen")
	fs.StringVar(&targetStr, "target", "", "target endpoint: tcp://127.0.0.1:1080 or unix:///tmp/out.sock")
	fs.StringVar(&targetStr, "t", "", "short form of --target")
	fs.DurationVar(&dialTO, "dial-timeout", defaultDialTimeout, "target dial timeout")
	fs.DurationVar(&idleTO, "idle-timeout", defaultIdleTimeout, "connection idle timeout; 0 disables it")
	fs.DurationVar(&shutdownTO, "shutdown-timeout", defaultShutdownTimeout, "graceful shutdown timeout")
	fs.IntVar(&maxConns, "max-connections", defaultMaxConnections, "maximum active connections")
	fs.IntVar(&maxPending, "max-pending-accept", defaultMaxPending, "maximum pending accept queue size")
	fs.StringVar(&unixMode, "unix-mode", defaultUnixMode, "permission mode for listened unix socket")
	fs.BoolVar(&verbose, "v", false, "verbose connection logs")

	fs.Usage = func() {
		printProxyHelp(stderr, fs)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if listenStr == "" || targetStr == "" {
		fs.Usage()
		return 2
	}

	if dialTO < 0 || idleTO < 0 || shutdownTO < 0 || maxConns <= 0 || maxPending < 0 {
		fmt.Fprintf(stderr, "invalid resource limits or timeout values\n")
		return 1
	}

	listenEP, err := endpoint.Parse(listenStr)
	if err != nil {
		fmt.Fprintf(stderr, "invalid listen endpoint: %v\n", err)
		return 1
	}

	targetEP, err := endpoint.Parse(targetStr)
	if err != nil {
		fmt.Fprintf(stderr, "invalid target endpoint: %v\n", err)
		return 1
	}

	mode64, err := strconv.ParseUint(unixMode, 8, 32)
	if err != nil || mode64 > 0777 {
		fmt.Fprintf(stderr, "invalid --unix-mode %q: mode must be valid octal <= 0777\n", unixMode)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialer := &transport.DirectDialer{Timeout: dialTO}

	cfg := proxy.ServerConfig{
		ListenNetwork:    listenEP.Network,
		ListenAddress:    listenEP.Address,
		Target:           transport.Target{Network: targetEP.Network, Address: targetEP.Address},
		Dialer:           dialer,
		UnixMode:         os.FileMode(mode64),
		MaxConnections:   maxConns,
		MaxPendingAccept: maxPending,
		ShutdownTimeout:  shutdownTO,
		IdleTimeout:      idleTO,
		OnEvent: func(e proxy.Event) {
			switch e.Event {
			case "listening":
				fmt.Fprintf(stderr, "listening on %s -> %s\n", listenEP.Raw, targetEP.Raw)
			case "closed":
				if e.Reason == "dial_error" {
					fmt.Fprintf(stderr, "dial %s failed: %v\n", targetEP.Raw, e.Result.Err)
				}
				if verbose {
					fmt.Fprintf(stderr, "event=closed connection_id=%d source=%s reason=%s s2t=%d t2s=%d elapsed_ms=%d\n",
						e.ID, e.Source, e.Reason, e.Result.SourceToTarget, e.Result.TargetToSource, e.Result.Duration.Milliseconds())
				}
			case "rejected":
				if verbose {
					fmt.Fprintf(stderr, "event=rejected connection_id=%d source=%s reason=%s\n",
						e.ID, e.Source, e.Reason)
				}
			}
		},
	}

	if err := proxy.ListenAndServe(ctx, cfg); err != nil {
		fmt.Fprintf(stderr, "proxy error: %v\n", err)
		return 1
	}

	return 0
}

func runConnectCLI(args []string, stdin io.ReadCloser, stdout io.WriteCloser, stderr io.Writer) int {
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dialTO  time.Duration
		verbose bool
	)

	fs.DurationVar(&dialTO, "dial-timeout", defaultDialTimeout, "target dial timeout")
	fs.BoolVar(&verbose, "v", false, "verbose logs to stderr")

	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage:
  tcp2socket connect [options] <target>
  tcp2socket <target>

Examples:
  tcp2socket connect tcp://127.0.0.1:8999
  tcp2socket connect unix:///tmp/app.sock
  tcp2socket 127.0.0.1:8999
  tcp2socket /tmp/app.sock

Important:
  stdin/stdout are used as the raw data channel.
  All logs are written to stderr.
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}

	if dialTO < 0 {
		fmt.Fprintf(stderr, "invalid dial timeout\n")
		return 1
	}

	targetEP, err := endpoint.Parse(fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "invalid target endpoint: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialer := &transport.DirectDialer{Timeout: dialTO}
	targetStream, err := dialer.DialContext(ctx, transport.Target{
		Network: targetEP.Network,
		Address: targetEP.Address,
	})
	if err != nil {
		fmt.Fprintf(stderr, "dial %s: %v\n", targetEP.Raw, err)
		return 1
	}
	defer targetStream.Close()

	if verbose {
		fmt.Fprintf(stderr, "connected stdio <-> %s\n", targetEP.Raw)
	}

	stdioStream := transport.NewStdio(stdin, stdout)

	res := proxy.Relay(ctx, stdioStream, targetStream, proxy.RelayOptions{
		StopOnTargetEOF: true,
	})

	if verbose {
		fmt.Fprintf(stderr, "event=closed target=%s reason=%s s2t=%d t2s=%d elapsed_ms=%d\n",
			targetEP.Raw, res.Reason, res.SourceToTarget, res.TargetToSource, res.Duration.Milliseconds())
	}

	if res.Err != nil && !errors.Is(res.Err, context.Canceled) {
		fmt.Fprintf(stderr, "connect relay error: %v\n", res.Err)
		return 1
	}

	return 0
}

func runSocksStdioCLI(args []string, stdin io.ReadCloser, stdout io.WriteCloser, stderr io.Writer) int {
	fs := flag.NewFlagSet("socks-stdio", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		dialTO  time.Duration
		verbose bool
	)

	fs.DurationVar(&dialTO, "dial-timeout", defaultDialTimeout, "target dial timeout")
	fs.BoolVar(&verbose, "v", false, "verbose logs to stderr")

	fs.Usage = func() {
		fmt.Fprintf(stderr, `Usage:
  tcp2socket socks-stdio [options]

Serve one SOCKS5 connection over stdin/stdout.
Supports no authentication and the CONNECT command for TCP targets.

Example:
  ssh host 'exec tcp2socket socks-stdio'

Important:
  stdin/stdout are used as the raw SOCKS5 data channel.
  All logs are written to stderr.
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

	if dialTO < 0 {
		fmt.Fprintf(stderr, "invalid dial timeout\n")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialer := &transport.DirectDialer{Timeout: dialTO}
	stdioStream := transport.NewStdio(stdin, stdout)

	opts := socks5.Options{
		Verbose:         verbose,
		StopOnTargetEOF: true,
		OnConnected: func(target string) {
			if verbose {
				fmt.Fprintf(stderr, "connected stdio SOCKS5 -> %s\n", target)
			}
		},
		OnClosed: func(target string, res proxy.Result) {
			if verbose {
				fmt.Fprintf(stderr, "event=closed target=%s reason=%s s2t=%d t2s=%d elapsed_ms=%d\n",
					target, res.Reason, res.SourceToTarget, res.TargetToSource, res.Duration.Milliseconds())
			}
		},
	}

	if err := socks5.Serve(ctx, stdioStream, dialer, opts); err != nil {
		if errors.Is(err, context.Canceled) {
			return 0
		}
		fmt.Fprintf(stderr, "socks-stdio error: %v\n", err)
		return 1
	}

	return 0
}
