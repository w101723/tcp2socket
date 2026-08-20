package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type endpoint struct {
	network string
	address string
	raw     string
}

func parseEndpoint(s string) (endpoint, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return endpoint{}, errors.New("empty endpoint")
	}

	switch {
	case strings.HasPrefix(s, "tcp://"):
		addr := strings.TrimPrefix(s, "tcp://")
		if addr == "" {
			return endpoint{}, fmt.Errorf("invalid tcp endpoint: %q", s)
		}
		return endpoint{network: "tcp", address: addr, raw: s}, nil

	case strings.HasPrefix(s, "unix://"):
		path := strings.TrimPrefix(s, "unix://")
		if path == "" {
			return endpoint{}, fmt.Errorf("invalid unix endpoint: %q", s)
		}
		if !filepath.IsAbs(path) {
			return endpoint{}, fmt.Errorf("unix socket path must be absolute: %q", path)
		}
		return endpoint{network: "unix", address: path, raw: s}, nil

	default:
		if strings.HasPrefix(s, "/") {
			return endpoint{network: "unix", address: s, raw: s}, nil
		}
		if strings.Contains(s, ":") {
			return endpoint{network: "tcp", address: s, raw: s}, nil
		}
		return endpoint{}, fmt.Errorf(
			"unsupported endpoint %q; use tcp://host:port or unix:///path.sock", s,
		)
	}
}

type config struct {
	listen      endpoint
	target      endpoint
	dialTimeout time.Duration
	idleTimeout time.Duration
	unixMode    os.FileMode
	verbose     bool
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.LstdFlags)

	// nc-compatible shorthand:
	//   tcp2socket 127.0.0.1:8999
	//   tcp2socket /tmp/app.sock
	//
	// Explicit mode:
	//   tcp2socket connect tcp://127.0.0.1:8999
	//   tcp2socket connect unix:///tmp/app.sock
	if len(os.Args) >= 2 {
		if os.Args[1] == "socks-stdio" {
			runSocksStdioCLI(os.Args[2:])
			return
		}
		if os.Args[1] == "connect" {
			runConnectCLI(os.Args[2:])
			return
		}
		if !strings.HasPrefix(os.Args[1], "-") {
			runConnectCLI(os.Args[1:])
			return
		}
	}

	runProxyCLI(os.Args[1:])
}

func runProxyCLI(args []string) {
	fs := flag.NewFlagSet("tcp2socket", flag.ExitOnError)

	var (
		listenStr string
		targetStr string
		dialTO    time.Duration
		idleTO    time.Duration
		unixMode  string
		verbose   bool
	)

	fs.StringVar(&listenStr, "listen", "", "listen endpoint: tcp://0.0.0.0:8080 or unix:///tmp/in.sock")
	fs.StringVar(&listenStr, "l", "", "short form of --listen")
	fs.StringVar(&targetStr, "target", "", "target endpoint: tcp://127.0.0.1:1080 or unix:///tmp/out.sock")
	fs.StringVar(&targetStr, "t", "", "short form of --target")
	fs.DurationVar(&dialTO, "dial-timeout", 10*time.Second, "target dial timeout")
	fs.DurationVar(&idleTO, "idle-timeout", 0, "connection idle timeout; 0 disables it")
	fs.StringVar(&unixMode, "unix-mode", "0660", "permission mode for listened unix socket")
	fs.BoolVar(&verbose, "v", false, "verbose connection logs")

	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintf(out, `tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy

Proxy mode:
  tcp2socket -l <listen> -t <target> [options]

Connect mode:
  tcp2socket connect <target> [options]

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
		fs.PrintDefaults()
	}

	_ = fs.Parse(args)

	if listenStr == "" || targetStr == "" {
		fs.Usage()
		os.Exit(2)
	}

	listenEP, err := parseEndpoint(listenStr)
	if err != nil {
		log.Fatalf("invalid listen endpoint: %v", err)
	}

	targetEP, err := parseEndpoint(targetStr)
	if err != nil {
		log.Fatalf("invalid target endpoint: %v", err)
	}

	mode64, err := strconv.ParseUint(unixMode, 8, 32)
	if err != nil {
		log.Fatalf("invalid --unix-mode %q: %v", unixMode, err)
	}

	cfg := config{
		listen:      listenEP,
		target:      targetEP,
		dialTimeout: dialTO,
		idleTimeout: idleTO,
		unixMode:    os.FileMode(mode64),
		verbose:     verbose,
	}

	if err := runProxy(cfg); err != nil {
		log.Fatal(err)
	}
}

func runSocksStdioCLI(args []string) {
	fs := flag.NewFlagSet("socks-stdio", flag.ExitOnError)

	var (
		dialTO  time.Duration
		verbose bool
	)

	fs.DurationVar(&dialTO, "dial-timeout", 10*time.Second, "target dial timeout")
	fs.BoolVar(&verbose, "v", false, "verbose logs to stderr")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `Usage:
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

	_ = fs.Parse(args)

	if fs.NArg() != 0 {
		fs.Usage()
		os.Exit(2)
	}

	if err := runSocksStdio(dialTO, verbose); err != nil {
		log.Fatal(err)
	}
}

func runConnectCLI(args []string) {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)

	var (
		dialTO  time.Duration
		verbose bool
	)

	fs.DurationVar(&dialTO, "dial-timeout", 10*time.Second, "target dial timeout")
	fs.BoolVar(&verbose, "v", false, "verbose logs to stderr")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), `Usage:
  tcp2socket connect <target> [options]
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

	_ = fs.Parse(args)

	if fs.NArg() != 1 {
		fs.Usage()
		os.Exit(2)
	}

	target, err := parseEndpoint(fs.Arg(0))
	if err != nil {
		log.Fatalf("invalid target endpoint: %v", err)
	}

	if err := runConnect(target, dialTO, verbose); err != nil {
		log.Fatal(err)
	}
}

func runConnect(target endpoint, dialTimeout time.Duration, verbose bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dialer := net.Dialer{Timeout: dialTimeout}

	conn, err := dialer.DialContext(ctx, target.network, target.address)
	if err != nil {
		return fmt.Errorf("dial %s: %w", target.raw, err)
	}
	defer conn.Close()

	if verbose {
		log.Printf("connected stdio <-> %s", target.raw)
	}

	// When used as:
	//   ssh host 'exec tcp2socket 127.0.0.1:8999'
	// stdout is the transport stream. Never write logs to stdout.
	//
	// stdin  -> socket
	// socket -> stdout
	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(conn, os.Stdin)
		closeWrite(conn)
		errCh <- err
	}()

	go func() {
		_, err := io.Copy(os.Stdout, conn)
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil
	case err := <-errCh:
		// Wait briefly for the opposite direction to finish naturally after half-close.
		select {
		case <-errCh:
		case <-time.After(200 * time.Millisecond):
			_ = conn.Close()
		}
		if err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	}
}

func runProxy(cfg config) error {
	if cfg.listen.network == "unix" {
		if err := prepareUnixSocket(cfg.listen.address); err != nil {
			return err
		}
		defer os.Remove(cfg.listen.address)
	}

	ln, err := net.Listen(cfg.listen.network, cfg.listen.address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listen.raw, err)
	}
	defer ln.Close()

	if cfg.listen.network == "unix" {
		if err := os.Chmod(cfg.listen.address, cfg.unixMode); err != nil {
			return fmt.Errorf("chmod unix socket %s: %w", cfg.listen.address, err)
		}
	}

	log.Printf("listening on %s -> %s", cfg.listen.raw, cfg.target.raw)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("shutdown")
				return nil
			}

			var ne net.Error
			if errors.As(err, &ne) && ne.Temporary() {
				log.Printf("temporary accept error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}

		wg.Add(1)
		go func(src net.Conn) {
			defer wg.Done()
			handleConn(ctx, cfg, src)
		}(conn)
	}
}

func prepareUnixSocket(path string) error {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing to remove non-socket path: %s", path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale unix socket %s: %w", path, err)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("stat unix socket %s: %w", path, err)
	}
	return nil
}

func handleConn(ctx context.Context, cfg config, src net.Conn) {
	defer src.Close()

	dialer := net.Dialer{Timeout: cfg.dialTimeout}

	dst, err := dialer.DialContext(ctx, cfg.target.network, cfg.target.address)
	if err != nil {
		log.Printf("dial %s failed: %v", cfg.target.raw, err)
		return
	}
	defer dst.Close()

	if cfg.verbose {
		log.Printf("connected: %s -> %s", remoteAddr(src), cfg.target.raw)
		defer log.Printf("closed: %s -> %s", remoteAddr(src), cfg.target.raw)
	}

	if cfg.idleTimeout > 0 {
		proxyWithIdleTimeout(src, dst, cfg.idleTimeout)
		return
	}

	proxy(src, dst)
}

func proxy(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(b, a)
		closeWrite(b)
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(a, b)
		closeWrite(a)
	}()

	wg.Wait()
}

func proxyWithIdleTimeout(a, b net.Conn, timeout time.Duration) {
	setDeadline := func(c net.Conn) {
		_ = c.SetDeadline(time.Now().Add(timeout))
	}

	setDeadline(a)
	setDeadline(b)

	var wg sync.WaitGroup
	wg.Add(2)

	copyWithRefresh := func(dst, src net.Conn) {
		defer wg.Done()
		buf := make([]byte, 32*1024)

		for {
			n, err := src.Read(buf)
			if n > 0 {
				setDeadline(src)
				setDeadline(dst)

				if _, werr := dst.Write(buf[:n]); werr != nil {
					break
				}

				setDeadline(src)
				setDeadline(dst)
			}
			if err != nil {
				break
			}
		}
		closeWrite(dst)
	}

	go copyWithRefresh(b, a)
	go copyWithRefresh(a, b)
	wg.Wait()
}

func closeWrite(c net.Conn) {
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := c.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func remoteAddr(c net.Conn) string {
	if c.RemoteAddr() == nil {
		return "unknown"
	}
	return c.RemoteAddr().String()
}
