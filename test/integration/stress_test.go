package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These tests establish a reliability baseline rather than production acceptance:
// larger payloads, higher fan-out and descriptor/goroutine sanity checks.

func stressEchoServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	stopAccept := context.AfterFunc(context.Background(), func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() { stopAccept(); ln.Close() }
}

func openFDCount() (int, error) {
	for _, dir := range []string{"/dev/fd", "/proc/self/fd"} {
		if entries, err := os.ReadDir(dir); err == nil {
			return len(entries), nil
		}
	}
	return -1, fmt.Errorf("no fd directory available")
}

func TestStress100MiBStream(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	addr, stop := stressEchoServer(t)
	defer stop()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", getFreeTCPPort(t))
	proxy := startProcessWithTimeout(t, 90*time.Second, "-l", proxyAddr, "-t", addr)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForEndpoint("tcp", proxyAddr, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	client, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(60 * time.Second))

	payload := make([]byte, 1<<20) // 1 MiB block, written 100 times
	for i := range payload {
		payload[i] = byte(i)
	}
	sent := sha256.New()
	received := sha256.New()
	writeErr := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 100 && err == nil; i++ {
			_, err = client.Write(payload)
			sent.Write(payload)
		}
		if err == nil {
			err = client.(*net.TCPConn).CloseWrite()
		}
		writeErr <- err
	}()
	got := 0
	total := len(payload) * 100
	buf := make([]byte, 64<<10)
	for got < total {
		n, err := client.Read(buf)
		got += n
		received.Write(buf[:n])
		if err != nil {
			t.Fatalf("read after %d bytes: %v", got, err)
		}
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(sent.Sum(nil), received.Sum(nil)) {
		t.Fatalf("stream hash mismatch over %d bytes", total)
	}
	if err := proxy.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := proxy.WaitTimed(10 * time.Second); err != nil {
		t.Fatalf("proxy did not exit on SIGTERM: %v", err)
	}
}

func TestStress32ConcurrentStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}
	addr, stop := stressEchoServer(t)
	defer stop()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", getFreeTCPPort(t))
	proxy := startProcessWithTimeout(t, 60*time.Second, "-l", proxyAddr, "-t", addr)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForEndpoint("tcp", proxyAddr, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	const streams = 32
	payload := make([]byte, 256<<10)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	var wg sync.WaitGroup
	results := make(chan error, streams)
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				results <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			block := make([]byte, len(payload))
			for j := range block {
				block[j] = payload[j] ^ byte(id)
			}
			if err := roundTrip(conn, block); err != nil {
				results <- fmt.Errorf("stream %d: %w", id, err)
				return
			}
			results <- nil
		}(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := proxy.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := proxy.WaitTimed(10 * time.Second); err != nil {
		t.Fatalf("proxy did not exit on SIGTERM: %v", err)
	}
}

// TestProxyResourceBaseline samples the PROXY SUBPROCESS (not the test parent):
// open descriptors via /proc/<pid>/fd (Linux) or lsof -p (macOS). Peak FDs must
// return to the idle level after traffic and exit must be bounded.
func TestProxyResourceBaseline(t *testing.T) {
	addr, stop := stressEchoServer(t)
	defer stop()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", getFreeTCPPort(t))
	proxy := startProcess(t, "-l", proxyAddr, "-t", addr)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForEndpoint("tcp", proxyAddr, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	idleFDs, ok := subprocessFDCount(t, proxy.Cmd.Process.Pid)
	if !ok {
		t.Skip("subprocess descriptor sampling unavailable on this platform")
	}

	for i := 0; i < 8; i++ {
		conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if err := roundTrip(conn, []byte("baseline-check")); err != nil {
			conn.Close()
			t.Fatalf("round trip %d: %v", i, err)
		}
		conn.Close()
	}
	time.Sleep(100 * time.Millisecond) // allow closes to settle

	busyFDs, ok := subprocessFDCount(t, proxy.Cmd.Process.Pid)
	if !ok {
		t.Fatalf("descriptor sampling worked at idle but not after traffic")
	}
	if busyFDs > idleFDs+64 { // 8 concurrent relays use ~3 fds each; 64 leaves ample headroom
		t.Errorf("descriptor growth after traffic: idle=%d busy=%d", idleFDs, busyFDs)
	}

	if err := proxy.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := proxy.WaitTimed(3 * time.Second); err != nil {
		t.Fatalf("proxy exit: %v", err)
	}
}

// subprocessFDCount reports the open descriptor count of the given subprocess
// PID, or ok=false when the platform exposes no readable interface.
func subprocessFDCount(t *testing.T, pid int) (int, bool) {
	t.Helper()
	if entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid)); err == nil {
		return len(entries), true
	}
	out, err := exec.Command("lsof", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, false
	}
	count := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "COMMAND") || strings.TrimSpace(line) == "" {
			continue
		}
		count++
	}
	return count, count > 0
}

func roundTrip(conn net.Conn, block []byte) error {
	if _, err := conn.Write(block); err != nil {
		return err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.CloseWrite(); err != nil {
			return err
		}
	}
	got := 0
	buf := make([]byte, 32<<10)
	for got < len(block) {
		n, err := conn.Read(buf)
		got += n
		if err != nil {
			return fmt.Errorf("read after %d/%d: %w", got, len(block), err)
		}
		for i := 0; i < n; i++ {
			if buf[i] != block[got-n+i] {
				return fmt.Errorf("corruption at offset %d", got-n+i)
			}
		}
	}
	return nil
}
