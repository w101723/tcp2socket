package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
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

func TestProxyResourceBaseline(t *testing.T) {
	addr, stop := stressEchoServer(t)
	defer stop()

	before := runtime.NumGoroutine()
	fdsBefore, err := openFDCount()

	proxyAddr := fmt.Sprintf("127.0.0.1:%d", getFreeTCPPort(t))
	proxy := startProcess(t, "-l", proxyAddr, "-t", addr)
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForEndpoint("tcp", proxyAddr, 5*time.Second); err != nil {
		t.Fatal(err)
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
	if err := proxy.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := proxy.WaitTimed(3 * time.Second); err != nil {
		t.Fatalf("proxy exit: %v", err)
	}

	if err == nil {
		if fdsAfter, err := openFDCount(); err == nil && fdsAfter > fdsBefore {
			t.Errorf("descriptor leak: before=%d after=%d", fdsBefore, fdsAfter)
		}
	} else {
		t.Logf("skip fd check: %v", err)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
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
