package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func tcpPair(t testing.TB) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.AcceptTCP()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func resultWithin(t *testing.T, done <-chan Result) Result {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("relay did not terminate")
		return Result{}
	}
}

func TestRelayDelayedResponse(t *testing.T) {
	client, source := tcpPair(t)
	target, server := tcpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Relay(ctx, source, target, RelayOptions{}) }()
	serverDone := make(chan error, 1)
	go func() {
		request, err := io.ReadAll(server)
		if err == nil && string(request) != "request" {
			err = errors.New("incorrect request")
		}
		if err == nil {
			time.Sleep(350 * time.Millisecond)
			_, err = server.Write([]byte("response"))
		}
		_ = server.CloseWrite()
		serverDone <- err
	}()
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "response" {
		t.Fatalf("response=%q err=%v", response, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	r := resultWithin(t, done)
	if r.Err != nil || r.SourceToTarget != 7 || r.TargetToSource != 8 || r.Reason != "eof" {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayCancellation(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	defer peerB.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() { done <- Relay(ctx, a, b, RelayOptions{}) }()
	cancel()
	if r := resultWithin(t, done); !errors.Is(r.Err, context.Canceled) {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayIdleTimeout(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	defer peerB.Close()
	done := make(chan Result, 1)
	go func() { done <- Relay(context.Background(), a, b, RelayOptions{IdleTimeout: 30 * time.Millisecond}) }()
	if r := resultWithin(t, done); !errors.Is(r.Err, ErrIdleTimeout) {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayActivityRefreshesIdleTimeout(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	defer peerB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Relay(ctx, a, b, RelayOptions{IdleTimeout: 300 * time.Millisecond}) }()
	for i := 0; i < 5; i++ {
		write := make(chan error, 1)
		go func() { _, err := peerA.Write([]byte("x")); write <- err }()
		_ = peerB.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := io.ReadFull(peerB, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		if err := <-write; err != nil {
			t.Fatal(err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	cancel()
	r := resultWithin(t, done)
	if !errors.Is(r.Err, context.Canceled) || r.SourceToTarget != 5 {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayTargetEOFStopsUnfinishedSource(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	done := make(chan Result, 1)
	go func() { done <- Relay(context.Background(), a, b, RelayOptions{StopOnTargetEOF: true}) }()
	_ = peerB.Close()
	if r := resultWithin(t, done); r.Reason != "target_eof" || r.Err != nil {
		t.Fatalf("result=%+v", r)
	}
}

func BenchmarkRelay(b *testing.B) {
	payload := make([]byte, 1024*1024)
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		client, source := tcpPair(b)
		target, server := tcpPair(b)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan Result, 1)
		go func() { done <- Relay(ctx, source, target, RelayOptions{}) }()
		copied := make(chan error, 1)
		go func() { _, err := io.Copy(io.Discard, server); _ = server.CloseWrite(); copied <- err }()
		_, err := client.Write(payload)
		_ = client.CloseWrite()
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, client)
		if err := <-copied; err != nil {
			b.Fatal(err)
		}
		r := <-done
		cancel()
		client.Close()
		server.Close()
		if r.Err != nil {
			b.Fatal(r.Err)
		}
	}
}
