package proxy

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestRelayTargetHalfClosePreservesSourceUpload(t *testing.T) {
	client, source := tcpPair(t)
	target, server := tcpPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Relay(ctx, source, target, RelayOptions{}) }()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	_ = server.SetDeadline(time.Now().Add(time.Second))
	if err := server.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(client); err != nil || len(data) != 0 {
		t.Fatalf("read=%q err=%v", data, err)
	}
	if _, err := client.Write([]byte("upload after remote FIN")); err != nil {
		t.Fatal(err)
	}
	_ = client.CloseWrite()
	data, err := io.ReadAll(server)
	if err != nil || string(data) != "upload after remote FIN" {
		t.Fatalf("upload=%q err=%v", data, err)
	}
	if r := resultWithin(t, done); r.Err != nil || r.SourceToTarget != int64(len(data)) {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayReadDeadlineFailure(t *testing.T) {
	_, source := tcpPair(t)
	target, _ := tcpPair(t)
	if err := source.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	done := make(chan Result, 1)
	go func() { done <- Relay(context.Background(), source, target, RelayOptions{}) }()
	r := resultWithin(t, done)
	if r.Err == nil || r.Reason != "io_error" {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayRepeatedCancellationJoinsCopies(t *testing.T) {
	for i := 0; i < 50; i++ {
		TestRelayCancellation(t)
	}
}
