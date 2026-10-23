package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"tcp2socket/internal/transport"
)

type dialFunc func(context.Context, transport.Target) (transport.Stream, error)

func (f dialFunc) DialContext(ctx context.Context, target transport.Target) (transport.Stream, error) {
	return f(ctx, target)
}

func TestServerQuotaAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listening := make(chan string, 1)
	started := make(chan struct{}, 4)
	rejected := make(chan struct{}, 4)
	var active, maximum atomic.Int32
	dialer := dialFunc(func(ctx context.Context, _ transport.Target) (transport.Stream, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	done := make(chan error, 1)
	go func() {
		done <- ListenAndServe(ctx, ServerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0", Dialer: dialer,
			MaxConnections: 1, MaxPendingAccept: 1, ShutdownTimeout: 30 * time.Millisecond,
			OnEvent: func(e Event) {
				switch e.Event {
				case "listening":
					listening <- e.Source
				case "rejected":
					rejected <- struct{}{}
				}
			},
		})
	}()
	var addr string
	select {
	case addr = <-listening:
	case <-time.After(time.Second):
		t.Fatal("server not listening")
	}
	dial := func() net.Conn {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	_ = dial()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dial not started")
	}
	_ = dial()
	_ = dial()
	select {
	case <-rejected:
	case <-time.After(time.Second):
		t.Fatal("overflow not rejected")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown hung")
	}
	if maximum.Load() != 1 || active.Load() != 0 {
		t.Fatalf("maximum=%d active=%d", maximum.Load(), active.Load())
	}
	select {
	case <-started:
		t.Fatal("queued connection dialed during shutdown")
	default:
	}
}

func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "t2s-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestUnixSocketSafety(t *testing.T) {
	t.Run("active", func(t *testing.T) {
		path := shortSocketPath(t)
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		before, _ := os.Lstat(path)
		if err := prepareUnixSocket(path); err == nil {
			t.Fatal("active socket accepted")
		}
		after, err := os.Lstat(path)
		if err != nil || !os.SameFile(before, after) {
			t.Fatal("active socket replaced")
		}
	})
	t.Run("stale", func(t *testing.T) {
		path := shortSocketPath(t)
		ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		ln.SetUnlinkOnClose(false)
		ln.Close()
		if err := prepareUnixSocket(path); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("stale path remains: %v", err)
		}
	})
	t.Run("file_and_symlink", func(t *testing.T) {
		path := shortSocketPath(t)
		if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := prepareUnixSocket(path); err == nil {
			t.Fatal("file accepted")
		}
		link := path + ".link"
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		if err := prepareUnixSocket(link); err == nil {
			t.Fatal("symlink accepted")
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "keep" {
			t.Fatal("file changed")
		}
	})
	t.Run("cleanup_preserves_replacement", func(t *testing.T) {
		path := shortSocketPath(t)
		ln, cleanup, err := listen("unix", path, 0600)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("mode %v", info.Mode())
		}
		defer ln.Close()
		if err := os.Rename(path, path+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
			t.Fatal(err)
		}
		cleanup()
		if data, err := os.ReadFile(path); err != nil || string(data) != "replacement" {
			t.Fatal("replacement removed")
		}
	})
}

type failWriter struct{ net.Conn }

func (f failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func TestRelayWriteFailure(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	defer peerB.Close()
	done := make(chan Result, 1)
	go func() { done <- Relay(context.Background(), a, failWriter{b}, RelayOptions{}) }()
	_ = peerA.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = peerA.Write([]byte("payload"))
	r := resultWithin(t, done)
	if !errors.Is(r.Err, io.ErrClosedPipe) || r.Reason != "io_error" {
		t.Fatalf("result=%+v", r)
	}
}

func TestRelayCancelBlockedWrite(t *testing.T) {
	a, peerA := net.Pipe()
	b, peerB := net.Pipe()
	defer peerA.Close()
	defer peerB.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Result, 1)
	go func() { done <- Relay(ctx, a, b, RelayOptions{}) }()
	_ = peerA.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peerA.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if r := resultWithin(t, done); !errors.Is(r.Err, context.Canceled) {
		t.Fatalf("result=%+v", r)
	}
}
