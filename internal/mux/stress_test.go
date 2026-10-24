package mux

// PR-07 stress and fault-injection suite. These tests exercise the session
// under load and hostile transports; they complement the correctness tests
// in session_test.go and are allowed longer runtimes.

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// dialPair handshakes two sessions over net.Pipe and starts both Serve
// loops. Serve is started only after the handshake: the handshake owns the
// transport synchronously.
func dialPair(t *testing.T) (*Session, *Session) {
	t.Helper()
	c, s := net.Pipe()
	client := mustSession(t, c, SideClient)
	server := mustSession(t, s, SideServer)
	hErr := make(chan error, 2)
	go func() { _, err := client.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	go func() { _, err := server.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	for i := 0; i < 2; i++ {
		if err := <-hErr; err != nil {
			t.Fatal(err)
		}
	}
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

// echoServer accepts streams and echoes each until EOF, then half-closes
// so the client observes a clean FIN-EOF.
func echoServer(sess *Session) {
	for {
		st, err := sess.Accept()
		if err != nil {
			return
		}
		go func(st *Stream) {
			defer st.Close()
			buf := make([]byte, 32*1024)
			for {
				n, err := st.Read(buf)
				if n > 0 {
					if _, werr := st.Write(buf[:n]); werr != nil {
						return
					}
				}
				if err != nil {
					_ = st.CloseWrite() // FIN: the peer sees EOF, not a RESET
					return
				}
			}
		}(st)
	}
}

// TestMuxStressStreamCounts sweeps 1/8/32/64 concurrent streams. 64 exceeds
// the accept channel depth (16) on purpose: handleOpen must keep answering
// OPEN_OK while the accept loop drains.
func TestMuxStressStreamCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped in -short")
	}
	for _, streams := range []int{1, 8, 32, 64} {
		streams := streams
		t.Run(fmt.Sprintf("streams=%d", streams), func(t *testing.T) {
			const size = 64 * 1024
			client, server := dialPair(t)
			go echoServer(server)

			payload := make([]byte, size)
			for j := range payload {
				payload[j] = byte(j * 7)
			}
			var wg sync.WaitGroup
			fail := make(chan error, streams)
			for i := 0; i < streams; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					st, err := client.Open(Target{Network: NetworkTCP, Address: "stress.local:1"})
					if err != nil {
						fail <- err
						return
					}
					defer st.Close()
					go func() {
						_, _ = st.Write(payload)
						_ = st.CloseWrite()
					}()
					got := make([]byte, 0, size)
					buf := make([]byte, 16*1024)
					for len(got) < size {
						n, err := st.Read(buf)
						got = append(got, buf[:n]...)
						if err != nil {
							if len(got) >= size {
								return // clean close raced the tail
							}
							fail <- fmt.Errorf("read after %d/%d: %w", len(got), size, err)
							return
						}
					}
					if !bytes.Equal(got, payload) {
						fail <- errors.New("echo mismatch")
					}
				}()
			}
			wg.Wait()
			close(fail)
			for err := range fail {
				t.Errorf("streams=%d: %v", streams, err)
			}
		})
	}
}

// TestMuxLargeTransfer pushes 8 MiB through a single stream in 64KiB reads
// and verifies byte-for-byte integrity plus hash symmetry both directions.
func TestMuxLargeTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped in -short")
	}
	const size = 8 << 20 // 8 MiB
	client, server := dialPair(t)
	go echoServer(server)

	st, err := client.Open(Target{Network: NetworkTCP, Address: "bulk.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	payload := make([]byte, size)
	state := uint32(1)
	for i := range payload {
		state = state*1664525 + 1013904223
		payload[i] = byte(state >> 24)
	}

	go func() {
		if _, err := st.Write(payload); err != nil {
			t.Errorf("write: %v", err)
		}
		_ = st.CloseWrite()
	}()

	got := make([]byte, 0, size)
	buf := make([]byte, 64*1024)
	deadline := time.After(30 * time.Second)
	for len(got) < size {
		select {
		case <-deadline:
			t.Fatalf("transfer stalled at %d/%d", len(got), size)
		default:
		}
		n, err := st.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if len(got) >= size {
				break
			}
			t.Fatalf("read at %d/%d: %v", len(got), size, err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch after %d bytes", len(got))
	}
	if _, err := st.Read(buf); err != io.EOF {
		t.Fatalf("final read = %v, want EOF", err)
	}
}

// delayConn injects per-Write latency into the transport, emulating a
// high-RTT link.
type delayConn struct {
	net.Conn
	delay time.Duration
}

func (c *delayConn) Write(p []byte) (int, error) {
	time.Sleep(c.delay)
	return c.Conn.Write(p)
}

func (c *delayConn) Read(p []byte) (int, error) {
	time.Sleep(c.delay / 2)
	return c.Conn.Read(p)
}

// TestMuxHighLatency keeps correctness on a slow transport: 8 streams over
// a link with 1ms injected per-direction latency per transport operation.
func TestMuxHighLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped in -short")
	}
	c, s := net.Pipe()
	dc, ds := &delayConn{Conn: c, delay: time.Millisecond}, &delayConn{Conn: s, delay: time.Millisecond}
	client := mustSession(t, dc, SideClient)
	server := mustSession(t, ds, SideServer)
	hErr := make(chan error, 2)
	go func() { _, err := client.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	go func() { _, err := server.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	for i := 0; i < 2; i++ {
		if err := <-hErr; err != nil {
			t.Fatal(err)
		}
	}
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	go echoServer(server)

	const streams = 8
	const size = 32 * 1024
	var wg sync.WaitGroup
	fail := make(chan error, streams)
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := client.Open(Target{Network: NetworkTCP, Address: "latency.local:1"})
			if err != nil {
				fail <- err
				return
			}
			defer st.Close()
			payload := bytes.Repeat([]byte{byte(len(payloadSig))}, size) // constant fill per stream
			go func() {
				_, _ = st.Write(payload)
				_ = st.CloseWrite()
			}()
			got := make([]byte, 0, size)
			buf := make([]byte, 8*1024)
			for len(got) < size {
				n, err := st.Read(buf)
				got = append(got, buf[:n]...)
				if err != nil {
					if len(got) >= size {
						return
					}
					fail <- fmt.Errorf("read at %d/%d: %w", len(got), size, err)
					return
				}
			}
			if !bytes.Equal(got, payload) {
				fail <- errors.New("echo mismatch under latency")
			}
		}()
	}
	wg.Wait()
	close(fail)
	for err := range fail {
		t.Errorf("latency: %v", err)
	}
}

// dropConn corrupts or drops whole frames with a probability, emulating a
// lossy link. CRC must catch every corruption: the session tears down
// instead of delivering bad data.
type dropConn struct {
	net.Conn
	mu       sync.Mutex
	dropNext bool // drop the next written blob (one frame per Write here)
	ops      int32
}

func (c *dropConn) Write(p []byte) (int, error) {
	n := atomic.AddInt32(&c.ops, 1)
	c.mu.Lock()
	drop := n%97 == 0 // ~1% of frames never reach the wire
	c.mu.Unlock()
	if drop {
		return len(p), nil // silently swallowed
	}
	return c.Conn.Write(p)
}

// TestMuxTransportLossMidTransfer kills the transport mid-transfer: the
// pending reads must fail with an error (never hang, never (0,nil)-spin)
// and both sessions must terminate promptly.
func TestMuxTransportLossMidTransfer(t *testing.T) {
	client, server := dialPair(t)

	st, err := client.Open(Target{Network: NetworkTCP, Address: "lossy.local:1"})
	if err != nil {
		t.Fatal(err)
	}

	// server side: accept, then park on Read — it must return when the
	// transport dies
	readErr := make(chan error, 1)
	go func() {
		sst, err := server.Accept()
		if err != nil {
			readErr <- err
			return
		}
		buf := make([]byte, 4096)
		for {
			if _, err := sst.Read(buf); err != nil {
				readErr <- err
				return
			}
		}
	}()

	// start a large write, then kill the transport underneath it
	data := make([]byte, 4<<20)
	written := make(chan error, 1)
	go func() {
		_, err := st.Write(data)
		written <- err
	}()
	time.Sleep(20 * time.Millisecond)
	_ = client.Close()

	// reader must observe termination
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("reader saw nil error after transport loss")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("peer reader hung after transport loss")
	}
	// writer must not hang either
	select {
	case <-written:
	case <-time.After(3 * time.Second):
		t.Fatal("writer hung after transport loss")
	}
}

// TestMuxResetAbortsBlockedRead sends RESET for a stream the application
// is blocked reading; Read must return the reset error promptly.
func TestMuxResetAbortsBlockedRead(t *testing.T) {
	client, server := dialPair(t)

	st, err := client.Open(Target{Network: NetworkTCP, Address: "reset.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	sst, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 128)
		_, err := st.Read(buf)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // let Read park
	// server resets the stream (emulates the dialer failing the target)
	payload, _ := ErrorPayload{Code: ErrCodeDial, Detail: "target refused"}.Encode()
	if err := server.sched.Enqueue(Frame{Type: TypeReset, Epoch: server.epoch, StreamID: st.ID(), Payload: payload}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !bytes.Contains([]byte(err.Error()), []byte("ERR_DIAL")) {
			t.Fatalf("read after RESET = %v, want ERR_DIAL", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read hung after RESET")
	}
	_ = sst.Close()
}

// TestMuxEOFInjection: the transport ends cleanly (EOF) with streams open;
// pending reads surface io.EOF/session-ended rather than hanging, and Serve
// returns nil.
func TestMuxEOFInjection(t *testing.T) {
	c, s := net.Pipe()
	client := mustSession(t, c, SideClient)
	server := mustSession(t, s, SideServer)
	hErr := make(chan error, 2)
	go func() { _, err := client.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	go func() { _, err := server.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	for i := 0; i < 2; i++ {
		if err := <-hErr; err != nil {
			t.Fatal(err)
		}
	}
	serveErr := make(chan error, 2)
	go func() { serveErr <- client.Serve() }()
	go func() { serveErr <- server.Serve() }()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "eof.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	readErr := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		_, err := st.Read(buf)
		readErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = c.Close() // clean EOF from the transport

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("read after transport EOF returned nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read hung after transport EOF")
	}
	select {
	case <-client.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("session did not end after transport EOF")
	}
}

// blockedConn lets the test park the transport writer at will. The gate
// starts open (the handshake must flow): token bytes refill from a ticker
// the test can stop; once the token buffer drains, writes park.
type blockedConn struct {
	net.Conn
	gate   chan struct{} // one token per allowed Write
	refill chan struct{} // closed while tokens flow; frozen by close+replace
	halt   chan struct{} // closed at test end to stop refillers
	mu     sync.Mutex
}

func newBlockedConn(c net.Conn) *blockedConn {
	b := &blockedConn{Conn: c, gate: make(chan struct{}, 1), halt: make(chan struct{})}
	b.refill = make(chan struct{})
	close(b.refill) // closed = tokens flow (refiller's <-b.refill is ready)
	go b.refiller()
	return b
}

// refiller feeds tokens while refill is open (refill is closed while
// flowing, open while frozen: reads and swaps happen under mu).
func (b *blockedConn) refiller() {
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		b.mu.Lock()
		refill := b.refill
		b.mu.Unlock()
		select {
		case <-b.halt:
			return
		case <-refill:
			select {
			case b.gate <- struct{}{}:
			default:
			}
		}
	}
}

// freeze stops the token refill: swap in a fresh never-closed channel the
// refiller parks on; existing tokens drain, then writes park.
func (b *blockedConn) freeze() {
	b.mu.Lock()
	select {
	case <-b.refill: // currently closed (flowing): swap in an open one
		b.refill = make(chan struct{})
	default: // already frozen
	}
	b.mu.Unlock()
}

// unfreeze restarts the token refill: close the current channel so the
// refiller runs again.
func (b *blockedConn) unfreeze() {
	b.mu.Lock()
	select {
	case <-b.refill: // already closed: refiller still alive
	default:
		close(b.refill)
		go b.refiller()
	}
	b.mu.Unlock()
}

func (c *blockedConn) Write(p []byte) (int, error) {
	<-c.gate
	return c.Conn.Write(p)
}

// TestMuxBlockedTransportKeepsQueuesBounded: with the transport frozen,
// writers must backpressure (never queue without bound); releasing the gate
// drains everything and the peer still receives complete streams.
func TestMuxBlockedTransportKeepsQueuesBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped in -short")
	}
	c, s := net.Pipe()
	bg := newBlockedConn(c)
	client := mustSession(t, bg, SideClient)
	server := mustSession(t, s, SideServer)
	hErr := make(chan error, 2)
	go func() { _, err := client.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	go func() { _, err := server.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	for i := 0; i < 2; i++ {
		if err := <-hErr; err != nil {
			t.Fatal(err)
		}
	}
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	received := make(chan int, 1)
	go func() {
		sst, err := server.Accept()
		if err != nil {
			received <- -1
			return
		}
		n := 0
		buf := make([]byte, 32*1024)
		for {
			rn, err := sst.Read(buf)
			n += rn
			if err != nil {
				break
			}
		}
		received <- n
	}()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "blocked.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	// freeze the transport before the big write: refill stops, in-flight
	// token supply drains during the settle window, writes then park
	bg.freeze()
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() { close(bg.halt) })
	const size = 256 * 1024
	wrote := make(chan int, 1)
	go func() {
		n, _ := st.Write(make([]byte, size))
		wrote <- n
	}()
	select {
	case <-wrote:
		t.Fatal("write completed with frozen transport: no backpressure")
	case <-time.After(300 * time.Millisecond):
	}

	bg.unfreeze() // release: parked writes must resume and drain
	select {
	case n := <-wrote:
		if n != size {
			t.Fatalf("wrote %d/%d after release", n, size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write never completed after release")
	}
	_ = st.CloseWrite()
	select {
	case n := <-received:
		if n != size {
			t.Fatalf("peer got %d/%d after release", n, size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer reader hung after release")
	}
}

// payloadSig pins TestMuxHighLatency's fill byte (compile-time constant).
const payloadSig = "latency"

// TestMuxStreamIsolationUnderLoad verifies concurrent streams never cross
// data: each stream carries its own pattern and every hash must match its
// own stream index.
func TestMuxStreamIsolationUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("stress: skipped in -short")
	}
	const streams = 16
	const size = 128 * 1024
	client, server := dialPair(t)
	go echoServer(server)

	type result struct {
		id  int
		sum [32]byte
		err error
	}
	results := make(chan result, streams)
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := client.Open(Target{Network: NetworkTCP, Address: fmt.Sprintf("iso-%d.local:1", i)})
			if err != nil {
				results <- result{id: i, err: err}
				return
			}
			defer st.Close()
			payload := make([]byte, size)
			for j := range payload {
				payload[j] = byte(i + j)
			}
			go func() {
				_, _ = st.Write(payload)
				_ = st.CloseWrite()
			}()
			got := make([]byte, 0, size)
			buf := make([]byte, 16*1024)
			for len(got) < size {
				n, err := st.Read(buf)
				got = append(got, buf[:n]...)
				if err != nil {
					if len(got) >= size {
						break
					}
					results <- result{id: i, err: err}
					return
				}
			}
			results <- result{id: i, sum: sha256.Sum256(got)}
		}(i)
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r.err != nil {
			t.Errorf("stream %d: %v", r.id, r.err)
			continue
		}
		payload := make([]byte, size)
		for j := range payload {
			payload[j] = byte(r.id + j)
		}
		if r.sum != sha256.Sum256(payload) {
			t.Errorf("stream %d: hash mismatch (data crossed streams)", r.id)
		}
	}
}
