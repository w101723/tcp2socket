package mux

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// mustSession builds a session over rw with protocol defaults.
func mustSession(t *testing.T, rw io.ReadWriteCloser, side Side) *Session {
	t.Helper()
	s, err := NewSession(rw, side, SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestMuxEchoStream: open a stream, exchange data both ways, half-close in
// order, verify EOF propagation end to end through a real echo server.
func TestMuxEchoStream(t *testing.T) {
	c, s := net.Pipe()
	client := mustSession(t, c, SideClient)
	server := mustSession(t, s, SideServer)

	// handshake first (owns the transport), then the loops
	hErr := make(chan error, 2)
	go func() { _, err := client.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	go func() { _, err := server.Handshake([]byte{CapStreamMux}, []byte{CapStreamMux}); hErr <- err }()
	for i := 0; i < 2; i++ {
		if err := <-hErr; err != nil {
			t.Fatalf("handshake: %v", err)
		}
	}
	done := make(chan error, 2)
	go func() { done <- client.Serve() }()
	go func() { done <- server.Serve() }() // Serve may outlive the test: no t.Logf here

	// server accept loop: reply OPEN_OK then echo until EOF
	serverErr := make(chan error, 1)
	go func() {
		for {
			st, err := server.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			go func(st *Stream) {
				buf := make([]byte, 4096)
				for {
					n, err := st.Read(buf)
					if n > 0 {
						if _, werr := st.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						_ = st.CloseWrite()
						return
					}
				}
			}(st)
		}
	}()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "echo.local:1"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	msg := bytes.Repeat([]byte("t2s2 stream data "), 100) // 1.7KiB
	go func() {
		if _, err := st.Write(msg); err != nil {
			t.Errorf("write: %v", err)
		}
		_ = st.CloseWrite()
	}()

	got := make([]byte, 0, len(msg))
	buf := make([]byte, 4096)
	deadline := time.After(5 * time.Second)
	for len(got) < len(msg) {
		select {
		case <-deadline:
			t.Fatalf("echo incomplete: %d/%d", len(got), len(msg))
		default:
		}
		n, err := st.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch: %d bytes, sha %x vs %x", len(got),
			sha256.Sum256(got), sha256.Sum256(msg))
	}
	// after echo + our FIN + server FIN: Read must observe EOF
	if _, err := st.Read(buf); err != io.EOF {
		t.Fatalf("final read err = %v, want EOF", err)
	}
	_ = st.Close()
	_ = client.Close()
	_ = server.Close()
}

// TestMuxConcurrentStreams: 8 streams, each moving 256KiB with an
// independent hash, over one session. Verifies isolation and flow control.
func TestMuxConcurrentStreams(t *testing.T) {
	const streams = 8
	const size = 256 * 1024

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

	// Serve only after the handshake: the handshake owns the transport
	// synchronously, then hands it to the single reader/writer loops.
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()

	// server: accept and echo each stream
	go func() {
		for {
			st, err := server.Accept()
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
						return
					}
				}
			}(st)
		}
	}()

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
			st, err := client.Open(Target{Network: NetworkTCP, Address: "stress.local:1"})
			if err != nil {
				results <- result{id: i, err: err}
				return
			}
			defer st.Close()
			payload := make([]byte, size)
			for j := range payload {
				payload[j] = byte(i + j)
			}
			want := sha256.Sum256(payload)

			go func() {
				_, _ = st.Write(payload)
				_ = st.CloseWrite()
			}()

			got := make([]byte, 0, size)
			buf := make([]byte, 16*1024)
			for len(got) < size {
				n, err := st.Read(buf)
				if n > 0 {
					got = append(got, buf[:n]...)
				}
				if err != nil {
					// The deferred Close() races the tail of the echo: once
					// the full payload is verified, a peer RESET for our
					// local close is expected, not a failure.
					if len(got) >= size {
						break
					}
					results <- result{id: i, err: err}
					return
				}
			}
			results <- result{id: i, sum: sha256.Sum256(got), err: nil}
			_ = want
		}(i)
	}
	wg.Wait()
	close(results)

	for r := range results {
		if r.err != nil {
			t.Fatalf("stream %d: %v", r.id, r.err)
		}
		payload := make([]byte, size)
		for j := range payload {
			payload[j] = byte(r.id + j)
		}
		if r.sum != sha256.Sum256(payload) {
			t.Fatalf("stream %d: hash mismatch (data crossed streams)", r.id)
		}
	}
	_ = client.Close()
	_ = server.Close()
}

// TestMuxTransportLoss: closing the transport fails every stream with a
// clear error and ends both Serve loops — no silent hang.
func TestMuxTransportLoss(t *testing.T) {
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

	// Serve only after the handshake: the handshake owns the transport
	// synchronously, then hands it to the single reader/writer loops.
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()

	// server accepts but never echoes (its goroutine will die with the pipe)
	go func() {
		for {
			st, err := server.Accept()
			if err != nil {
				return
			}
			_ = st
		}
	}()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "loss.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close() // transport dies

	// the blocked reader must unblock with an error, not hang
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := st.Read(buf)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil || err == io.EOF {
			t.Fatalf("read after transport loss = %v, want error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream read hung after transport loss")
	}

	// both sessions must end
	for _, sess := range []*Session{client, server} {
		select {
		case <-sess.Done():
		case <-time.After(3 * time.Second):
			t.Fatal("session did not end after transport loss")
		}
	}
	// writes on the dead stream fail, not block forever
	wErr := make(chan error, 1)
	go func() {
		_, err := st.Write(make([]byte, 1024))
		wErr <- err
	}()
	select {
	case err := <-wErr:
		if err == nil {
			t.Fatal("write on dead stream succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write hung on dead stream")
	}
}

// TestMuxCloseFlushesWrittenData: a Write that returned nil must reach the
// peer even when Close follows immediately. Regression for the scheduler
// dropping a retired stream's queued DATA frames (the tail was lost, the
// peer saw a short transfer then RESET).
func TestMuxCloseFlushesWrittenData(t *testing.T) {
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

	// server: read everything to EOF, count the bytes
	size := 256 * 1024
	gotCh := make(chan int, 1)
	go func() {
		st, err := server.Accept()
		if err != nil {
			gotCh <- -1
			return
		}
		n := 0
		buf := make([]byte, 32*1024)
		for {
			rn, err := st.Read(buf)
			n += rn
			if err != nil {
				break
			}
		}
		gotCh <- n
	}()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "flush.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, size)
	for j := range payload {
		payload[j] = byte(j)
	}
	if _, err := st.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = st.Close() // immediately after Write: queued tail must still flush

	select {
	case n := <-gotCh:
		if n != size {
			t.Fatalf("peer got %d/%d bytes after Close (queued frames lost)", n, size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server reader hung")
	}
}

// TestMuxFlowControlBackpressure: a server that never reads must stop the
// client's writes (window exhaustion), then proceed once the server reads.
func TestMuxFlowControlBackpressure(t *testing.T) {
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

	// Serve only after the handshake: the handshake owns the transport
	// synchronously, then hands it to the single reader/writer loops.
	go func() { _ = client.Serve() }()
	go func() { _ = server.Serve() }()

	slowStart := make(chan struct{})
	serverStream := make(chan *Stream, 1)
	go func() {
		st, err := server.Accept()
		if err != nil {
			return
		}
		serverStream <- st
		<-slowStart // deliberately not reading until told
		// Pure sink: this test measures one-directional backpressure, so
		// the server must not echo (an echo would need the client to read
		// concurrently; that deadlock-by-design belongs to the fault
		// injection suite, not this test).
		buf := make([]byte, 8*1024)
		for {
			if _, err := st.Read(buf); err != nil {
				return
			}
		}
	}()

	st, err := client.Open(Target{Network: NetworkTCP, Address: "slow.local:1"})
	if err != nil {
		t.Fatal(err)
	}

	// Write more than the initial window: the writer must park.
	written := make(chan int, 1)
	go func() {
		n, _ := st.Write(make([]byte, InitialStreamWindow+64*1024))
		written <- n
	}()

	select {
	case <-written:
		t.Fatal("write completed despite unread peer: flow control not applied")
	case <-time.After(300 * time.Millisecond):
	}

	close(slowStart) // server starts reading; window replenishes

	select {
	case n := <-written:
		if n != InitialStreamWindow+64*1024 {
			t.Fatalf("wrote %d bytes, want %d", n, InitialStreamWindow+64*1024)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("backpressured write never completed after peer resumed")
	}
	_ = st.Close()
	_ = client.Close()
	_ = server.Close()
}
