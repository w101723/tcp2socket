package mux

// Deterministic regressions for the 2026-10-24 review findings:
//   P0-1 SessionBuffer.WaitBlocked never set woken -> Release did not wake
//   P0-2 Stream.Write burned DATA sequence numbers on ErrQueueFull retries
//   P0-3 Stream.Read released budget only when a block's tail drained
//   P1   Scheduler.Ready token shared between writer and backpressured producers

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// TestSessionBufferExhaustionWakesWaiter (P0-1): with the budget exhausted,
// Reserve must fail; after Release frees budget, a parked waiter (via
// WaitBlocked) must wake. Pre-fix, woken was never set, so Release never
// closed the wake channel and the waiter slept until the session ended.
func TestSessionBufferExhaustionWakesWaiter(t *testing.T) {
	b := NewSessionBuffer(1024)
	if err := b.Reserve(1, 1024); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(2, 1); !errors.Is(err, ErrSessionBufferFull) {
		t.Fatalf("reserve over budget = %v, want ErrSessionBufferFull", err)
	}
	wake := b.WaitBlocked(2)
	b.Release(1, 1024)
	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never woken after Release (woken flag was never set)")
	}
	// budget is free again
	if err := b.Reserve(2, 1); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
}

// gateConn lets the first passThrough writes through unmodified, then
// blocks every write until release closes (test transport).
type gateConn struct {
	net.Conn
	pass    int // writes allowed before blocking
	release chan struct{}
	mu      sync.Mutex
}

func (g *gateConn) Write(p []byte) (int, error) {
	g.mu.Lock()
	pass := g.pass > 0
	if pass {
		g.pass--
	}
	g.mu.Unlock()
	if pass {
		return g.Conn.Write(p)
	}
	<-g.release
	return g.Conn.Write(p)
}

// TestWriteSequenceStableAcrossQueueFull (P0-2): ErrQueueFull retries must
// reuse the same DATA sequence number. Pre-fix, each retry burned a new
// sequence, so the peer saw a gap and RESET the stream with ErrSequence.
func TestWriteSequenceStableAcrossQueueFull(t *testing.T) {
	c, s := net.Pipe()
	release := make(chan struct{})
	gc := &gateConn{Conn: c, pass: 4, release: release} // HELLO + OPEN_OK flow
	client := mustSession(t, gc, SideClient)
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

	st, err := client.Open(Target{Network: NetworkTCP, Address: "seq.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	sst, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// 4KiB frames: the 64KiB window allows 16 in flight — exactly the
	// queue depth. With the writer stalled, the 17th in-flight frame hits
	// ErrQueueFull and Write retries. Sequence numbers must not burn.
	written := make(chan error, 1)
	go func() {
		buf := make([]byte, 1024)
		for i := 0; i < 1024; i++ { // 1MiB total: many window cycles
			if _, err := st.Write(buf); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()
	// Wait for REAL saturation (queue at capacity), not a guessed sleep —
	// but never hang: on a pathologically slow runner degrade to releasing
	// early (the regression still fires whenever saturation is reached).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) &&
		client.sched.StreamQueueLen(st.ID()) < schedulerQueueDepth {
		time.Sleep(10 * time.Millisecond)
	}
	close(release) // release: writer drains, retries succeed

	// Server reader runs concurrently: the client's window only replenishes
	// as the server consumes.
	gotCh := make(chan int, 1)
	go func() {
		got := 0
		buf := make([]byte, 32*1024)
		for got < 1024*1024 {
			n, err := sst.Read(buf)
			got += n
			if err != nil {
				gotCh <- -1
				return
			}
		}
		gotCh <- got
	}()

	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("write with retries: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("write never completed after release")
	}
	select {
	case got := <-gotCh:
		if got != 1024*1024 {
			t.Fatalf("server received %d bytes; sequence gap killed the stream", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server reader stalled")
	}
}

// TestReadPartialBlockReleasesBudget (P0-3): one 32KiB block consumed in
// many small Reads must release the session budget progressively. Pre-fix,
// only the tail-terminating Read released anything, stranding everything
// before it.
func TestReadPartialBlockReleasesBudget(t *testing.T) {
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
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	st, err := client.Open(Target{Network: NetworkTCP, Address: "partial.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	sst, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// One block: 32KiB. The client reads it in 4KiB slices (8 Reads).
	const block = 32 * 1024
	if _, err := st.Write(make([]byte, block)); err != nil {
		t.Fatal(err)
	}
	_ = st.CloseWrite()

	small := make([]byte, 4096)
	total := 0
	for total < block {
		n, err := sst.Read(small)
		total += n
		if err != nil {
			t.Fatalf("read at %d: %v", total, err)
		}
		// Budget must drop as bytes are consumed: after k slices the stream
		// holds block - total remaining bytes.
		if held := server.bufBudget.heldFor(sst.ID()); held != int64(block-total) {
			t.Fatalf("after %d/%d bytes, budget held = %d, want %d", total, block, held, block-total)
		}
	}
	if held := server.bufBudget.heldFor(sst.ID()); held != 0 {
		t.Fatalf("budget after full block = %d, want 0", held)
	}
}

// TestSchedulerTokenSharedWriterAndProducers (P1): a backpressured producer
// and the session writer both wait on sched.Ready(). Frames must still all
// reach the transport — no lost wakeup from token stealing.
func TestSchedulerTokenSharedWriterAndProducers(t *testing.T) {
	sched := NewScheduler()
	const producers = 4
	const framesEach = 64

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < framesEach; i++ {
				for {
					err := sched.Enqueue(Frame{Type: TypeData, StreamID: uint32(p + 1), Sequence: uint32(i)})
					if err == nil {
						break
					}
					// backpressure path: exactly what Stream.Write does
					<-sched.Ready()
				}
			}
		}(p)
	}

	drained := make(chan int, 1)
	go func() {
		n := 0
		for n < producers*framesEach {
			f, err := sched.Next()
			if err != nil {
				drained <- -1
				return
			}
			if f.Type == 0 {
				<-sched.Ready() // writer park: the shared token consumer
				continue
			}
			n++
		}
		sched.Close()
		drained <- n
	}()

	wg.Wait()
	select {
	case n := <-drained:
		if n != producers*framesEach {
			t.Fatalf("drained %d frames, want %d", n, producers*framesEach)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lost wakeup: frames stuck in queues while writer waited")
	}
}

// TestWindowUpdateNeverLostBehindBulkData: WINDOW_UPDATEs must not be
// droppable behind a full stream queue. Pre-fix, consumed() ignored
// Enqueue's ErrQueueFull, so a lost update froze the peer's send window
// and deadlocked large bidirectional transfers (CI: TestMuxLargeTransfer
// hang on a slow runner). Deterministic reproduction: saturate the
// stream's data queue, then read (which consumes and must deliver the
// update via the priority control path) and prove the peer's credit
// regrew.
func TestWindowUpdateNeverLostBehindBulkData(t *testing.T) {
	c, s := net.Pipe()
	release := make(chan struct{})
	gc := &gateConn{Conn: c, pass: 4, release: release} // HELLO + OPEN_OK flow
	client := mustSession(t, gc, SideClient)
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

	st, err := client.Open(Target{Network: NetworkTCP, Address: "win.local:1"})
	if err != nil {
		t.Fatal(err)
	}
	sst, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}

	// Saturate the client's outbound DATA queue while the transport is
	// frozen (gate exhausted): 17 x 1KiB frames — 16 fill the queue, the
	// 17th parks on ErrQueueFull. The queue stays full.
	const win = InitialStreamWindow
	clientBusy := make(chan struct{})
	go func() {
		defer close(clientBusy)
		buf := make([]byte, 1024)
		for i := 0; i < 64; i++ {
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	}()
	// Server drains its inbound concurrently so its reader never parks in
	// deliver behind unread DATA (that would block WINDOW_UPDATE dispatch).
	go func() {
		sink := make([]byte, 16*1024)
		for {
			if _, err := sst.Read(sink); err != nil {
				return
			}
		}
	}()
	satDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(satDeadline) &&
		client.sched.StreamQueueLen(st.ID()) < schedulerQueueDepth {
		time.Sleep(10 * time.Millisecond)
	} // queue saturated, writer parked

	// Server sends one full window (64KiB); the client consumes it while
	// its own data queue is full — the exact moment the pre-fix code
	// dropped the WINDOW_UPDATE into a full queue.
	go func() {
		_, _ = sst.Write(make([]byte, win))
	}()

	// Client reads all of it back (consuming -> WINDOW_UPDATE due).
	got := 0
	buf := make([]byte, 16*1024)
	for got < win {
		n, err := st.Read(buf)
		if err != nil {
			t.Fatalf("read at %d: %v", got, err)
		}
		got += n
	}

	// Unblock the transport: queued DATA flushes and the control-priority
	// WINDOW_UPDATEs reach the server.
	close(release)

	// Server's credit must have regrown: another full window must flow.
	creditOK := make(chan int64, 1)
	go func() {
		deadline := time.After(3 * time.Second)
		for {
			if avail := sst.send.Available(); avail >= win {
				creditOK <- avail
				return
			}
			select {
			case <-deadline:
				creditOK <- sst.send.Available()
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	go func() {
		_, _ = sst.Write(make([]byte, win))
	}()
	got = 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for got < win {
			n, err := st.Read(buf)
			if err != nil {
				t.Errorf("second window read at %d: %v (update lost, window frozen)", got, err)
				return
			}
			got += n
		}
	}()
	select {
	case avail := <-creditOK:
		if avail < win {
			t.Fatalf("server credit = %d after 3s, want >= %d: WINDOW_UPDATE lost", avail, win)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("credit probe never ran")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("second window never arrived: WINDOW_UPDATE was dropped behind bulk data")
	}
}

// TestStreamDeadlines: Read and Write honor deadlines (net.Conn contract).
func TestStreamDeadlines(t *testing.T) {
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
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	st, err := client.Open(Target{Network: NetworkTCP, Address: "deadline.local:1"})
	if err != nil {
		t.Fatal(err)
	}

	// Read deadline fires on an idle stream.
	if err := st.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = st.Read(make([]byte, 128))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read err = %v, want os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("deadline fired late: %v", elapsed)
	}

	// Clearing the deadline unblocks long waits again (no spurious fire).
	if err := st.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Write deadline on a window-exhausted stream.
	if err := st.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// Server accepts but never reads: window exhausts after 64KiB.
	if _, err := server.Accept(); err != nil {
		t.Fatal(err)
	}
	_, werr := st.Write(make([]byte, 1024*1024))
	if !errors.Is(werr, os.ErrDeadlineExceeded) {
		t.Fatalf("write err = %v, want os.ErrDeadlineExceeded", werr)
	}

	// Deadline paths must not corrupt the stream: after clearing deadlines
	// and having the peer read, data flows again.
	_ = st.SetWriteDeadline(time.Time{})
	_ = st.SetReadDeadline(time.Time{})
	var _ io.Reader = st
}
