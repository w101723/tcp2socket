package control

import (
	"bytes"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"tcp2socket/internal/session"
)

var fastCfg = HeartbeatConfig{Interval: 10 * time.Millisecond, Timeout: 20 * time.Millisecond, MaxMissed: 2}

// startTestHeartbeat wires a heartbeat with a failure channel and a read loop
// feeding HandleFrame, mirroring the caller wiring production code will use.
func startTestHeartbeat(t *testing.T, conn net.Conn, cfg HeartbeatConfig) (*Heartbeat, <-chan error) {
	t.Helper()
	failed := make(chan error, 1)
	hb, err := StartHeartbeat(conn, testID, cfg, func(e error) { failed <- e })
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			f, err := ReadFrame(conn)
			if err != nil {
				return
			}
			if hb.HandleFrame(f) != nil {
				return
			}
		}
	}()
	return hb, failed
}

func expectHeartbeatFailure(t *testing.T, failed <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-failed:
		return err
	case <-time.After(within):
		t.Fatal("heartbeat did not fail within deadline")
		return nil
	}
}

func expectNoFailure(t *testing.T, failed <-chan error, within time.Duration) {
	t.Helper()
	select {
	case err := <-failed:
		t.Fatalf("unexpected heartbeat failure: %v", err)
	case <-time.After(within):
	}
}

// drainPings reads peer frames; reply, when set, runs for each PING received.
func drainPings(conn net.Conn, reply func(Frame)) {
	for {
		f, err := ReadFrame(conn)
		if err != nil {
			return
		}
		if reply != nil && f.Type == TypePing {
			reply(f)
		}
	}
}

func TestHeartbeatConfigValidation(t *testing.T) {
	bad := []HeartbeatConfig{
		{Interval: 0, Timeout: time.Second, MaxMissed: 1},
		{Interval: time.Second, Timeout: 0, MaxMissed: 1},
		{Interval: time.Second, Timeout: time.Second, MaxMissed: 0},
	}
	for _, cfg := range bad {
		rw := struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(nil), io.Discard}
		if _, err := StartHeartbeat(rw, testID, cfg, nil); !errors.Is(err, ErrInvalidHeartbeatConfig) {
			t.Fatalf("cfg %+v: err = %v", cfg, err)
		}
	}
	if err := DefaultHeartbeatConfig.validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

// TestHeartbeatTimeout: a peer that reads pings but never answers must be
// detected within a bounded window, with a single failure report.
func TestHeartbeatTimeout(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go drainPings(b, nil) // read pings, never reply

	hb, failed := startTestHeartbeat(t, a, fastCfg)
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, ErrHeartbeatTimeout) {
		t.Fatalf("err = %v, want ErrHeartbeatTimeout", err)
	}
	select {
	case <-hb.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not exit after failure")
	}
	hb.Close()
}

// TestDuplicatePong: the second PONG for the same sequence is stale evidence
// of desync and must fail the heartbeat immediately.
func TestDuplicatePong(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	replied := false
	go drainPings(b, func(f Frame) {
		if replied {
			return
		}
		replied = true
		pong := Frame{Type: TypePong, SessionID: testID, Sequence: f.Sequence}
		_ = WriteFrame(b, pong)
		_ = WriteFrame(b, pong) // duplicate
	})

	hb, failed := startTestHeartbeat(t, a, fastCfg)
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, ErrStalePong) {
		t.Fatalf("err = %v, want ErrStalePong", err)
	}
	hb.Close()
}

func TestUnsolicitedPong(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		_ = WriteFrame(b, Frame{Type: TypePong, SessionID: testID, Sequence: 999})
	}()
	hb, failed := startTestHeartbeat(t, a, fastCfg)
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, ErrStalePong) {
		t.Fatalf("err = %v, want ErrStalePong", err)
	}
	hb.Close()
}

// TestHeartbeatForeignSession: frames from an old or foreign session must not
// influence this heartbeat; they terminate it instead.
func TestHeartbeatForeignSession(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go func() {
		_ = WriteFrame(b, Frame{Type: TypePong, SessionID: staleID, Sequence: 1})
	}()
	hb, failed := startTestHeartbeat(t, a, fastCfg)
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, ErrSessionIDMismatch) {
		t.Fatalf("err = %v, want ErrSessionIDMismatch", err)
	}
	hb.Close()
}

// TestHeartbeatPeerEOF: transport loss surfaces as a write failure on the
// next ping, reported exactly once.
func TestHeartbeatPeerEOF(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	go drainPings(b, nil)
	hb, failed := startTestHeartbeat(t, a, fastCfg)
	_ = a.Close() // transport gone
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err = %v, want ErrClosedPipe", err)
	}
	select {
	case <-hb.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not exit after transport loss")
	}
}

// TestSimultaneousClose: two live heartbeats closing at the same time must
// both reach Done, report no failure and leave no goroutines behind.
func TestSimultaneousClose(t *testing.T) {
	a, b := net.Pipe()
	hbA, failedA := startTestHeartbeat(t, a, fastCfg)
	hbB, failedB := startTestHeartbeat(t, b, fastCfg)

	expectNoFailure(t, failedA, 150*time.Millisecond)
	expectNoFailure(t, failedB, time.Millisecond)

	before := runtime.NumGoroutine()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); hbA.Close() }()
	go func() { defer wg.Done(); hbB.Close() }()
	wg.Wait()
	_ = a.Close()
	_ = b.Close()

	for _, hb := range []*Heartbeat{hbA, hbB} {
		select {
		case <-hb.Done():
		case <-time.After(time.Second):
			t.Fatal("heartbeat did not stop after Close")
		}
		if err := hb.Err(); err != nil {
			t.Fatalf("clean close reported failure: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}

// TestWriteStall: a peer that stops reading must not stall the heartbeat
// state machine. The plan's single-writer contract strands only the writer
// goroutine on the blocked write; missed-pong detection keeps running and
// fails the heartbeat on budget, and hb.Close() stays responsive throughout.
// A peer that never reads never answers pings, so the detected failure is the
// timeout — the write error itself is covered by TestHeartbeatPeerEOF.
func TestWriteStall(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	hb, failed := startTestHeartbeat(t, a, HeartbeatConfig{Interval: 5 * time.Millisecond, Timeout: 20 * time.Millisecond, MaxMissed: 5})

	// The first ping write is now blocked on the unread pipe; detection must
	// continue regardless and bound the failure time.
	err := expectHeartbeatFailure(t, failed, 2*time.Second)
	if !errors.Is(err, ErrHeartbeatTimeout) {
		t.Fatalf("err = %v, want ErrHeartbeatTimeout", err)
	}
	select {
	case <-hb.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not exit while its write was stalled")
	}

	_ = a.Close() // unblock the stranded writer goroutine

	closed := make(chan struct{})
	go func() { hb.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked after transport close")
	}
}

// TestHeartbeatEchoLoop: two properly wired heartbeats answer each other and
// never fail; a clean shutdown reports no error.
func TestHeartbeatEchoLoop(t *testing.T) {
	a, b := net.Pipe()
	hbA, failedA := startTestHeartbeat(t, a, fastCfg)
	hbB, failedB := startTestHeartbeat(t, b, fastCfg)

	expectNoFailure(t, failedA, 200*time.Millisecond)
	expectNoFailure(t, failedB, time.Millisecond)

	hbA.Close()
	hbB.Close()
	_ = a.Close()
	_ = b.Close()
	for i, hb := range []*Heartbeat{hbA, hbB} {
		select {
		case <-hb.Done():
		case <-time.After(time.Second):
			t.Fatalf("heartbeat %d did not stop", i)
		}
		if err := hb.Err(); err != nil {
			t.Fatalf("heartbeat %d failed: %v", i, err)
		}
	}
}

// TestHeartbeatMarksSessionDegraded verifies the plan's failure contract end
// to end: heartbeat failure drives session READY -> DEGRADED, new work is
// refused, and no implicit recovery to READY exists.
func TestHeartbeatMarksSessionDegraded(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	go drainPings(b, nil)

	s, err := session.New()
	if err != nil {
		t.Fatal(err)
	}
	// DEGRADED is observable in the event stream: Close advances the snapshot
	// to CLOSING immediately, but the READY->DEGRADED edge is recorded forever
	// and no later event may return the session to READY.
	var sawDegraded bool
	s.OnEvent(func(e session.Event) {
		if e.Type == session.EventStateChanged && e.From == session.StateReady && e.To == session.StateDegraded {
			sawDegraded = true
		}
	})
	for _, next := range []session.State{session.StateConnecting, session.StateHandshaking, session.StateReady} {
		if err := s.Transition(next); err != nil {
			t.Fatal(err)
		}
	}
	failures := make(chan error, 1)
	hb, err := StartHeartbeat(a, testID, fastCfg, func(e error) {
		// Production wiring: mark degraded (recorded as a READY->DEGRADED
		// event), then close the session. Finish is the lifecycle teardown
		// step, not part of failure marking. No path returns to READY.
		// The channel send comes last so the receive observes every write
		// made here (happens-before edge).
		_ = s.Transition(session.StateDegraded)
		s.Close(session.ReasonTimeout)
		failures <- e
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failures:
		if !errors.Is(err, ErrHeartbeatTimeout) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not fail within deadline")
	}
	hb.Close()

	if !sawDegraded {
		t.Fatal("session never entered degraded state on heartbeat failure")
	}
	if s.Snapshot().State.CanAcceptWork() {
		t.Fatal("session after heartbeat failure must refuse new work")
	}
	// Explicit lifecycle teardown: closing -> closed, exactly once.
	s.Close(session.ReasonTimeout)
	if err := s.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if s.Snapshot().State != session.StateClosed {
		t.Fatalf("state = %s, want closed", s.Snapshot().State)
	}
	select {
	case <-s.Done():
	case <-time.After(time.Second):
		t.Fatal("session Done did not fire after Finish")
	}
}
