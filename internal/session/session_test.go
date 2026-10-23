package session

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestStateTransition(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []State{StateConnecting, StateHandshaking, StateReady} {
		if err := s.Transition(next); err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
	}
	if s.Snapshot().State != StateReady {
		t.Fatalf("state = %s, want ready", s.Snapshot().State)
	}
	s.Close(ReasonEOF)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.State != StateClosed || snap.Reason != ReasonEOF {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestInvalidTransition(t *testing.T) {
	s, _ := New()
	if err := s.Transition(StateReady); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("init -> ready: %v", err)
	}
	if err := s.Transition(StateClosed); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("init -> closed: %v", err)
	}
	if err := s.Transition(StateConnecting); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(StateInit); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("connecting -> init: %v", err)
	}
	s.Close(ReasonTimeout)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(StateConnecting); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed session transition: %v", err)
	}
}

func TestDegradedCannotRecover(t *testing.T) {
	s, _ := New()
	for _, next := range []State{StateConnecting, StateHandshaking, StateReady, StateDegraded} {
		if err := s.Transition(next); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Transition(StateReady); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("degraded -> ready must fail: %v", err)
	}
	s.Close(ReasonTimeout)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentClose(t *testing.T) {
	s, _ := New()
	s.Transition(StateConnecting)
	closedEvents := 0
	var mu sync.Mutex
	s.OnEvent(func(e Event) {
		if e.Type == EventClosed {
			mu.Lock()
			closedEvents++
			mu.Unlock()
		}
	})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Close(ReasonTransport)
			_ = s.Finish()
		}()
	}
	wg.Wait()
	s.Wait(context.Background())
	snap := s.Snapshot()
	if snap.State != StateClosed {
		t.Fatalf("state = %s", snap.State)
	}
	if closedEvents != 1 {
		t.Fatalf("closed events = %d, want 1", closedEvents)
	}
	if snap.Reason != ReasonTransport {
		t.Fatalf("reason = %s", snap.Reason)
	}
}

func TestCancelDuringConnect(t *testing.T) {
	s, _ := New()
	if err := s.Transition(StateConnecting); err != nil {
		t.Fatal(err)
	}
	ctxCanceled := make(chan struct{})
	go func() {
		<-s.Done()
		close(ctxCanceled)
	}()
	s.Close(ReasonCanceled)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctxCanceled:
	case <-time.After(time.Second):
		t.Fatal("session did not finish after cancel")
	}
	if s.Snapshot().Reason != ReasonCanceled {
		t.Fatalf("reason = %s", s.Snapshot().Reason)
	}
}

func TestCancelDuringHandshake(t *testing.T) {
	s, _ := New()
	if err := s.Transition(StateConnecting); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(StateHandshaking); err != nil {
		t.Fatal(err)
	}
	s.Close(ReasonTimeout)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.State != StateClosed || snap.Reason != ReasonTimeout {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestCloseUnblocksWait(t *testing.T) {
	s, _ := New()
	waited := make(chan error, 1)
	go func() { waited <- s.Wait(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("wait returned before close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	s.Close(ReasonEOF)
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait not unblocked by close")
	}
}

func TestWaitContext(t *testing.T) {
	s, _ := New()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait ctx err = %v", err)
	}
	s.Close(ReasonEOF)
	_ = s.Finish()
}

func TestMaxSessions(t *testing.T) {
	m := NewManager(2)
	s1, err := m.New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.New(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.New(); !errors.Is(err, ErrCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	s1.Close(ReasonEOF)
	if err := s1.Finish(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for m.Count() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.Count() != 1 {
		t.Fatalf("count after close = %d", m.Count())
	}
	if _, err := m.New(); err != nil {
		t.Fatalf("capacity not released: %v", err)
	}
	m.CloseAll(ReasonCanceled)
}

func TestEventCallbackReentrancy(t *testing.T) {
	var s *Session
	var err error
	s, err = New()
	if err != nil {
		t.Fatal(err)
	}
	s.OnEvent(func(e Event) {
		_ = s.Snapshot()
		if e.Type == EventStateChanged && e.To == StateReady {
			s.Close(ReasonProtocol)
			_ = s.Finish()
		}
	})
	if err := s.Transition(StateConnecting); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(StateHandshaking); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(StateReady); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx); err != nil {
		t.Fatalf("reentrant close deadlocked: %v", err)
	}
	if s.Snapshot().Reason != ReasonProtocol {
		t.Fatalf("reason = %s", s.Snapshot().Reason)
	}
}

func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 200; i++ {
		s, err := New()
		if err != nil {
			t.Fatal(err)
		}
		s.Close(ReasonEOF)
		if err := s.Finish(); err != nil {
			t.Fatal(err)
		}
		if err := s.Wait(context.Background()); err != nil {
			t.Fatal(err)
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

// TestConcurrentLifecycleEventOrdering is the post-review race closure.
// Many goroutines drive the lifecycle while observers record events; with
// the pre-fix code the emit path re-read s.updatedAt after releasing the
// lock, which -race flags against concurrent Transition/Close/Finish and
// Snapshot writers. The asserted contract:
//
//  1. per-session event Seq values are exactly 1..N with no gaps, and
//  2. the sequence reconstructed by Seq is a legal state path ending CLOSED
//     with non-decreasing At timestamps.
//
// Emission happens after the lock is released, so observers may receive
// events out of transition order under contention; Seq is the authoritative
// order and the timestamp is a lock-scoped local, never a shared re-read.
func TestConcurrentLifecycleEventOrdering(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []Event
	s.OnEvent(func(e Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})
	if err := s.Transition(StateConnecting); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				switch (i + j) % 4 {
				case 0:
					_ = s.Transition(StateHandshaking)
				case 1:
					_ = s.Transition(StateReady)
				case 2:
					_ = s.Snapshot()
				default:
					_ = s.Transition(StateDegraded)
				}
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Blocks until Finish closes done; also exercises Wait racing
			// concurrent transitions.
			_ = s.Wait(context.Background())
		}()
	}
	close(start)
	time.Sleep(50 * time.Millisecond) // let the racers interleave
	s.Close(ReasonCanceled)
	if err := s.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	wg.Wait()

	if state := s.Snapshot().State; state != StateClosed {
		t.Fatalf("state = %s, want closed", state)
	}
	if err := s.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 3 {
		t.Fatalf("too few events recorded: %d", len(events))
	}
	// The created event (Seq 1) fired inside New, before OnEvent registered
	// this observer; deliveries may otherwise be out of order. Reconstruct
	// by Seq: the first observed event must be Seq 2 (the connecting
	// transition) and the run must be gap-free from there.
	sorted := make([]Event, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Seq < sorted[j].Seq })
	if sorted[0].Seq != 2 || sorted[0].Type != EventStateChanged || sorted[0].To != StateConnecting {
		t.Fatalf("first observed event = {Seq %d %s %s->%s}, want Seq 2 state_changed init->connecting",
			sorted[0].Seq, sorted[0].Type, sorted[0].From, sorted[0].To)
	}
	for i, e := range sorted {
		if e.Seq != uint64(i+2) {
			t.Fatalf("event %d has Seq %d, want exactly %d (gap or duplicate)", i, e.Seq, i+2)
		}
		if i > 0 && e.At.Before(sorted[i-1].At) {
			t.Fatalf("event %d timestamp %v precedes earlier event %v", i, e.At, sorted[i-1].At)
		}
		if i > 0 && sorted[i].Type != EventClosed && sorted[i].From != sorted[i-1].To {
			t.Fatalf("event %d (%s) From=%s does not chain from previous To=%s",
				i, e.Type, sorted[i].From, sorted[i-1].To)
		}
	}
	if last := sorted[len(sorted)-1]; last.Type != EventClosed || last.To != StateClosed {
		t.Fatalf("path must end with a closed event: last={%s %s->%s}", last.Type, last.From, last.To)
	}
}
