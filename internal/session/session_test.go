package session

import (
	"context"
	"errors"
	"runtime"
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
