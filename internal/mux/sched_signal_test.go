package mux

import (
	"testing"
	"time"
)

// TestSchedulerSignalNoLoss: after Next() returns a frame without re-signal
// and a later Enqueue's signal is "dropped" (token slot occupied), the
// writer must still wake and drain the new frame.
func TestSchedulerSignalNoLoss(t *testing.T) {
	s := NewScheduler()
	defer s.Close()

	_ = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 1, Payload: []byte("a")})
	// writer wakes, takes the frame (token may remain or be consumed)
	f1, err := s.Next()
	if err != nil || f1.StreamID != 1 {
		t.Fatalf("first: %+v %v", f1, err)
	}
	// writer writes f1; meanwhile a second frame arrives
	_ = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 2, Payload: []byte("b")})
	// writer parks on Ready
	select {
	case <-s.Ready():
	case <-time.After(time.Second):
		t.Fatal("writer would sleep forever: wakeup lost")
	}
	f2, err := s.Next()
	if err != nil || f2.StreamID != 1 || f2.Sequence != 2 {
		t.Fatalf("second: %+v %v", f2, err)
	}
}

// TestSchedulerSignalAfterEmptyNext: Next() returns empty, then enqueue.
func TestSchedulerSignalAfterEmptyNext(t *testing.T) {
	s := NewScheduler()
	defer s.Close()
	if f, err := s.Next(); err != nil || f.Type != 0 {
		t.Fatalf("empty: %+v %v", f, err)
	}
	_ = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 1})
	select {
	case <-s.Ready():
	case <-time.After(time.Second):
		t.Fatal("no wakeup after empty Next + enqueue")
	}
	if f, err := s.Next(); err != nil || f.Sequence != 1 {
		t.Fatalf("drain: %+v %v", f, err)
	}
}
