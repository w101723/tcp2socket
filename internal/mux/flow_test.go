package mux

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSendCredit(t *testing.T) {
	c := NewSendCredit(100)
	if got := c.Acquire(60); got != 60 {
		t.Fatalf("acquire 60 = %d", got)
	}
	if got := c.Acquire(60); got != 40 { // only what remains
		t.Fatalf("acquire 60 over 40 = %d", got)
	}
	if got := c.Acquire(10); got != 0 {
		t.Fatalf("acquire when empty = %d", got)
	}
	if err := c.Grant(50); err != nil {
		t.Fatal(err)
	}
	if c.Available() != 50 {
		t.Fatalf("avail = %d, want 50", c.Available())
	}
	if err := c.Grant(uint32(MaxWindowDelta)); err == nil {
		t.Fatal("window overflow not detected")
	}
	c.Close()
	if got := c.Acquire(10); got != 0 {
		t.Fatal("acquire after close")
	}
}

// TestSendCreditWaitWakesOnGrant: a blocked sender must be woken by a grant.
func TestSendCreditWaitWakesOnGrant(t *testing.T) {
	c := NewSendCredit(10)
	_ = c.Acquire(10) // exhaust
	wait := c.Wait()
	select {
	case <-wait:
		t.Fatal("wait fired before grant")
	case <-time.After(20 * time.Millisecond):
	}
	if err := c.Grant(10); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Wait():
		// fresh channel; the old one is closed too
	case <-wait:
	case <-time.After(time.Second):
		t.Fatal("grant did not wake the waiter")
	}
}

func TestSessionBuffer(t *testing.T) {
	b := NewSessionBuffer(1000)
	if err := b.Reserve(1, 600); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(2, 300); err != nil {
		t.Fatal(err)
	}
	if err := b.Reserve(3, 200); !errors.Is(err, ErrSessionBufferFull) {
		t.Fatalf("overrun = %v, want ErrSessionBufferFull", err)
	}
	if b.Used() != 900 {
		t.Fatalf("used = %d, want 900", b.Used())
	}
	b.Release(1, 600)
	if b.Used() != 300 {
		t.Fatalf("used after release = %d, want 300", b.Used())
	}
	if err := b.Reserve(3, 200); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerFairness: one hot stream and one slow stream must share the
// wire — a burst of hot-stream frames must not consume all slots before the
// slow stream sends anything.
func TestSchedulerFairness(t *testing.T) {
	s := NewScheduler()
	defer s.Close()

	hot := uint32(1)
	slow := uint32(3)
	payload := make([]byte, 1024)
	for i := 0; i < 8; i++ {
		if err := s.Enqueue(Frame{Type: TypeData, StreamID: hot, Sequence: uint32(i + 1), Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Enqueue(Frame{Type: TypeData, StreamID: slow, Sequence: 1, Payload: payload}); err != nil {
		t.Fatal(err)
	}

	var order []uint32
	for i := 0; i < 9; i++ {
		f, err := s.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if f.Type == 0 {
			t.Fatalf("no frame at %d though queues are stocked", i)
		}
		order = append(order, f.StreamID)
	}
	// The slow stream's frame must appear within the first few picks, not
	// after all 8 hot frames (round-robin, one per visit).
	slowSeenAt := -1
	for i, id := range order {
		if id == slow {
			slowSeenAt = i
			break
		}
	}
	if slowSeenAt < 0 || slowSeenAt > 3 {
		t.Fatalf("slow stream served at position %d (order %v): hot stream monopolized the wire", slowSeenAt, order)
	}
}

// TestSchedulerControlPriority: control frames jump the data queue.
func TestSchedulerControlPriority(t *testing.T) {
	s := NewScheduler()
	defer s.Close()
	for i := 0; i < 4; i++ {
		_ = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: uint32(i + 1), Payload: []byte("x")})
	}
	if err := s.EnqueueControl(Frame{Type: TypeGoaway, Payload: []byte{byte(ErrCodeShutdown)}}); err != nil {
		t.Fatal(err)
	}
	f, err := s.Next()
	if err != nil || f.Type != TypeGoaway {
		t.Fatalf("first out = %+v err=%v, want GOAWAY", f, err)
	}
}

// TestSchedulerBoundedQueues: a full stream queue backpressures its
// producer instead of buffering without bound.
func TestSchedulerBoundedQueues(t *testing.T) {
	s := NewScheduler()
	defer s.Close()
	var err error
	for i := 0; i < schedulerQueueDepth; i++ {
		if err = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: uint32(i + 1)}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if err = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 99}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow enqueue = %v, want ErrQueueFull", err)
	}
	// drain one slot, enqueue succeeds again
	if _, e := s.Next(); e != nil {
		t.Fatal(e)
	}
	if err = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 99}); err != nil {
		t.Fatal(err)
	}
}

// TestSchedulerRemoveAndClose: a retired stream's queued frames are still
// flushed in order (Write-must-not-lose-data); close drains then reports
// terminal.
func TestSchedulerRemoveAndClose(t *testing.T) {
	s := NewScheduler()
	_ = s.Enqueue(Frame{Type: TypeData, StreamID: 1, Sequence: 1})
	s.RemoveStream(1)
	// retired queue drains before the scheduler reports empty
	f, err := s.Next()
	if err != nil || f.StreamID != 1 {
		t.Fatalf("retired queue must still flush: %+v %v", f, err)
	}
	f, err = s.Next()
	if err != nil || f.Type != 0 {
		t.Fatalf("after drain: %+v %v", f, err)
	}
	_ = s.Enqueue(Frame{Type: TypeData, StreamID: 3, Sequence: 1})
	s.Close()
	f, err = s.Next()
	if err != nil || f.StreamID != 3 {
		t.Fatalf("closed must still drain: %+v %v", f, err)
	}
	if _, err = s.Next(); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("drained closed = %v, want ErrSchedulerClosed", err)
	}
	if err := s.Enqueue(Frame{Type: TypeData, StreamID: 5, Sequence: 1}); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("enqueue after close = %v", err)
	}
}

// TestSchedulerConcurrent: parallel producers plus one consumer under
// -race; every enqueued frame must come out exactly once.
func TestSchedulerConcurrent(t *testing.T) {
	s := NewScheduler()
	defer s.Close()
	const producers = 4
	const perProducer = 200
	var sent struct {
		mu sync.Mutex
		m  map[uint32]int
	}
	sent.m = make(map[uint32]int)

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			id := uint32(p*2 + 1)
			for i := 0; i < perProducer; i++ {
				for {
					err := s.Enqueue(Frame{Type: TypeData, StreamID: id, Sequence: uint32(i + 1), Payload: []byte{byte(p)}})
					if err == nil {
						break
					}
					if errors.Is(err, ErrSchedulerClosed) {
						return
					}
					// queue full: the consumer is behind; retry with a yield
					time.Sleep(50 * time.Microsecond)
					continue
				}
				sent.mu.Lock()
				sent.m[id]++
				sent.mu.Unlock()
			}
		}(p)
	}

	received := make(map[uint32]int)
	deadline := time.After(5 * time.Second)
	total := producers * perProducer
	for got := 0; got < total; {
		select {
		case <-deadline:
			t.Fatalf("received only %d/%d frames", got, total)
		default:
		}
		f, err := s.Next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if f.Type == 0 {
			time.Sleep(100 * time.Microsecond)
			continue
		}
		received[f.StreamID]++
		got++
	}
	wg.Wait()
	for id, n := range received {
		if n != perProducer {
			t.Fatalf("stream %d: received %d, want %d", id, n, perProducer)
		}
	}
}
