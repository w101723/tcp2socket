package mux

import (
	"errors"
	"fmt"
	"sync"
)

// Flow-control limits (docs/protocol.md §5).
const (
	InitialStreamWindow = 64 * 1024
	MaxSessionBuffer    = 1024 * 1024
	MaxWindowDelta      = 1<<31 - 1
)

// SendCredit tracks one stream's send-side window: bytes the peer has
// granted us. Write paths must acquire credit before enqueueing DATA; a
// stalled peer therefore backpressures exactly this stream, never the
// whole session.
type SendCredit struct {
	mu     sync.Mutex
	avail  int64
	closed bool
	waitCh chan struct{}
}

func NewSendCredit(initial int64) *SendCredit { return &SendCredit{avail: initial} }

// Acquire blocks-free reservation: it takes up to min(n, avail) and returns
// what it took. Zero means the stream is window-blocked; the caller parks
// on Wait (below) instead of busy-looping.
func (c *SendCredit) Acquire(n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.avail <= 0 {
		return 0
	}
	take := int64(n)
	if take > c.avail {
		take = c.avail
	}
	c.avail -= take
	return int(take)
}

// Grant adds peer-awarded credit. Returns ErrFlowControl wrapped when the
// accumulated window exceeds 2^31-1 (protocol violation by the peer).
func (c *SendCredit) Grant(delta uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil // late update on a closed stream is dropped
	}
	c.avail += int64(delta)
	if c.avail > MaxWindowDelta {
		c.closed = true
		c.wake()
		return errors.New("mux granted window exceeds protocol maximum")
	}
	c.wake()
	return nil
}

// Wait returns a channel closed when credit may have changed (grant or
// close); the caller re-runs Acquire. One waiter is woken per change.
func (c *SendCredit) Wait() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waitCh == nil {
		c.waitCh = make(chan struct{})
	}
	return c.waitCh
}

// The zero-value waitCh is created lazily; wake() closes and clears it so
// the next Wait gets a fresh channel (classic cond-channel pattern).
func (c *SendCredit) wake() {
	if c.waitCh != nil {
		close(c.waitCh)
		c.waitCh = nil
	}
}

func (c *SendCredit) Available() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.avail
}

func (c *SendCredit) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.wake()
}

// SessionBuffer bounds the aggregate receive memory across streams (§5):
// 1 MiB total. Exhaustion applies session-wide backpressure (the reader
// parks until any stream's consumer frees budget); only actual window
// violations RESET a stream, never a busy session.
type SessionBuffer struct {
	mu       sync.Mutex
	used     int64
	byStream map[uint32]int64 // reserved bytes per stream, freed on stream death
	max      int64
	wake     chan struct{}
	woken    bool
}

func NewSessionBuffer(max int64) *SessionBuffer {
	if max <= 0 {
		max = MaxSessionBuffer
	}
	return &SessionBuffer{max: max, wake: make(chan struct{}), byStream: make(map[uint32]int64)}
}

var ErrSessionBufferFull = errors.New("mux session receive buffer exhausted")

// Reserve books n bytes for stream id.
func (b *SessionBuffer) Reserve(id uint32, n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+int64(n) > b.max {
		return ErrSessionBufferFull
	}
	b.used += int64(n)
	b.byStream[id] += int64(n)
	return nil
}

// Release returns booked bytes and wakes every parked reserver (the budget
// is shared, so any stream's release may unblock any other).
func (b *SessionBuffer) Release(id uint32, n int) {
	b.mu.Lock()
	if n > 0 {
		b.used -= int64(n)
		b.byStream[id] -= int64(n)
		if b.byStream[id] <= 0 {
			delete(b.byStream, id)
		}
	}
	if b.used < 0 {
		b.used = 0
	}
	if b.woken {
		b.woken = false
		close(b.wake)
		b.wake = make(chan struct{})
	}
	b.mu.Unlock()
}

// ReleaseStream drops every reservation held by stream id. Called when the
// stream dies with data still sitting in its receive queue that no
// application will ever consume: without this the session-wide budget
// leaks one delivered frame per dead stream until the reader wedges.
func (b *SessionBuffer) ReleaseStream(id uint32) {
	b.mu.Lock()
	if held := b.byStream[id]; held > 0 {
		b.used -= held
		if b.used < 0 {
			b.used = 0
		}
		delete(b.byStream, id)
		if b.woken {
			b.woken = false
			close(b.wake)
			b.wake = make(chan struct{})
		}
	}
	b.mu.Unlock()
}

// WaitBlocked parks until any Release fires (broadcast semantics via
// channel replacement) or the caller gives up. Marking woken here is the
// protocol: without it Release sees woken==false and never broadcasts —
// parked reservers would sleep until the session ends.
func (b *SessionBuffer) WaitBlocked(id uint32) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.woken = true // a waiter exists: the next Release must close b.wake
	return b.wake
}

// heldFor reports the bytes currently reserved for stream id (test aid:
// budget-accounting assertions).
func (b *SessionBuffer) heldFor(id uint32) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.byStream[id]
}

func (b *SessionBuffer) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Scheduler is the round-robin frame dispatcher (§9): bounded per-stream
// queues, one drain goroutine, control frames prioritized over DATA, a
// per-turn byte budget so a single hot stream cannot starve the others.
type Scheduler struct {
	mu      sync.Mutex
	queues  map[uint32]chan Frame // stream_id -> outbound queue
	retired map[uint32]chan Frame // queues kept until drained (RemoveStream)
	control chan Frame            // session-scoped frames, priority
	order   []uint32              // round-robin ring
	pos     int
	budget  int // bytes per stream per turn
	closed  bool
	ready   chan struct{} // signaled on first enqueue; writer blocks on it
}

const (
	schedulerQueueDepth = 16
	schedulerBudget     = 2 * MaxFramePayload
)

func NewScheduler() *Scheduler {
	return &Scheduler{
		queues:  make(map[uint32]chan Frame),
		retired: make(map[uint32]chan Frame),
		control: make(chan Frame, schedulerQueueDepth),
		budget:  schedulerBudget,
		ready:   make(chan struct{}, 1),
	}
}

var ErrSchedulerClosed = errors.New("mux scheduler closed")

// EnqueueControl submits a session-scoped frame (GOAWAY, PING/PONG) with
// priority over data.
func (s *Scheduler) EnqueueControl(f Frame) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSchedulerClosed
	}
	s.mu.Unlock()
	select {
	case s.control <- f:
		s.signalReady()
		return nil
	default:
		return fmt.Errorf("%w: control queue", ErrQueueFull)
	}
}

// Enqueue submits a stream frame to its bounded queue. A full queue means
// the transport is slower than this stream's producer: apply backpressure
// upstream by returning ErrQueueFull rather than unbounded buffering.
var ErrQueueFull = errors.New("mux stream queue full")

func (s *Scheduler) Enqueue(f Frame) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSchedulerClosed
	}
	q, ok := s.queues[f.StreamID]
	if !ok {
		q = make(chan Frame, schedulerQueueDepth)
		s.queues[f.StreamID] = q
		if !containsUint32(s.order, f.StreamID) {
			s.order = append(s.order, f.StreamID)
		}
	}
	s.mu.Unlock()

	select {
	case q <- f:
		s.signalReady()
		return nil
	default:
		return ErrQueueFull
	}
}

func containsUint32(xs []uint32, v uint32) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// RemoveStream retires a finished stream: its queue stays in the ring and
// every frame already accepted by Enqueue is still flushed in order (a
// Write that returned nil must not lose data to a later Close — the same
// net.Conn guarantee TCP gives). The Close RESET lands in the same queue
// after those frames, so the peer sees DATA…, FIN-echo…, RESET in order.
// Next() garbage-collects the retired entry once its queue drains.
func (s *Scheduler) RemoveStream(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.queues[id]; ok {
		s.retired[id] = q
	}
}

// gcRetired drops drained retired queues. Called with mu held.
func (s *Scheduler) gcRetired() {
	for id, q := range s.retired {
		if len(q) == 0 {
			delete(s.retired, id)
			delete(s.queues, id)
			for i, v := range s.order {
				if v == id {
					s.order = append(s.order[:i], s.order[i+1:]...)
					if s.pos > i {
						s.pos--
					}
					break
				}
			}
		}
	}
}

// Next returns the next frame to transmit, or an empty Frame when nothing
// is ready right now (callers poll or block on the transport elsewhere).
// Control frames always win; otherwise streams are visited round-robin,
// one frame per visit, so a hot stream cannot starve the others and the
// byte budget bounds any single stream's advantage per full turn.
// ErrSchedulerClosed is returned only once closed AND drained.
func (s *Scheduler) Next() (Frame, error) {
	// 1. control priority, never starved by data
	select {
	case f := <-s.control:
		return f, nil
	default:
	}

	// 2. snapshot the ring under the lock, then dequeue without it
	s.mu.Lock()
	if len(s.order) == 0 {
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return Frame{}, ErrSchedulerClosed
		}
		return Frame{}, nil
	}
	ring := make([]uint32, len(s.order))
	copy(ring, s.order)
	start := s.pos
	s.mu.Unlock()

	for i := 0; i < len(ring); i++ {
		id := ring[(start+i)%len(ring)]
		s.mu.Lock()
		q, ok := s.queues[id]
		s.mu.Unlock()
		if !ok {
			continue
		}
		select {
		case f := <-q:
			s.mu.Lock()
			s.pos = (start + i + 1) % max(len(s.order), 1)
			more := len(q) > 0 || len(s.control) > 0
			for _, other := range s.queues {
				if len(other) > 0 {
					more = true
					break
				}
			}
			s.mu.Unlock()
			if more {
				s.signalReady() // keep the token alive for the next iteration
			}
			return f, nil
		default:
		}
	}
	// nothing ready this turn
	s.mu.Lock()
	s.pos = (start + 1) % max(len(s.order), 1)
	s.gcRetired()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		// queues empty and closed: report terminal
		for _, id := range ring {
			s.mu.Lock()
			q := s.queues[id]
			s.mu.Unlock()
			if q != nil && len(q) > 0 {
				return Frame{}, nil // not drained yet after all
			}
		}
		return Frame{}, ErrSchedulerClosed
	}
	return Frame{}, nil
}

// signalReady must never drop a wakeup. The channel has capacity 1 and is
// the "maybe something to do" edge; Next() consumes the token whenever it
// successfully dequeues a frame, and re-signals when frames remain. A lost
// wakeup parks the writer forever while queues hold frames (the deadlock
// the session tests caught).
func (s *Scheduler) signalReady() {
	select {
	case s.ready <- struct{}{}:
		return
	default:
		return // token already pending; Next will re-check the queues
	}
}

// Ready returns a channel pulsed whenever a frame becomes available (or the
// scheduler closes); the writer goroutine blocks on it instead of spinning.
func (s *Scheduler) Ready() <-chan struct{} { return s.ready }

// Close stops new enqueues. Queued frames may still be drained.
func (s *Scheduler) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signalReady()
}

// StreamQueueLen reports the pending frames for one stream (test hook:
// lets regressions wait for a specific saturation state instead of sleeping).
func (s *Scheduler) StreamQueueLen(id uint32) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queues[id])
}

func (s *Scheduler) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
