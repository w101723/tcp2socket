package mux

import (
	"errors"
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
// 1 MiB total. A stream that keeps filling while its consumer does not
// read is the violator; the scheduler RESETs it, not the session.
type SessionBuffer struct {
	mu      sync.Mutex
	used    int64
	max     int64
	perWait map[uint32]chan struct{}
}

func NewSessionBuffer(max int64) *SessionBuffer {
	if max <= 0 {
		max = MaxSessionBuffer
	}
	return &SessionBuffer{max: max, perWait: make(map[uint32]chan struct{})}
}

var ErrSessionBufferFull = errors.New("mux session receive buffer exhausted")

// Reserve books n bytes for stream id. It fails when the aggregate limit
// would be exceeded, naming the caller so the session can apply backpressure
// to exactly that stream.
func (b *SessionBuffer) Reserve(id uint32, n int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+int64(n) > b.max {
		return ErrSessionBufferFull
	}
	b.used += int64(n)
	return nil
}

// Release returns booked bytes (application read them).
func (b *SessionBuffer) Release(id uint32, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= int64(n)
	if b.used < 0 {
		b.used = 0
	}
	if ch, ok := b.perWait[id]; ok {
		delete(b.perWait, id)
		close(ch)
	}
}

// WaitBlocked parks a stream whose Reserve failed until the buffer drains
// or the session closes. Returns false if the session is gone.
func (b *SessionBuffer) WaitBlocked(id uint32) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.perWait == nil {
		b.perWait = make(map[uint32]chan struct{})
	}
	if _, ok := b.perWait[id]; !ok {
		b.perWait[id] = make(chan struct{})
	}
	return b.perWait[id]
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
	control chan Frame            // session-scoped frames, priority
	order   []uint32              // round-robin ring
	pos     int
	budget  int // bytes per stream per turn
	closed  bool
}

const (
	schedulerQueueDepth = 16
	schedulerBudget     = 2 * MaxFramePayload
)

func NewScheduler() *Scheduler {
	return &Scheduler{
		queues:  make(map[uint32]chan Frame),
		control: make(chan Frame, schedulerQueueDepth),
		budget:  schedulerBudget,
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
		return nil
	default:
		return errors.New("mux control queue full")
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

// RemoveStream drops a finished stream from the ring. Its queued frames are
// abandoned (the stream is closed; nothing reads them anymore).
func (s *Scheduler) RemoveStream(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
			// advance past this stream; drop it from the ring when its
			// queue is empty and closed-out (caller removes via RemoveStream)
			s.pos = (start + i + 1) % max(len(s.order), 1)
			s.mu.Unlock()
			return f, nil
		default:
		}
	}
	// nothing ready this turn
	s.mu.Lock()
	s.pos = (start + 1) % max(len(s.order), 1)
	s.mu.Unlock()
	if s.closed {
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

// Close stops new enqueues. Queued frames may still be drained.
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

func (s *Scheduler) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
