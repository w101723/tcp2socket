package control

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	ErrHeartbeatTimeout       = errors.New("control heartbeat timeout")
	ErrStalePong              = errors.New("stale or unsolicited control pong")
	ErrHeartbeatClosed        = errors.New("control heartbeat closed")
	ErrInvalidHeartbeatConfig = errors.New("invalid control heartbeat config")
)

// HeartbeatConfig bounds failure detection. These defaults follow the plan
// (interval 10s, per-pong timeout 5s, 3 consecutive misses); tests use small
// values. Detection latency is bounded by roughly Interval*MaxMissed.
type HeartbeatConfig struct {
	Interval  time.Duration
	Timeout   time.Duration
	MaxMissed int
}

var DefaultHeartbeatConfig = HeartbeatConfig{
	Interval:  10 * time.Second,
	Timeout:   5 * time.Second,
	MaxMissed: 3,
}

func (c HeartbeatConfig) validate() error {
	if c.Interval <= 0 || c.Timeout <= 0 || c.MaxMissed < 1 {
		return fmt.Errorf("%w: interval=%v timeout=%v max_missed=%d", ErrInvalidHeartbeatConfig, c.Interval, c.Timeout, c.MaxMissed)
	}
	return nil
}

// frameWriter is the plan's "single writer goroutine": every frame toward the
// peer is funneled through one bounded queue and one goroutine that owns the
// underlying writer. Heartbeat pings, pong replies and future control writes
// therefore never interleave on the stream, and a blocked write can never
// stall the read loop — reads keep flowing, which is what prevents the
// synchronous-transport write/write deadlock. A failed writer records its
// first error, reports it once via onFail, drops further frames and stops.
type frameWriter struct {
	w    io.Writer
	ch   chan Frame
	quit chan struct{}
	done chan struct{}

	mu       sync.Mutex
	closed   bool
	writeErr error
	onFail   func(error)
}

const writerQueueDepth = 16

func newFrameWriter(w io.Writer, onFail func(error)) *frameWriter {
	fw := &frameWriter{
		w:      w,
		ch:     make(chan Frame, writerQueueDepth),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
		onFail: onFail,
	}
	go fw.run()
	return fw
}

func (fw *frameWriter) run() {
	defer close(fw.done)
	failing := false
	for {
		select {
		case <-fw.quit:
			return
		case f := <-fw.ch:
			if failing {
				continue // frames are worthless once the stream errored
			}
			if err := WriteFrame(fw.w, f); err != nil {
				fw.mu.Lock()
				if fw.writeErr == nil {
					fw.writeErr = fmt.Errorf("control write: %w", err)
					cb := fw.onFail
					fw.mu.Unlock()
					if cb != nil {
						cb(fw.writeErr)
					}
				} else {
					fw.mu.Unlock()
				}
			}
		}
	}
}

func (fw *frameWriter) enqueue(f Frame) error {
	fw.mu.Lock()
	if fw.closed {
		err := fw.callerErr()
		fw.mu.Unlock()
		return err
	}
	fw.mu.Unlock()
	select {
	case fw.ch <- f:
		return nil
	case <-fw.done:
		fw.mu.Lock()
		defer fw.mu.Unlock()
		return fw.callerErr()
	}
}

func (fw *frameWriter) callerErr() error {
	if fw.writeErr != nil {
		return fw.writeErr
	}
	return ErrHeartbeatClosed
}

// close stops accepting frames and ends the writer goroutine. It does not
// wait for a write blocked on a stalled transport; close the transport first
// (the same contract as transport.Stream.Close).
func (fw *frameWriter) close() {
	fw.mu.Lock()
	if fw.closed {
		fw.mu.Unlock()
		return
	}
	fw.closed = true
	fw.mu.Unlock()
	close(fw.quit)
}

// Heartbeat exchanges PING/PONG over an established, READY control session.
// It detects a dead or stalled peer and reports failure exactly once via
// onFail; marking the session DEGRADED and closing the transport is the
// caller's job (see the session integration test). Start it only after a
// successful handshake: READY is a handshake-level property and this type
// stays decoupled from the session state machine.
type Heartbeat struct {
	sessionID [16]byte
	cfg       HeartbeatConfig
	writer    *frameWriter
	onFail    func(error)

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}

	mu          sync.Mutex
	outstanding map[uint32]time.Time
	nextSeq     uint32
	missed      int
	failed      bool
	failErr     error
}

// StartHeartbeat begins the ping loop. It sends the first PING immediately.
func StartHeartbeat(rw io.ReadWriter, sessionID [16]byte, cfg HeartbeatConfig, onFail func(error)) (*Heartbeat, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	h := &Heartbeat{
		sessionID:   sessionID,
		cfg:         cfg,
		onFail:      onFail,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		outstanding: make(map[uint32]time.Time),
	}
	h.writer = newFrameWriter(rw, func(err error) { h.fail(err) })
	go h.loop()
	return h, nil
}

// Done closes when the loop has exited, whether by failure or Close.
func (h *Heartbeat) Done() <-chan struct{} { return h.done }

// Err returns the failure reason, or nil when the heartbeat stopped cleanly.
func (h *Heartbeat) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failErr
}

// Close stops the heartbeat without invoking onFail. It does not wait for a
// write that is blocked on a stalled transport; close the transport first.
func (h *Heartbeat) Close() {
	h.stopOnce.Do(func() {
		close(h.stop)
		h.writer.close()
	})
}

func (h *Heartbeat) fail(err error) {
	h.mu.Lock()
	if h.failed {
		h.mu.Unlock()
		return
	}
	h.failed = true
	h.failErr = err
	h.mu.Unlock()
	h.writer.close()
	if h.onFail != nil {
		h.onFail(err)
	}
}

func (h *Heartbeat) loop() {
	defer close(h.done)
	if !h.tick() {
		return
	}
	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-ticker.C:
			if !h.tick() {
				return
			}
		}
	}
}

// tick expires unanswered pings, checks the consecutive-miss budget and sends
// the next ping. It returns false once the heartbeat has stopped.
func (h *Heartbeat) tick() bool {
	// All shared state is read under the lock and captured as locals; the
	// fail path below runs unlocked and a concurrent valid PONG may already
	// be resetting h.missed.
	h.mu.Lock()
	now := time.Now()
	for seq, sent := range h.outstanding {
		if now.Sub(sent) >= h.cfg.Timeout {
			delete(h.outstanding, seq)
			h.missed++
		}
	}
	fail := h.missed >= h.cfg.MaxMissed
	missed := h.missed
	var seq uint32
	if !fail {
		h.nextSeq++
		seq = h.nextSeq
		h.outstanding[seq] = now
	}
	h.mu.Unlock()

	if fail {
		h.fail(fmt.Errorf("%w: %d consecutive missed pong(s), max %d", ErrHeartbeatTimeout, missed, h.cfg.MaxMissed))
		return false
	}
	if err := h.writer.enqueue(Frame{Type: TypePing, SessionID: h.sessionID, Sequence: seq}); err != nil {
		if !errors.Is(err, ErrHeartbeatClosed) {
			h.fail(err)
		}
		return false
	}
	return true
}

// HandleFrame processes PING and PONG frames from the session read loop.
// PINGs are answered with an echoing PONG; PONGs must reference an
// outstanding sequence and the negotiated session ID. A duplicate, stale or
// unsolicited PONG, or any foreign session ID, fails the heartbeat
// immediately: it indicates desync or cross-session contamination. Other
// frame types are ignored here; the caller owns ERROR/CLOSE handling.
func (h *Heartbeat) HandleFrame(f Frame) error {
	if err := CheckSession(h.sessionID, f); err != nil {
		werr := fmt.Errorf("heartbeat: %w", err)
		h.fail(werr)
		return werr
	}
	switch f.Type {
	case TypePing:
		if err := h.writer.enqueue(Frame{Type: TypePong, SessionID: h.sessionID, Sequence: f.Sequence}); err != nil {
			if !errors.Is(err, ErrHeartbeatClosed) {
				h.fail(err)
			}
			return err
		}
		return nil
	case TypePong:
		h.mu.Lock()
		_, ok := h.outstanding[f.Sequence]
		if ok {
			delete(h.outstanding, f.Sequence)
			h.missed = 0
		}
		h.mu.Unlock()
		if !ok {
			err := fmt.Errorf("%w: sequence %d not outstanding", ErrStalePong, f.Sequence)
			h.fail(err)
			return err
		}
		return nil
	default:
		return nil
	}
}
