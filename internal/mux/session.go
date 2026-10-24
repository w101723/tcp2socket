package mux

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Session assembles the frozen T2S2 pieces into a usable transport: one
// reader goroutine (the only reader), one writer goroutine (the only
// writer, fed by the Scheduler), a stream registry and flow control. It
// exposes stream-open/accept on top of any io.ReadWriteCloser that is
// already an authenticated, encrypted channel (protocol §1).
type Session struct {
	epoch     [16]byte
	side      Side
	transport io.ReadWriteCloser

	reg       *Registry
	sched     *Scheduler
	bufBudget *SessionBuffer

	mu       sync.Mutex
	streams  map[uint32]*Stream
	nextID   uint32
	closed   bool
	closeErr error
	serving  bool

	acceptCh chan *Stream
	done     chan struct{}

	// receive-side state per stream lives in the Stream's pipe.
}

var (
	ErrSessionClosed = errors.New("mux session closed")
	ErrNotReady      = errors.New("mux session not ready")
	ErrStreamClosed  = errors.New("mux stream closed")
)

// SessionConfig tunes a Session. Zero values select the protocol defaults.
type SessionConfig struct {
	MaxStreams    int
	SessionBuffer int64
	InitialWindow int64
}

// NewSession builds a session over an established transport. side picks the
// stream ID domain (client odd / server even). The client seeds a random
// epoch carried in its HELLO; the server adopts it from the HELLO during
// Handshake, so both directions share one epoch (protocol §2).
func NewSession(transport io.ReadWriteCloser, side Side, cfg SessionConfig) (*Session, error) {
	if transport == nil {
		return nil, errors.New("mux session requires a transport")
	}
	var epoch [16]byte
	if _, err := rand.Read(epoch[:]); err != nil {
		return nil, fmt.Errorf("mux epoch: %w", err)
	}
	window := cfg.InitialWindow
	if window <= 0 {
		window = InitialStreamWindow
	}
	s := &Session{
		epoch:     epoch,
		side:      side,
		transport: transport,
		reg:       NewRegistry(side, cfg.MaxStreams),
		sched:     NewScheduler(),
		bufBudget: NewSessionBuffer(cfg.SessionBuffer),
		streams:   make(map[uint32]*Stream),
		nextID:    map[Side]uint32{SideClient: 1, SideServer: 2}[side],
		acceptCh:  make(chan *Stream, 16),
		done:      make(chan struct{}),
	}
	return s, nil
}

// Handshake performs the HELLO/HELLO_ACK exchange (§7.1). It owns the
// transport synchronously and must complete BEFORE Serve: starting the
// reader first would let it consume the peer's handshake frames. Both sides
// run Handshake concurrently; the client sends first.
func (s *Session) Handshake(offer []byte, require []byte) (accepted []byte, err error) {
	s.mu.Lock()
	ready := s.serving
	s.mu.Unlock()
	if ready {
		return nil, errors.New("mux Handshake must run before Serve")
	}
	caps, err := EncodeCapabilities(offer)
	if err != nil {
		return nil, err
	}
	var hello Frame
	if s.side == SideClient {
		hello = Frame{Type: TypeHello, Epoch: s.epoch, Payload: caps}
		if err := WriteFrame(s.transport, hello); err != nil {
			return nil, fmt.Errorf("mux send HELLO: %w", err)
		}
		ack, err := ReadFrame(s.transport)
		if err != nil {
			return nil, fmt.Errorf("mux read HELLO_ACK: %w", err)
		}
		if ack.Type != TypeHelloAck {
			return nil, fmt.Errorf("mux handshake: expected HELLO_ACK, got %s", ack.Type)
		}
		// One epoch for both directions (§2): the client generated it, the
		// ACK must echo it; a mismatching ACK is a protocol violation.
		if ack.Epoch != s.epoch {
			return nil, fmt.Errorf("mux handshake: HELLO_ACK epoch mismatch")
		}
		got, err := ParseCapabilities(ack.Payload)
		if err != nil {
			return nil, fmt.Errorf("mux handshake ack: %w", err)
		}
		if err := subset(require, got); err != nil {
			return nil, err
		}
		return got, nil
	}
	// server: read HELLO, answer ACK
	h, err := ReadFrame(s.transport)
	if err != nil {
		return nil, fmt.Errorf("mux read HELLO: %w", err)
	}
	if h.Type != TypeHello {
		return nil, fmt.Errorf("mux handshake: expected HELLO, got %s", h.Type)
	}
	// Adopt the client's epoch: one epoch for both directions (§2).
	s.epoch = h.Epoch
	got, err := ParseCapabilities(h.Payload)
	if err != nil {
		return nil, fmt.Errorf("mux handshake hello: %w", err)
	}
	if err := subset(require, got); err != nil {
		return nil, err
	}
	// echo back the requested set that we accept (v1: everything offered
	// that we know; unknown capabilities are not accepted)
	acceptedCaps := intersect(knownCapabilities(), got)
	ackPayload, err := EncodeCapabilities(acceptedCaps)
	if err != nil {
		return nil, err
	}
	if err := WriteFrame(s.transport, Frame{Type: TypeHelloAck, Epoch: s.epoch, Payload: ackPayload}); err != nil {
		return nil, fmt.Errorf("mux send HELLO_ACK: %w", err)
	}
	if err := subset(require, acceptedCaps); err != nil {
		return nil, err
	}
	return acceptedCaps, nil
}

func subset(require, got []byte) error {
	have := make(map[byte]bool, len(got))
	for _, c := range got {
		have[c] = true
	}
	for _, c := range require {
		if !have[c] {
			return fmt.Errorf("mux peer lacks required capability 0x%02x", c)
		}
	}
	return nil
}

func intersect(a, b []byte) []byte {
	set := make(map[byte]bool, len(a))
	for _, c := range a {
		set[c] = true
	}
	var out []byte
	for _, c := range b {
		if set[c] {
			out = append(out, c)
		}
	}
	return out
}

func knownCapabilities() []byte {
	return []byte{CapStreamMux, CapHeartbeat}
}

// serving flags that Serve has started (guards Handshake ordering).
func (s *Session) markServing() {
	s.mu.Lock()
	s.serving = true
	s.mu.Unlock()
}

// Serve starts the reader and writer loops. It blocks until the session
// ends and returns the terminal error (nil on clean GOAWAY/close).
func (s *Session) Serve() error {
	s.markServing()
	writerDone := make(chan error, 1)
	go func() { writerDone <- s.writeLoop() }()
	err := s.readLoop()
	// reader ended (EOF, error or local close): tear everything down
	s.teardown(err)
	s.sched.Close()
	werr := <-writerDone
	if err == nil {
		err = werr
	}
	return err
}

// writeLoop is the single writer goroutine: drains the scheduler onto the
// transport (§9). It never holds stream-table locks and blocks on the
// scheduler's ready signal instead of spinning.
func (s *Session) writeLoop() error {
	for {
		f, err := s.sched.Next()
		if err != nil { // ErrSchedulerClosed after drain
			return nil
		}
		if f.Type == 0 {
			// nothing ready: park until a new frame or session end
			select {
			case <-s.sched.Ready():
			case <-s.done:
				return nil
			}
			continue
		}
		if err := WriteFrame(s.transport, f); err != nil {
			s.teardown(err)
			return err
		}
	}
}

// readLoop is the single reader goroutine.
func (s *Session) readLoop() error {
	for {
		f, err := ReadFrame(s.transport)
		if err != nil {
			return err
		}
		if f.Epoch != s.epoch {
			return fmt.Errorf("mux stale epoch frame (type %s)", f.Type)
		}
		if err := s.dispatch(f); err != nil {
			return err
		}
	}
}

// dispatch applies one inbound frame. Stream violations RESET the stream;
// framing/epoch violations tear down the session.
func (s *Session) dispatch(f Frame) error {
	switch f.Type {
	case TypeOpen:
		return s.handleOpen(f)
	case TypeOpenOk:
		if err := s.reg.OpenAck(f.StreamID); err != nil {
			return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
		}
		s.wakeOpenWaiter(f.StreamID, nil)
		return nil
	case TypeOpenErr:
		e, perr := ParseErrorPayload(f.Payload)
		if perr != nil {
			return fmt.Errorf("mux OPEN_ERR payload: %w", perr)
		}
		_ = s.reg.Close(f.StreamID)
		s.wakeOpenWaiter(f.StreamID, fmt.Errorf("mux open rejected: %s: %s", e.Code, sanitizeDetail(e.Detail)))
		return nil
	case TypeData:
		return s.handleData(f)
	case TypeWindowUpdate:
		if err := s.reg.CheckWindowUpdate(f.StreamID, f.Sequence); err != nil {
			return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
		}
		delta, err := ParseWindowUpdate(f.Payload)
		if err != nil {
			return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
		}
		if st := s.streamOf(f.StreamID); st != nil {
			if err := st.send.Grant(delta); err != nil {
				return s.resetStream(f.StreamID, ErrCodeFlowControl, err.Error())
			}
		}
		return nil
	case TypeFin:
		st := s.streamOf(f.StreamID)
		if err := s.reg.RemoteFin(f.StreamID); err != nil {
			if errors.Is(err, ErrGhostStream) {
				return s.resetStream(f.StreamID, ErrCodeStreamState, "ghost FIN")
			}
			// duplicate FIN: tolerated once per protocol §7.2 (ignore-and-log)
			return nil
		}
		if st != nil {
			st.remoteEOF()
		}
		return nil
	case TypeReset:
		e, _ := ParseErrorPayload(f.Payload)
		_ = s.reg.Close(f.StreamID)
		if st := s.streamOf(f.StreamID); st != nil {
			st.fail(fmt.Errorf("mux stream reset by peer: %s: %s", e.Code, sanitizeDetail(e.Detail)))
		}
		return nil
	case TypePing:
		return s.sched.EnqueueControl(Frame{Type: TypePong, Epoch: s.epoch})
	case TypePong:
		return nil // heartbeat consumers handle this in v1.4 wiring
	case TypeGoaway:
		e, _ := ParseErrorPayload(f.Payload)
		return fmt.Errorf("mux peer GOAWAY: %s: %s", e.Code, sanitizeDetail(e.Detail))
	default:
		return fmt.Errorf("mux unexpected frame type %s", f.Type)
	}
}

func (s *Session) handleOpen(f Frame) error {
	tg, err := ParseTarget(f.Payload)
	if err != nil {
		return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
	}
	if err := s.reg.AcceptOpen(f.StreamID, InitialStreamWindow); err != nil {
		return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
	}
	st := newStream(s, f.StreamID, tg)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.streams[f.StreamID] = st
	s.mu.Unlock()

	// The protocol requires an explicit OPEN_OK/OPEN_ERR answer (§7.2).
	// v1 accepts structurally valid opens unconditionally; the dialer
	// wiring in the CLI integration layer owns target policy and will
	// convert this into per-target OPEN_ERR decisions.
	if err := s.sched.Enqueue(Frame{Type: TypeOpenOk, Epoch: s.epoch, StreamID: f.StreamID}); err != nil {
		s.dropStream(f.StreamID)
		payload, _ := ErrorPayload{Code: ErrCodeShutdown, Detail: "session shutting down"}.Encode()
		_ = s.sched.Enqueue(Frame{Type: TypeOpenErr, Epoch: s.epoch, StreamID: f.StreamID, Payload: payload})
		return nil
	}
	st.openResolved(nil)

	// Hand the stream to the application without blocking the reader: a
	// slow accept loop must not wedge the session. Overflow closes the
	// stream politely.
	select {
	case s.acceptCh <- st:
		return nil
	default:
		s.dropStream(f.StreamID)
		payload, _ := ErrorPayload{Code: ErrCodeShutdown, Detail: "accept backlog full"}.Encode()
		return s.sched.Enqueue(Frame{Type: TypeOpenErr, Epoch: s.epoch, StreamID: f.StreamID, Payload: payload})
	}
}

func (s *Session) handleData(f Frame) error {
	remaining, err := s.reg.CheckData(f.StreamID, f.Sequence, len(f.Payload))
	if err != nil {
		if errors.Is(err, ErrGhostStream) {
			return s.resetStream(f.StreamID, ErrCodeStreamState, "ghost DATA")
		}
		return s.resetStream(f.StreamID, ErrCodeStreamState, err.Error())
	}
	if remaining < 0 {
		return s.resetStream(f.StreamID, ErrCodeFlowControl, "window overrun")
	}
	// Session aggregate budget (§5): apply backpressure first — park the
	// reader until other streams' consumers free budget or the stream dies.
	// A stream is RESET only for actual window violations (checked above),
	// never merely for sharing a busy session.
	for {
		err := s.bufBudget.Reserve(f.StreamID, len(f.Payload))
		if err == nil {
			break
		}
		wait := s.bufBudget.WaitBlocked(f.StreamID)
		select {
		case <-wait:
		case <-s.done:
			return ErrSessionClosed
		}
	}
	st := s.streamOf(f.StreamID)
	if st == nil {
		s.bufBudget.Release(f.StreamID, len(f.Payload))
		return s.resetStream(f.StreamID, ErrCodeStreamState, "data for untracked stream")
	}
	if err := st.deliver(f.Payload); err != nil {
		if errors.Is(err, ErrStreamClosed) {
			return nil // stream died while delivering: already failed locally
		}
		return s.resetStream(f.StreamID, ErrCodeFlowControl, err.Error())
	}
	return nil
}

func (s *Session) streamOf(id uint32) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[id]
}

func (s *Session) wakeOpenWaiter(id uint32, err error) {
	if st := s.streamOf(id); st != nil {
		st.openResolved(err)
	}
}

// resetStream sends a RESET for a stream-level protocol violation and
// fails the local handle.
func (s *Session) resetStream(id uint32, code ErrorCode, detail string) error {
	payload, _ := ErrorPayload{Code: code, Detail: detail}.Encode()
	frame := Frame{Type: TypeReset, Epoch: s.epoch, StreamID: id, Payload: payload}
	if err := s.sched.Enqueue(frame); err != nil && !errors.Is(err, ErrSchedulerClosed) {
		return err
	}
	if st := s.streamOf(id); st != nil {
		st.fail(fmt.Errorf("mux stream %d: %s", id, detail))
	}
	_ = s.reg.Close(id)
	return nil
}

// teardown closes every stream and marks the session ended. Idempotent.
func (s *Session) teardown(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.closeErr = err
	streams := make([]*Stream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.streams = make(map[uint32]*Stream)
	close(s.done)
	s.mu.Unlock()

	for _, id := range s.reg.CloseAll() {
		_ = id
	}
	for _, st := range streams {
		if err != nil {
			st.fail(fmt.Errorf("mux session ended: %w", err))
		} else {
			st.remoteEOF()
			st.fail(io.EOF)
		}
	}
}

// Done reports session termination.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns the terminal error after Done.
func (s *Session) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}

// Accept blocks for the next peer-initiated stream.
func (s *Session) Accept() (*Stream, error) {
	select {
	case st := <-s.acceptCh:
		return st, nil
	case <-s.done:
		return nil, ErrSessionClosed
	}
}

// Open initiates a stream toward the target described by tg. It blocks
// until OPEN_OK/OPEN_ERR or the session ends.
func (s *Session) Open(tg Target) (*Stream, error) {
	payload, err := tg.Encode()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	id := s.nextID
	s.nextID += 2
	s.mu.Unlock()

	if err := s.reg.Open(id, InitialStreamWindow); err != nil {
		return nil, err
	}
	st := newStream(s, id, Target{})
	s.mu.Lock()
	s.streams[id] = st
	s.mu.Unlock()

	if err := s.sched.Enqueue(Frame{Type: TypeOpen, Epoch: s.epoch, StreamID: id, Payload: payload}); err != nil {
		s.dropStream(id)
		return nil, err
	}
	if err := st.waitOpen(); err != nil {
		s.dropStream(id)
		return nil, err
	}
	st.target = tg
	return st, nil
}

func (s *Session) dropStream(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
	_ = s.reg.Close(id)
	s.sched.RemoveStream(id)
}

// Close sends GOAWAY, closes the transport and ends the session; Serve
// returns shortly. Idempotent.
func (s *Session) Close() error {
	payload, _ := ErrorPayload{Code: ErrCodeShutdown, Detail: "local close"}.Encode()
	_ = s.sched.EnqueueControl(Frame{Type: TypeGoaway, Epoch: s.epoch, Payload: payload})
	err := s.transport.Close()
	s.teardown(nil)
	s.sched.Close()
	return err
}

// Stream is one logical T2S2 stream. It implements net.Conn-ish semantics
// over the session (half-close via CloseWrite supported on the wire).
//
// The receive side is a bounded buffer channel, not an io.Pipe: deliver
// must never block the session reader goroutine (protocol §9), so a
// stalled application backpressures through the flow-control window (its
// peer stops getting WINDOW_UPDATE) instead of wedging the whole session.
type Stream struct {
	session *Session
	id      uint32
	target  Target

	// Receive side: blocks queue with a byte budget. The budget equals the
	// peer's send window (InitialStreamWindow), so a peer honoring its
	// window can never overflow the queue; deliver() failing means a
	// window violation, and consumed() replenishes both the budget and the
	// peer's window with one WINDOW_UPDATE.
	recv    chan []byte
	recvCur []byte        // current block being consumed by Read (cursor)
	recvEOF chan struct{} // closed on FIN; may be closed at most once
	recvErr error
	recvMu  sync.Mutex

	send *SendCredit

	openOnce sync.Once
	openDone chan struct{}
	openErr  error

	writeMu sync.Mutex // one writer at a time on this stream

	dataMu  sync.Mutex
	dataSeq uint32
	winMu   sync.Mutex
	winSeq  uint32

	dead    chan struct{}
	deadErr error
	deadMu  sync.Mutex
}

// streamRecvSlots bounds the block queue. Byte volume is bounded by the
// window budget (recvUsed <= InitialStreamWindow); slots only keep the
// channel deep enough that 32KiB frames do not spin on the semaphore.
const streamRecvSlots = 4

func newStream(s *Session, id uint32, tg Target) *Stream {
	return &Stream{
		session:  s,
		id:       id,
		target:   tg,
		recv:     make(chan []byte, streamRecvSlots),
		recvEOF:  make(chan struct{}),
		send:     NewSendCredit(InitialStreamWindow),
		openDone: make(chan struct{}),
		dead:     make(chan struct{}),
	}
}

func (st *Stream) ID() uint32     { return st.id }
func (st *Stream) Target() Target { return st.target }

// Read blocks for peer data. Returns io.EOF after the peer's FIN with all
// data drained; returns the stream error if the stream/session died.
// The receive cursor consumes each buffered block possibly across several
// Read calls, so every delivered byte is released and window-updated
// exactly once. Read is single-consumer; the EOF channel is closed at most
// once (frames are processed in order, so FIN never races ahead of DATA).
func (st *Stream) Read(p []byte) (int, error) {
	for {
		// 1. continue the current block
		if len(st.recvCur) > 0 {
			n := copy(p, st.recvCur)
			st.recvCur = st.recvCur[n:]
			if len(st.recvCur) == 0 {
				st.consumed(n)
			}
			return n, nil
		}
		// 2. take the next block; EOF only after the queue drains
		select {
		case b := <-st.recv:
			n := copy(p, b)
			if n < len(b) {
				st.recvCur = b[n:] // tail consumed by later Read calls
			} else {
				st.consumed(n)
			}
			return n, nil
		default:
		}
		select {
		case b := <-st.recv:
			n := copy(p, b)
			if n < len(b) {
				st.recvCur = b[n:]
			} else {
				st.consumed(n)
			}
			return n, nil
		case <-st.recvEOF:
			if len(st.recv) > 0 { // in-flight blocks beat the EOF marker
				continue
			}
			// A clean FIN (no stream error) must surface as io.EOF, never
			// (0, nil): callers treat a nil-error zero-read as "no
			// progress yet" and would spin.
			if err := st.recvError(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		case <-st.dead:
			if len(st.recv) > 0 {
				continue // error only after buffered data is drained
			}
			if err := st.deadError(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
	}
}

// consumed releases the session budget and replenishes the peer's window
// for n fully-consumed bytes.
func (st *Stream) consumed(n int) {
	if n <= 0 {
		return
	}
	st.session.bufBudget.Release(st.id, n)
	// The registry window (the single ledger) regrows; the WINDOW_UPDATE
	// tells the peer its credit came back.
	_ = st.session.reg.GrowReceiveWindow(st.id, int64(n))
	_ = st.session.sched.Enqueue(Frame{
		Type: TypeWindowUpdate, Epoch: st.session.epoch, StreamID: st.id,
		Sequence: st.nextWinSeq(), Payload: WindowUpdatePayload(uint32(n)),
	})
}

func (st *Stream) recvError() error {
	st.recvMu.Lock()
	defer st.recvMu.Unlock()
	return st.recvErr
}

func (st *Stream) deadError() error {
	st.deadMu.Lock()
	defer st.deadMu.Unlock()
	return st.deadErr
}

// deliver feeds received payload into the stream's bounded receive buffer.
//
// Backpressure design: the registry's receive window is the single byte
// ledger (deducted in CheckData before deliver runs), so a peer honoring
// its granted credit can never overflow this stream. deliver therefore
// only blocks on the slot channel until the application reads; that parks
// the session reader briefly, which is the intended loop: the stalled
// stream stops issuing WINDOW_UPDATE, its peer's window runs dry, and the
// stall stays local while the session writer keeps flushing other streams.
func (st *Stream) deliver(b []byte) error {
	select {
	case st.recv <- b:
		return nil
	case <-st.dead:
		return ErrStreamClosed
	}
}

// Write splits p into window-granted chunks (never larger than
// MaxFramePayload) and enqueues DATA frames. It blocks while the peer's
// window is exhausted or the scheduler queue is full — backpressure, per
// net.Conn semantics.
func (st *Stream) Write(p []byte) (int, error) {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	total := len(p)
	for len(p) > 0 {
		select {
		case <-st.dead:
			return total - len(p), st.deadError()
		default:
		}
		n := st.send.Acquire(len(p))
		if n == 0 {
			wait := st.send.Wait()
			select {
			case <-wait:
			case <-st.dead:
				return total - len(p), st.deadError()
			}
			continue
		}
		// Frame-size alignment: one DATA frame never exceeds
		// MaxFramePayload, so a big grant is split and the unconsumed
		// remainder is returned to the window.
		if n > MaxFramePayload {
			st.send.Grant(uint32(n - MaxFramePayload))
			n = MaxFramePayload
		}
		chunk := p[:n]
		for {
			err := st.session.sched.Enqueue(Frame{
				Type: TypeData, Epoch: st.session.epoch, StreamID: st.id,
				Sequence: st.nextDataSeq(), Payload: append([]byte(nil), chunk...),
			})
			if err == nil {
				break
			}
			if errors.Is(err, ErrSchedulerClosed) || errors.Is(err, ErrSessionClosed) {
				st.send.Grant(uint32(n)) // credit not consumed after all
				return total - len(p), err
			}
			// ErrQueueFull: the transport is slower than this producer;
			// park until the writer drains a slot (bounded queue depth,
			// never unbounded buffering).
			select {
			case <-st.session.sched.Ready():
			case <-st.dead:
				st.send.Grant(uint32(n))
				return total - len(p), st.deadError()
			}
		}
		p = p[n:]
	}
	return total, nil
}

func (st *Stream) nextDataSeq() uint32 {
	st.dataMu.Lock()
	defer st.dataMu.Unlock()
	st.dataSeq++
	return st.dataSeq
}

func (st *Stream) nextWinSeq() uint32 {
	st.winMu.Lock()
	defer st.winMu.Unlock()
	st.winSeq++
	return st.winSeq
}

// CloseWrite sends FIN: the peer sees EOF after draining; we can still read.
func (st *Stream) CloseWrite() error {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	select {
	case <-st.dead:
		return st.deadError()
	default:
	}
	if err := st.session.reg.LocalFin(st.id); err != nil && !isAlreadyClosed(err) {
		return err
	}
	err := st.session.sched.Enqueue(Frame{Type: TypeFin, Epoch: st.session.epoch, StreamID: st.id})
	if err != nil && !isAlreadyClosed(err) {
		return err
	}
	return nil
}

func isAlreadyClosed(err error) bool {
	return errors.Is(err, ErrDuplicateFin) || errors.Is(err, ErrStreamClosed) ||
		errors.Is(err, ErrSchedulerClosed) || errors.Is(err, ErrSessionClosed)
}

// Close resets the stream if still open and releases local resources.
func (st *Stream) Close() error {
	st.deadMu.Lock()
	select {
	case <-st.dead:
		st.deadMu.Unlock()
		return nil
	default:
	}
	close(st.dead)
	st.deadErr = ErrStreamClosed
	st.deadMu.Unlock()

	st.send.Close()
	st.session.dropStream(st.id)

	// A stream already terminated by the peer (RESET received or transport
	// failure) is CLOSED per §7.2: answering a RESET with another RESET is
	// protocol noise that can race the peer's still-draining reads. Only a
	// locally-initiated close of a live stream announces a RESET.
	if st.peerTerminated() {
		return nil
	}
	payload, _ := ErrorPayload{Code: ErrCodeShutdown, Detail: "stream closed locally"}.Encode()
	_ = st.session.sched.Enqueue(Frame{Type: TypeReset, Epoch: st.session.epoch, StreamID: st.id, Payload: payload})
	return nil
}

// peerTerminated reports whether the stream ended by peer action (RESET or
// session failure) rather than a local decision.
func (st *Stream) peerTerminated() bool {
	st.recvMu.Lock()
	defer st.recvMu.Unlock()
	return st.recvErr != nil
}

// remoteEOF records the peer's FIN: readers drain the buffer then see EOF.
func (st *Stream) remoteEOF() {
	st.recvMu.Lock()
	defer st.recvMu.Unlock()
	select {
	case <-st.recvEOF:
		return // already closed
	default:
		close(st.recvEOF)
	}
}

// fail terminates the stream with an error. Even when the stream already
// died locally, the peer-termination fact must still be recorded: a later
// Close() relies on it to avoid answering a peer RESET with another RESET.
func (st *Stream) fail(err error) {
	st.deadMu.Lock()
	select {
	case <-st.dead:
		st.deadErr = err // peer termination observed after local close
		st.deadMu.Unlock()
		st.recvMu.Lock()
		if st.recvErr == nil {
			st.recvErr = err
		}
		select {
		case <-st.recvEOF:
		default:
			close(st.recvEOF)
		}
		st.recvMu.Unlock()
		st.send.Close()
		return
	default:
	}
	close(st.dead)
	st.deadErr = err
	st.deadMu.Unlock()
	st.send.Close()
	st.recvMu.Lock()
	if st.recvErr == nil {
		st.recvErr = err
	}
	select {
	case <-st.recvEOF:
	default:
		close(st.recvEOF)
	}
	st.recvMu.Unlock()
}

func (st *Stream) waitOpen() error {
	<-st.openDone
	return st.openErr
}

func (st *Stream) openResolved(err error) {
	st.openOnce.Do(func() {
		st.openErr = err
		close(st.openDone)
	})
}

// net.Conn compatibility (deadline stubs: the session transport owns real
// deadlines; per-stream deadlines arrive with the v0.4 dialer wiring).
func (st *Stream) LocalAddr() net.Addr                            { return muxAddr{} }
func (st *Stream) RemoteAddr() net.Addr                           { return muxAddr{} }
func (st *Stream) SetDeadline(t interface{ IsZero() bool }) error { return nil }
func (st *Stream) SetReadDeadline(t time.Time) error              { return nil }
func (st *Stream) SetWriteDeadline(t time.Time) error             { return nil }

type muxAddr struct{}

func (muxAddr) Network() string { return "t2s2" }
func (muxAddr) String() string  { return "t2s2" }
