package mux

import (
	"errors"
	"fmt"
	"sync"
)

// StreamState is the per-stream lifecycle from docs/protocol.md §7.2.
type StreamState uint8

const (
	StreamIdle StreamState = iota
	StreamOpening
	StreamOpen
	StreamHalfClosedLocal  // local FIN sent
	StreamHalfClosedRemote // remote FIN received
	StreamClosed
)

func (s StreamState) String() string {
	switch s {
	case StreamIdle:
		return "idle"
	case StreamOpening:
		return "opening"
	case StreamOpen:
		return "open"
	case StreamHalfClosedLocal:
		return "half_closed_local"
	case StreamHalfClosedRemote:
		return "half_closed_remote"
	case StreamClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(s))
	}
}

// Errors from the stream registry. All are local-side rejections of
// protocol violations; the caller decides between RESET and teardown.
var (
	ErrGhostStream       = errors.New("mux frame for unknown stream")
	ErrStreamIDParity    = errors.New("mux stream id has wrong parity for role")
	ErrStreamIDZero      = errors.New("mux stream id 0 is reserved for session frames")
	ErrStreamIDDuplicate = errors.New("mux stream id already in use")
	ErrDuplicateFin      = errors.New("mux duplicate FIN")
	ErrDuplicateReset    = errors.New("mux duplicate RESET")
	ErrDataAfterFin      = errors.New("mux DATA after FIN")
	ErrSequence          = errors.New("mux out-of-order or duplicate sequence")
)

// stream is the registry entry: state machine plus the receive side of the
// flow-control window (credit granted to the peer). Send-side credit lives
// in the session's stream handle (flowcontrol.go).
type stream struct {
	id    uint32
	state StreamState

	mu sync.Mutex
	// receive credit: bytes the peer may still send us
	recvWindow int64
	// sequences expected next from the peer (per frame type, §7.3)
	nextDataSeq uint32
	nextWinSeq  uint32
	// buffered received bytes not yet consumed by the application
	buffered int
}

// Side identifies which end of the session a registry belongs to; it fixes
// the stream ID parity (§6): the client opens odd streams, the server even.
type Side bool

const (
	SideClient Side = false
	SideServer Side = true
)

// Registry tracks all streams of one session endpoint and enforces the
// protocol's stream rules. It is transport-agnostic and lock-scoped: no
// network I/O happens under reg.mu (the scheduling contract, §9).
type Registry struct {
	side       Side
	mu         sync.Mutex
	streams    map[uint32]*stream
	maxStreams int
	closed     bool
}

func NewRegistry(side Side, maxStreams int) *Registry {
	if maxStreams <= 0 {
		maxStreams = 256
	}
	return &Registry{side: side, streams: make(map[uint32]*stream), maxStreams: maxStreams}
}

// validOpenID checks the parity and range rules for a locally or remotely
// initiated stream (§6).
func (r *Registry) validOpenID(id uint32) error {
	if id == 0 {
		return fmt.Errorf("%w: stream id 0", ErrStreamIDZero)
	}
	if r.side == SideClient && id%2 != 1 {
		return fmt.Errorf("%w: client streams must be odd, got %d", ErrStreamIDParity, id)
	}
	if r.side == SideServer && id%2 != 0 {
		return fmt.Errorf("%w: server streams must be even, got %d", ErrStreamIDParity, id)
	}
	return nil
}

// Open registers a locally initiated stream in StreamOpening. The caller
// owns stream ID allocation; this method only validates the protocol rules.
func (r *Registry) Open(id uint32, initialWindow int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("mux registry closed")
	}
	if err := r.validOpenID(id); err != nil {
		return err
	}
	if _, exists := r.streams[id]; exists {
		return fmt.Errorf("%w: %d", ErrStreamIDDuplicate, id)
	}
	if len(r.streams) >= r.maxStreams {
		return fmt.Errorf("mux stream limit %d reached", r.maxStreams)
	}
	r.streams[id] = &stream{id: id, state: StreamOpening, recvWindow: initialWindow, nextDataSeq: 1, nextWinSeq: 1}
	return nil
}

// OpenAck transitions a locally opened stream on OPEN_OK.
func (r *Registry) OpenAck(id uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	if s.state != StreamOpening {
		return fmt.Errorf("mux OPEN_OK on stream %d in state %s", id, s.state)
	}
	s.state = StreamOpen
	return nil
}

// AcceptOpen registers a remotely initiated stream (peer's OPEN). The peer
// must respect our parity: a client registry only accepts odd IDs from the
// server side of the wire, i.e. the roles are mirrored — a client accepts
// even stream IDs, a server accepts odd ones.
func (r *Registry) AcceptOpen(id uint32, initialWindow int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("mux registry closed")
	}
	if id == 0 {
		return fmt.Errorf("%w: stream id 0", ErrStreamIDZero)
	}
	// Mirror of validOpenID: the peer's domain is the opposite parity.
	if r.side == SideClient && id%2 != 0 {
		return fmt.Errorf("%w: client received odd stream %d", ErrStreamIDParity, id)
	}
	if r.side == SideServer && id%2 != 1 {
		return fmt.Errorf("%w: server received even stream %d", ErrStreamIDParity, id)
	}
	if _, exists := r.streams[id]; exists {
		return fmt.Errorf("%w: %d", ErrStreamIDDuplicate, id)
	}
	if len(r.streams) >= r.maxStreams {
		return fmt.Errorf("mux stream limit %d reached", r.maxStreams)
	}
	r.streams[id] = &stream{id: id, state: StreamOpen, recvWindow: initialWindow, nextDataSeq: 1, nextWinSeq: 1}
	return nil
}

// State returns a snapshot of the stream's state.
func (r *Registry) State(id uint32) (StreamState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return StreamClosed, fmt.Errorf("%w", ErrGhostStream)
	}
	return s.state, nil
}

// LocalFin records that we sent FIN on the stream (half-close local).
func (r *Registry) LocalFin(id uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	switch s.state {
	case StreamOpen:
		s.state = StreamHalfClosedLocal
		return nil
	case StreamHalfClosedRemote:
		s.state = StreamClosed
		return nil
	default:
		return fmt.Errorf("%w in state %s", ErrDuplicateFin, s.state)
	}
}

// RemoteFin applies a peer FIN (half-close remote). Late DATA after this is
// the peer's protocol error.
func (r *Registry) RemoteFin(id uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	switch s.state {
	case StreamOpen, StreamHalfClosedRemote:
		if s.state == StreamHalfClosedRemote {
			return fmt.Errorf("%w", ErrDuplicateFin)
		}
		s.state = StreamHalfClosedRemote
		return nil
	case StreamHalfClosedLocal:
		s.state = StreamClosed
		return nil
	default:
		return fmt.Errorf("%w in state %s", ErrDuplicateFin, s.state)
	}
}

// Close marks the stream closed from any state (RESET, OPEN_ERR or
// transport teardown). Duplicated RESET is reported for the caller's
// protocol-violation accounting.
func (r *Registry) Close(id uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	if s.state == StreamClosed {
		return fmt.Errorf("%w: %d", ErrDuplicateReset, id)
	}
	s.state = StreamClosed
	return nil
}

// Remove deregisters a fully closed stream so its ID is not reused but also
// stops counting against the stream limit once both sides observed closure.
// IDs are never recycled within an epoch (§6); removal only frees budget.
func (r *Registry) Remove(id uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.streams, id)
}

// Count returns the number of tracked streams.
func (r *Registry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}

// CheckData validates an inbound DATA frame against the stream state and
// sequence rules, deducting receive credit. It returns the credit still
// available after the frame; a negative value is a window violation the
// caller reports as ERR_FLOW_CONTROL.
func (r *Registry) CheckData(id uint32, seq uint32, n int) (remaining int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return 0, fmt.Errorf("%w", ErrGhostStream)
	}
	switch s.state {
	case StreamOpen, StreamHalfClosedLocal:
		// receiving data is legal while we haven't sent our own FIN
	default:
		return 0, fmt.Errorf("%w (state %s)", ErrDataAfterFin, s.state)
	}
	if seq != s.nextDataSeq {
		return 0, fmt.Errorf("%w: got %d want %d", ErrSequence, seq, s.nextDataSeq)
	}
	s.nextDataSeq++
	s.recvWindow -= int64(n)
	s.buffered += n
	return s.recvWindow, nil
}

// CheckWindowUpdate validates an inbound WINDOW_UPDATE sequence.
func (r *Registry) CheckWindowUpdate(id uint32, seq uint32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	if seq != s.nextWinSeq {
		return fmt.Errorf("%w: got %d want %d", ErrSequence, seq, s.nextWinSeq)
	}
	s.nextWinSeq++
	return nil
}

// GrowReceiveWindow returns n consumed bytes to the stream's receive
// credit after the application read them; the caller announces this to the
// peer with a WINDOW_UPDATE. This is the same ledger CheckData deducts
// from — the single authority on receive credit.
func (r *Registry) GrowReceiveWindow(id uint32, n int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return fmt.Errorf("%w: %d", ErrGhostStream, id)
	}
	s.recvWindow += n
	return nil
}

// CloseAll marks every stream closed (transport teardown path); returns the
// IDs so the caller can surface ERR_TRANSPORT_LOST per stream.
func (r *Registry) CloseAll() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	ids := make([]uint32, 0, len(r.streams))
	for id, s := range r.streams {
		s.state = StreamClosed
		ids = append(ids, id)
	}
	r.streams = make(map[uint32]*stream)
	return ids
}
