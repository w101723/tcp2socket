package session

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// Session tracks lifecycle state for one upstream conversation. It owns no
// network sockets itself; the caller associates them via Close and Wait.
// Close is idempotent, Wait always returns, and exactly one closed event fires.
type Session struct {
	id        [16]byte
	createdAt time.Time

	mu          sync.Mutex
	state       State
	reason      CloseReason
	updatedAt   time.Time
	closeOnce   sync.Once
	done        chan struct{}
	observers   []Observer
	observersMu sync.Mutex
	onFinish    func(*Session) // set by a Manager; removes the session from the registry
}

// New creates a session in StateInit with a random 128-bit identifier.
// The identifier is not an authentication credential.
func New() (*Session, error) {
	s := &Session{
		createdAt: time.Now(),
		updatedAt: time.Now(),
		done:      make(chan struct{}),
	}
	if _, err := rand.Read(s.id[:]); err != nil {
		return nil, fmt.Errorf("generate session id: %w", err)
	}
	s.emit(Event{SessionID: s.id, Type: EventCreated, To: StateInit, At: s.createdAt})
	return s, nil
}

func (s *Session) ID() [16]byte { return s.id }

// Snapshot returns a consistent view of mutable fields.
func (s *Session) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Snapshot{
		ID:        s.id,
		State:     s.state,
		CreatedAt: s.createdAt,
		UpdatedAt: s.updatedAt,
		Reason:    s.reason,
	}
}

type Snapshot struct {
	ID        [16]byte
	State     State
	CreatedAt time.Time
	UpdatedAt time.Time
	Reason    CloseReason
}

// Transition moves to the next state if the edge is legal.
// It returns ErrClosed once the session has begun closing.
func (s *Session) Transition(next State) error {
	s.mu.Lock()
	current := s.state
	if current.Terminal() || current == StateClosing {
		s.mu.Unlock()
		return ErrClosed
	}
	if !edgeAllowed(current, next) {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current, next)
	}
	s.state = next
	s.updatedAt = time.Now()
	s.mu.Unlock()
	s.emit(Event{SessionID: s.id, Type: EventStateChanged, From: current, To: next, At: s.updatedAt})
	return nil
}

func edgeAllowed(from, to State) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// Close begins an idempotent shutdown from any non-closed state and records
// the reason. Closing while already in StateClosing or StateClosed is a no-op.
func (s *Session) Close(reason CloseReason) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		from := s.state
		if from.Terminal() {
			s.mu.Unlock()
			return
		}
		s.state = StateClosing
		s.reason = reason
		s.updatedAt = time.Now()
		s.mu.Unlock()
		s.emit(Event{SessionID: s.id, Type: EventCloseInitiated, From: from, To: StateClosing, Reason: reason, At: s.updatedAt})
	})
}

// Finish marks the closed state; it must follow Close and returns ErrClosed if
// shutdown was never initiated. The manager deregistration hook runs here.
func (s *Session) Finish() error {
	s.mu.Lock()
	if s.state != StateClosing {
		s.mu.Unlock()
		return ErrClosed
	}
	s.state = StateClosed
	s.updatedAt = time.Now()
	hook := s.onFinish
	s.mu.Unlock()
	s.emit(Event{SessionID: s.id, Type: EventClosed, From: StateClosing, To: StateClosed, Reason: s.snapshotReason(), At: s.updatedAt})
	close(s.done)
	if hook != nil {
		hook(s)
	}
	return nil
}

func (s *Session) snapshotReason() CloseReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason
}

// Done is closed exactly once when the session reaches StateClosed.
func (s *Session) Done() <-chan struct{} { return s.done }

// Wait blocks until the session is closed or ctx is done; the ctx error is
// returned in the latter case, and Wait never panics on repeated calls.
func (s *Session) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) emit(e Event) {
	s.observersMu.Lock()
	observers := make([]Observer, len(s.observers))
	copy(observers, s.observers)
	s.observersMu.Unlock()
	for _, o := range observers {
		o(e)
	}
}

// OnEvent registers an observer. Callbacks run outside any session lock; a
// callback re-entering Close/Transition cannot deadlock but must not block.
func (s *Session) OnEvent(o Observer) {
	s.observersMu.Lock()
	s.observers = append(s.observers, o)
	s.observersMu.Unlock()
}
