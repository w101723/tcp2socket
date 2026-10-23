package session

import "sync"

// Manager owns session registration, lookup and a hard capacity limit.
type Manager struct {
	max int

	mu       sync.Mutex
	sessions map[[16]byte]*Session
}

func NewManager(maxSessions int) *Manager {
	return &Manager{max: maxSessions, sessions: make(map[[16]byte]*Session)}
}

// New registers a fresh session, or returns ErrCapacity at the limit.
func (m *Manager) New() (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.max > 0 && len(m.sessions) >= m.max {
		return nil, ErrCapacity
	}
	s, err := New()
	if err != nil {
		return nil, err
	}
	// The closed session deregisters itself once Finish runs.
	s.onFinish = func(closed *Session) { m.mu.Lock(); delete(m.sessions, closed.id); m.mu.Unlock() }
	m.sessions[s.id] = s
	return s, nil
}

func (m *Manager) Get(id [16]byte) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, ErrNotFound
	}
	return s, nil
}

func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// CloseAll closes every live session with the same reason; already closed
// sessions are unaffected. Callbacks are not invoked while m.mu is held.
func (m *Manager) CloseAll(reason CloseReason) {
	m.mu.Lock()
	live := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		live = append(live, s)
	}
	m.mu.Unlock()
	for _, s := range live {
		s.Close(reason)
		_ = s.Finish()
	}
}
