package session

import (
	"time"
)

type EventType string

const (
	EventCreated        EventType = "created"
	EventStateChanged   EventType = "state_changed"
	EventCloseInitiated EventType = "close_initiated"
	EventClosed         EventType = "closed"
)

// Event carries metadata only: never payloads or credentials.
type Event struct {
	SessionID [16]byte
	Type      EventType
	From, To  State
	Reason    CloseReason
	At        time.Time
	// Seq is a per-session monotonic sequence assigned while the state lock
	// is held. Emission happens after the lock is released, so two events can
	// reach observers out of transition order; Seq is the authoritative order
	// for reconstruction (see the concurrency closure tests).
	Seq uint64
}

type Observer func(Event)
