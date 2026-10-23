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
}

type Observer func(Event)
