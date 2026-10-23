package session

import "errors"

var (
	// ErrCapacity is returned by the manager when the session limit is reached.
	ErrCapacity = errors.New("session capacity exceeded")
	// ErrClosed indicates the session is already closed or closing.
	ErrClosed = errors.New("session is closed")
	// ErrInvalidTransition reports a state change not permitted by the machine.
	ErrInvalidTransition = errors.New("invalid session state transition")
	// ErrNotFound is returned when a session ID is unknown to the manager.
	ErrNotFound = errors.New("session not found")
)

type CloseReason string

const (
	ReasonEOF          CloseReason = "eof"
	ReasonCanceled     CloseReason = "canceled"
	ReasonTimeout      CloseReason = "timeout"
	ReasonProtocol     CloseReason = "protocol_error"
	ReasonTransport    CloseReason = "transport_error"
	ReasonUnauthorized CloseReason = "unauthorized"
)

type State uint8

const (
	StateInit State = iota
	StateConnecting
	StateHandshaking
	StateReady
	StateDegraded
	StateClosing
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateInit:
		return "init"
	case StateConnecting:
		return "connecting"
	case StateHandshaking:
		return "handshaking"
	case StateReady:
		return "ready"
	case StateDegraded:
		return "degraded"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Terminal reports whether no further transition is possible.
func (s State) Terminal() bool { return s == StateClosed }

// CanAcceptWork reports whether new business requests may start on this state.
// DEGRADED never accepts new work.
func (s State) CanAcceptWork() bool { return s == StateReady }

// transitions lists permitted edges; anything else is invalid.
var transitions = map[State][]State{
	StateInit:        {StateConnecting, StateClosing},
	StateConnecting:  {StateHandshaking, StateClosing},
	StateHandshaking: {StateReady, StateClosing},
	StateReady:       {StateDegraded, StateClosing},
	StateDegraded:    {StateClosing},
	StateClosing:     {StateClosed},
	StateClosed:      {},
}
