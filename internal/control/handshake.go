package control

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

// Capability codes. v0.3 defines only heartbeat (PR-05C); any other nonzero
// code is well formed but unsupported and rejected with ErrCodeUnsupportedCapability.
const (
	CapHeartbeat byte = 0x01
)

// ERROR frame payload codes.
const (
	ErrCodeUnsupportedCapability byte = 0x01
	ErrCodeProtocol              byte = 0x02
	ErrCodeUnauthorized          byte = 0x03
)

// DefaultHandshakeTimeout bounds one handshake when the caller passes 0? No:
// zero means caller-managed deadlines. This constant is the recommended value.
const DefaultHandshakeTimeout = 5 * time.Second

var (
	ErrHandshakeFailed       = errors.New("control handshake failed")
	ErrCapabilityUnsupported = errors.New("control capability unsupported")
	ErrSessionIDMismatch     = errors.New("control session id mismatch")
)

// Result is a completed handshake. Both sides are READY only after the
// initiator verified the ACK and the responder fully wrote it.
type Result struct {
	SessionID    [16]byte
	Capabilities []byte
}

// RemoteError is a structured ERROR frame received from the peer. Detail is
// peer-controlled text: never write credentials into it, never trust it.
type RemoteError struct {
	Code   byte
	Detail string
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote control error 0x%02x (%s): %s", e.Code, errorCodeName(e.Code), e.Detail)
}

func errorCodeName(c byte) string {
	switch c {
	case ErrCodeUnsupportedCapability:
		return "unsupported_capability"
	case ErrCodeProtocol:
		return "protocol_error"
	case ErrCodeUnauthorized:
		return "unauthorized"
	default:
		return "unknown"
	}
}

// ParseErrorPayload decodes an ERROR payload: one code byte plus detail text.
func ParseErrorPayload(payload []byte) (*RemoteError, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("%w: empty ERROR payload", ErrHandshakeFailed)
	}
	return &RemoteError{Code: payload[0], Detail: string(payload[1:])}, nil
}

func encodeErrorPayload(code byte, detail string) []byte {
	return append([]byte{code}, detail...)
}

func writeErrorFrame(w io.Writer, code byte, detail string) error {
	// SessionID is zero: the ERROR is often sent before a session is agreed.
	return WriteFrame(w, Frame{Type: TypeError, Payload: encodeErrorPayload(code, detail)})
}

// CheckSession isolates a live session from foreign or stale frames: every
// post-handshake frame must carry the negotiated session ID.
func CheckSession(id [16]byte, f Frame) error {
	if f.SessionID != id {
		return fmt.Errorf("%w: frame id %x != session id %x", ErrSessionIDMismatch, f.SessionID, id)
	}
	return nil
}

// parseCapabilityList validates structure only: no zero codes, no duplicates.
// Unknown-but-nonzero codes are a permission decision made by the responder.
func parseCapabilityList(payload []byte) ([]byte, error) {
	seen := make(map[byte]bool, len(payload))
	for i, c := range payload {
		if c == 0 {
			return nil, fmt.Errorf("%w: capability 0x00 at offset %d", ErrHandshakeFailed, i)
		}
		if seen[c] {
			return nil, fmt.Errorf("%w: duplicate capability 0x%02x", ErrHandshakeFailed, c)
		}
		seen[c] = true
	}
	return append([]byte(nil), payload...), nil
}

func containsCapability(list []byte, c byte) bool {
	for _, v := range list {
		if v == c {
			return true
		}
	}
	return false
}

type deadliner interface{ SetDeadline(time.Time) error }

// setDeadline enforces the handshake timeout. A stream without deadline
// support fails closed: an unbounded handshake would violate the guarantee
// that a stalled peer cannot hold the session hostage.
func setDeadline(rw io.ReadWriter, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	d, ok := rw.(deadliner)
	if !ok {
		return fmt.Errorf("%w: stream does not support deadlines; refusing unbounded handshake", ErrHandshakeFailed)
	}
	return d.SetDeadline(time.Now().Add(timeout))
}

func clearDeadline(rw io.ReadWriter) {
	if d, ok := rw.(deadliner); ok {
		_ = d.SetDeadline(time.Time{})
	}
}

// HandshakeInitiate sends HELLO(version, capabilities, sessionID) and verifies
// the ACK. The caller generates the session ID; it must be fresh per
// handshake, which is what makes stale-session frames detectable. Capability
// structure and permission are validated by the responder, which answers a
// malformed list with a structured ERROR.
func HandshakeInitiate(rw io.ReadWriter, sessionID [16]byte, capabilities []byte, timeout time.Duration) (Result, error) {
	if err := setDeadline(rw, timeout); err != nil {
		return Result{}, err
	}
	hello := Frame{Type: TypeHello, SessionID: sessionID, Payload: append([]byte(nil), capabilities...)}
	if err := WriteFrame(rw, hello); err != nil {
		return Result{}, fmt.Errorf("%w: write hello: %w", ErrHandshakeFailed, err)
	}
	f, err := ReadFrame(rw)
	if err != nil {
		return Result{}, fmt.Errorf("%w: read ack: %w", ErrHandshakeFailed, err)
	}
	if f.Type == TypeError {
		remote, perr := ParseErrorPayload(f.Payload)
		if perr != nil {
			return Result{}, perr
		}
		return Result{}, fmt.Errorf("%w: %w", ErrHandshakeFailed, remote)
	}
	if f.Type != TypeHelloAck {
		return Result{}, fmt.Errorf("%w: expected hello_ack, got %s", ErrHandshakeFailed, f.Type)
	}
	if err := CheckSession(sessionID, f); err != nil {
		return Result{}, fmt.Errorf("%w: ack: %w", ErrHandshakeFailed, err)
	}
	if !bytes.Equal(f.Payload, capabilities) {
		return Result{}, fmt.Errorf("%w: ack capabilities %v != requested %v", ErrHandshakeFailed, f.Payload, capabilities)
	}
	clearDeadline(rw)
	return Result{SessionID: sessionID, Capabilities: append([]byte(nil), capabilities...)}, nil
}

// HandshakeRespond waits for HELLO, validates version, type, reserved flags
// and size at the frame layer, then its own capability permission. On
// structural or permission failure it sends a structured ERROR (best effort)
// and stays non-READY. READY is reached only after the ACK is fully written.
func HandshakeRespond(rw io.ReadWriter, supported []byte, timeout time.Duration) (Result, error) {
	if err := setDeadline(rw, timeout); err != nil {
		return Result{}, err
	}
	f, err := ReadFrame(rw)
	if err != nil {
		// Frame-level rejection: the peer may still be able to read a
		// structured error. Best effort only; the stream may be garbage.
		if errors.Is(err, ErrUnsupportedVersion) || errors.Is(err, ErrBadMagic) ||
			errors.Is(err, ErrUnknownFrameType) || errors.Is(err, ErrReservedFlags) ||
			errors.Is(err, ErrPayloadTooLarge) {
			_ = writeErrorFrame(rw, ErrCodeProtocol, "invalid control frame")
		}
		return Result{}, fmt.Errorf("%w: read hello: %w", ErrHandshakeFailed, err)
	}
	if f.Type != TypeHello {
		_ = writeErrorFrame(rw, ErrCodeProtocol, fmt.Sprintf("expected hello, got %s", f.Type))
		return Result{}, fmt.Errorf("%w: expected hello, got %s", ErrHandshakeFailed, f.Type)
	}
	caps, err := parseCapabilityList(f.Payload)
	if err != nil {
		_ = writeErrorFrame(rw, ErrCodeProtocol, err.Error())
		return Result{}, err
	}
	for _, c := range caps {
		if !containsCapability(supported, c) {
			_ = writeErrorFrame(rw, ErrCodeUnsupportedCapability, fmt.Sprintf("capability %s not permitted", capName(c)))
			return Result{}, fmt.Errorf("%w: 0x%02x", ErrCapabilityUnsupported, c)
		}
	}
	ack := Frame{Type: TypeHelloAck, SessionID: f.SessionID, Payload: caps}
	if err := WriteFrame(rw, ack); err != nil {
		return Result{}, fmt.Errorf("%w: write ack: %w", ErrHandshakeFailed, err)
	}
	clearDeadline(rw)
	return Result{SessionID: f.SessionID, Capabilities: caps}, nil
}

func capName(c byte) string {
	if c == CapHeartbeat {
		return "heartbeat"
	}
	return fmt.Sprintf("0x%02x", c)
}
