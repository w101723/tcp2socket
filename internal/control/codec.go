// Package control implements the T2S1 control-channel framing for session
// handshake and heartbeat between two endpoints that already share an
// authorized, reliable byte stream. The framing provides state exchange and
// failure detection only: it performs no encryption and no authentication, and
// it must not be exposed to untrusted networks. It is not wired into the
// legacy proxy, STDIO or SOCKS paths in this phase.
package control

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Magic identifies the wire format; it spells "T2S1".
	Magic = "T2S1"
	// ProtocolVersion is the only accepted frame version.
	ProtocolVersion = 1
	// MaxPayloadSize bounds the declared payload length. It is validated
	// before any allocation.
	MaxPayloadSize = 4096
	// HeaderSize is the fixed header length in bytes:
	// magic(4) version(1) type(1) flags(2) length(4) session(16) sequence(4).
	HeaderSize = 32
)

// Type enumerates control frame types. Values 0x00 and >= 0x07 are invalid.
type Type uint8

const (
	TypeHello    Type = 0x01
	TypeHelloAck Type = 0x02
	TypePing     Type = 0x03
	TypePong     Type = 0x04
	TypeError    Type = 0x05
	TypeClose    Type = 0x06
)

func (t Type) valid() bool { return t >= TypeHello && t <= TypeClose }

func (t Type) String() string {
	switch t {
	case TypeHello:
		return "hello"
	case TypeHelloAck:
		return "hello_ack"
	case TypePing:
		return "ping"
	case TypePong:
		return "pong"
	case TypeError:
		return "error"
	case TypeClose:
		return "close"
	default:
		return "unknown"
	}
}

var (
	ErrTruncated          = errors.New("control frame truncated")
	ErrTrailingData       = errors.New("trailing bytes after control frame")
	ErrBadMagic           = errors.New("bad control frame magic")
	ErrUnsupportedVersion = errors.New("unsupported control protocol version")
	ErrReservedFlags      = errors.New("control frame reserved flags must be zero")
	ErrPayloadTooLarge    = errors.New("control frame payload too large")
	ErrUnknownFrameType   = errors.New("unknown control frame type")
)

// parseHeader validates the fixed header and returns the frame prefix plus the
// total on-wire length. The declared length is checked before any allocation.
func parseHeader(buf []byte) (Frame, int, error) {
	if string(buf[0:4]) != Magic {
		return Frame{}, 0, fmt.Errorf("%w: %q", ErrBadMagic, buf[0:4])
	}
	if buf[4] != ProtocolVersion {
		return Frame{}, 0, fmt.Errorf("%w: %d", ErrUnsupportedVersion, buf[4])
	}
	t := Type(buf[5])
	if !t.valid() {
		return Frame{}, 0, fmt.Errorf("%w: 0x%02x", ErrUnknownFrameType, uint8(t))
	}
	flags := binary.BigEndian.Uint16(buf[6:8])
	if flags != 0 {
		return Frame{}, 0, fmt.Errorf("%w: 0x%04x", ErrReservedFlags, flags)
	}
	plen := binary.BigEndian.Uint32(buf[8:12])
	if plen > MaxPayloadSize {
		return Frame{}, 0, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, plen, MaxPayloadSize)
	}
	var f Frame
	f.Type = t
	copy(f.SessionID[:], buf[12:28])
	f.Sequence = binary.BigEndian.Uint32(buf[28:32])
	return f, HeaderSize + int(plen), nil
}

// Encode serializes f, rejecting values the decoder would refuse. Flags are
// always written as zero.
func Encode(f Frame) ([]byte, error) {
	if !f.Type.valid() {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownFrameType, uint8(f.Type))
	}
	if f.Flags != 0 {
		return nil, fmt.Errorf("%w: 0x%04x", ErrReservedFlags, f.Flags)
	}
	if len(f.Payload) > MaxPayloadSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(f.Payload), MaxPayloadSize)
	}
	buf := make([]byte, HeaderSize+len(f.Payload))
	copy(buf[0:4], Magic)
	buf[4] = ProtocolVersion
	buf[5] = byte(f.Type)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(f.Payload)))
	copy(buf[12:28], f.SessionID[:])
	binary.BigEndian.PutUint32(buf[28:32], f.Sequence)
	copy(buf[HeaderSize:], f.Payload)
	return buf, nil
}
