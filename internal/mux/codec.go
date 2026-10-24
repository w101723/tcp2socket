// Package mux implements the T2S2 v1 stream multiplexing protocol frozen in
// docs/protocol.md. This file: frame types, constants, CRC32C and the
// encode/decode layer with strict pre-allocation length validation.
package mux

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Magic is the four ASCII bytes "T2S2".
const Magic = "T2S2"

// ProtocolVersion is the only accepted T2S2 version.
const ProtocolVersion = 1

// HeaderSize is the fixed frame header; TrailerSize the CRC32C trailer.
const (
	HeaderSize  = 38
	TrailerSize = 4
)

// MaxFramePayload is the largest payload any frame may carry (§5).
const MaxFramePayload = 32 * 1024

// MaxErrorDetail bounds the human-readable detail in error payloads (§5).
const MaxErrorDetail = 255

// MaxTargetAddress bounds an OPEN target address (§5).
const MaxTargetAddress = 200

// Type is a T2S2 frame type (§3).
type Type uint8

const (
	TypeHello        Type = 0x01
	TypeHelloAck     Type = 0x02
	TypeOpen         Type = 0x03
	TypeOpenOk       Type = 0x04
	TypeOpenErr      Type = 0x05
	TypeData         Type = 0x06
	TypeWindowUpdate Type = 0x07
	TypeFin          Type = 0x08
	TypeReset        Type = 0x09
	TypePing         Type = 0x0A
	TypePong         Type = 0x0B
	TypeGoaway       Type = 0x0C
)

func (t Type) valid() bool { return t >= TypeHello && t <= TypeGoaway }

func (t Type) String() string {
	switch t {
	case TypeHello:
		return "HELLO"
	case TypeHelloAck:
		return "HELLO_ACK"
	case TypeOpen:
		return "OPEN"
	case TypeOpenOk:
		return "OPEN_OK"
	case TypeOpenErr:
		return "OPEN_ERR"
	case TypeData:
		return "DATA"
	case TypeWindowUpdate:
		return "WINDOW_UPDATE"
	case TypeFin:
		return "FIN"
	case TypeReset:
		return "RESET"
	case TypePing:
		return "PING"
	case TypePong:
		return "PONG"
	case TypeGoaway:
		return "GOAWAY"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(t))
	}
}

// sessionScoped reports whether a type must carry stream_id 0 (§2).
func (t Type) sessionScoped() bool {
	switch t {
	case TypeHello, TypeHelloAck, TypePing, TypePong, TypeGoaway:
		return true
	}
	return false
}

// sequenced reports whether a type uses the per-stream sequence counter (§2).
func (t Type) sequenced() bool {
	return t == TypeData || t == TypeWindowUpdate
}

// Error codes (§4.3).
type ErrorCode uint8

const (
	ErrCodeDial          ErrorCode = 0x01
	ErrCodeTargetDenied  ErrorCode = 0x02
	ErrCodeHandshake     ErrorCode = 0x03
	ErrCodeVersion       ErrorCode = 0x04
	ErrCodeTimeout       ErrorCode = 0x05
	ErrCodeIdle          ErrorCode = 0x06
	ErrCodeFlowControl   ErrorCode = 0x07
	ErrCodeFrame         ErrorCode = 0x08
	ErrCodeTransportLost ErrorCode = 0x09
	ErrCodeAuthRequired  ErrorCode = 0x0A
	ErrCodeShutdown      ErrorCode = 0x0B
	ErrCodeStreamState   ErrorCode = 0x0C
)

func (c ErrorCode) String() string {
	switch c {
	case ErrCodeDial:
		return "ERR_DIAL"
	case ErrCodeTargetDenied:
		return "ERR_TARGET_DENIED"
	case ErrCodeHandshake:
		return "ERR_HANDSHAKE"
	case ErrCodeVersion:
		return "ERR_VERSION"
	case ErrCodeTimeout:
		return "ERR_TIMEOUT"
	case ErrCodeIdle:
		return "ERR_IDLE"
	case ErrCodeFlowControl:
		return "ERR_FLOW_CONTROL"
	case ErrCodeFrame:
		return "ERR_FRAME"
	case ErrCodeTransportLost:
		return "ERR_TRANSPORT_LOST"
	case ErrCodeAuthRequired:
		return "ERR_AUTH_REQUIRED"
	case ErrCodeShutdown:
		return "ERR_SHUTDOWN"
	case ErrCodeStreamState:
		return "ERR_STREAM_STATE"
	default:
		return fmt.Sprintf("ERR_UNKNOWN(0x%02x)", uint8(c))
	}
}

// Decode sentinels. All framing errors are session-fatal (§8); ErrCRC,
// ErrBadMagic, ErrUnsupportedVersion, ErrReservedFields, ErrPayloadTooLarge
// and ErrUnknownFrameType surface exactly which rule was violated.
var (
	ErrTruncated          = errors.New("mux frame truncated")
	ErrTrailingData       = errors.New("mux trailing data after frame")
	ErrBadMagic           = errors.New("mux bad magic")
	ErrUnsupportedVersion = errors.New("mux unsupported version")
	ErrReservedFields     = errors.New("mux reserved fields must be zero")
	ErrPayloadTooLarge    = errors.New("mux payload too large")
	ErrUnknownFrameType   = errors.New("mux unknown frame type")
	ErrCRC                = errors.New("mux crc mismatch")
)

// crc32c is the Castagnoli polynomial table (IEEE 802.3 / iSCSI / SCTP).
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC32C of b.
func Checksum(b []byte) uint32 { return crc32.Checksum(b, crc32c) }

// Frame is a decoded T2S2 frame. Payload semantics depend on Type (§4).
type Frame struct {
	Type     Type
	Flags    uint16 // must be 0 in v1
	Epoch    [16]byte
	StreamID uint32
	Sequence uint32
	Payload  []byte
}

// parseHeader validates the fixed header and returns the declared total
// length. No allocation happens before length validation.
func parseHeader(buf []byte) (f Frame, total int, err error) {
	if len(buf) < HeaderSize {
		return Frame{}, 0, fmt.Errorf("%w: header needs %d bytes, have %d", ErrTruncated, HeaderSize, len(buf))
	}
	if string(buf[0:4]) != Magic {
		return Frame{}, 0, fmt.Errorf("%w: %q", ErrBadMagic, buf[0:4])
	}
	if buf[4] != ProtocolVersion {
		return Frame{}, 0, fmt.Errorf("%w: T2S2 version %d", ErrUnsupportedVersion, buf[4])
	}
	t := Type(buf[5])
	if !t.valid() {
		return Frame{}, 0, fmt.Errorf("%w: 0x%02x", ErrUnknownFrameType, buf[5])
	}
	f.Type = t
	f.Flags = binary.BigEndian.Uint16(buf[6:8])
	if f.Flags != 0 {
		return Frame{}, 0, fmt.Errorf("%w: flags=0x%04x", ErrReservedFields, f.Flags)
	}
	if binary.BigEndian.Uint16(buf[8:10]) != 0 {
		return Frame{}, 0, fmt.Errorf("%w: reserved=0x%04x", ErrReservedFields, binary.BigEndian.Uint16(buf[8:10]))
	}
	length := binary.BigEndian.Uint32(buf[10:14])
	if length > MaxFramePayload {
		return Frame{}, 0, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, length, MaxFramePayload)
	}
	copy(f.Epoch[:], buf[14:30])
	f.StreamID = binary.BigEndian.Uint32(buf[30:34])
	f.Sequence = binary.BigEndian.Uint32(buf[34:38])
	return f, HeaderSize + int(length) + TrailerSize, nil
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}
