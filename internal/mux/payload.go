package mux

import (
	"fmt"
	"strings"
)

// Capabilities (§4.1).
const (
	CapStreamMux byte = 0x01 // required for multiplexing
	CapHeartbeat byte = 0x02 // optional
)

// EncodeCapabilities serializes a capability list: ascending, no dupes,
// no 0x00 (structure rules from the protocol; callers pass a valid set).
func EncodeCapabilities(caps []byte) ([]byte, error) {
	if len(caps) == 0 {
		return nil, nil
	}
	seen := make(map[byte]bool, len(caps))
	prev := byte(0)
	for i, c := range caps {
		if c == 0x00 {
			return nil, fmt.Errorf("capability 0x00 at offset %d", i)
		}
		if seen[c] {
			return nil, fmt.Errorf("duplicate capability 0x%02x", c)
		}
		if c < prev {
			return nil, fmt.Errorf("capabilities must be ascending: 0x%02x after 0x%02x", c, prev)
		}
		seen[c] = true
		prev = c
	}
	return append([]byte(nil), caps...), nil
}

// ParseCapabilities validates and copies a capability payload.
func ParseCapabilities(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	seen := make(map[byte]bool, len(payload))
	prev := byte(0)
	for i, c := range payload {
		if c == 0x00 {
			return nil, fmt.Errorf("capability 0x00 at offset %d", i)
		}
		if seen[c] {
			return nil, fmt.Errorf("duplicate capability 0x%02x", c)
		}
		if c < prev {
			return nil, fmt.Errorf("capabilities must be ascending: 0x%02x after 0x%02x", c, prev)
		}
		seen[c] = true
		prev = c
	}
	return append([]byte(nil), payload...), nil
}

// Target descriptor networks (§4.2).
const (
	NetworkTCP  byte = 0x01
	NetworkUnix byte = 0x02
)

// Target is an OPEN payload: network kind plus address.
type Target struct {
	Network byte // NetworkTCP or NetworkUnix
	Address string
}

// Encode serializes the target descriptor.
func (t Target) Encode() ([]byte, error) {
	if t.Network != NetworkTCP && t.Network != NetworkUnix {
		return nil, fmt.Errorf("unknown network 0x%02x", t.Network)
	}
	if t.Address == "" || len(t.Address) > MaxTargetAddress {
		return nil, fmt.Errorf("address length %d outside 1..%d", len(t.Address), MaxTargetAddress)
	}
	buf := make([]byte, 0, 2+len(t.Address))
	buf = append(buf, t.Network, byte(len(t.Address)))
	return append(buf, t.Address...), nil
}

// ParseTarget decodes an OPEN payload into a Target. Address bytes are
// opaque ASCII (host:port or a Unix path); semantic validation belongs to
// the dialer layer.
func ParseTarget(payload []byte) (Target, error) {
	if len(payload) < 2 {
		return Target{}, fmt.Errorf("target descriptor needs 2+ bytes, have %d", len(payload))
	}
	network := payload[0]
	if network != NetworkTCP && network != NetworkUnix {
		return Target{}, fmt.Errorf("unknown network 0x%02x", network)
	}
	length := int(payload[1])
	if length == 0 || length > MaxTargetAddress {
		return Target{}, fmt.Errorf("address length %d outside 1..%d", length, MaxTargetAddress)
	}
	if len(payload) != 2+length {
		return Target{}, fmt.Errorf("address length %d but %d payload bytes remain", length, len(payload)-2)
	}
	return Target{Network: network, Address: string(payload[2:])}, nil
}

// ErrorPayload is an OPEN_ERR/RESET/GOAWAY payload (§4.3).
type ErrorPayload struct {
	Code   ErrorCode
	Detail string
}

// Encode serializes the error payload, truncating detail to MaxErrorDetail.
func (e ErrorPayload) Encode() ([]byte, error) {
	detail := e.Detail
	if len(detail) > MaxErrorDetail {
		detail = detail[:MaxErrorDetail]
	}
	buf := make([]byte, 0, 1+len(detail))
	buf = append(buf, byte(e.Code))
	return append(buf, detail...), nil
}

// ParseErrorPayload decodes an error payload. Detail is peer-controlled
// display text: treat as untrusted, never log raw frames.
func ParseErrorPayload(payload []byte) (ErrorPayload, error) {
	if len(payload) < 1 {
		return ErrorPayload{}, fmt.Errorf("error payload needs at least the code byte")
	}
	if len(payload)-1 > MaxErrorDetail {
		return ErrorPayload{}, fmt.Errorf("detail length %d exceeds %d", len(payload)-1, MaxErrorDetail)
	}
	return ErrorPayload{Code: ErrorCode(payload[0]), Detail: string(payload[1:])}, nil
}

// WindowUpdatePayload encodes a granted credit delta (§3).
func WindowUpdatePayload(delta uint32) []byte {
	buf := make([]byte, 4)
	buf[0] = byte(delta >> 24)
	buf[1] = byte(delta >> 16)
	buf[2] = byte(delta >> 8)
	buf[3] = byte(delta)
	return buf
}

// ParseWindowUpdate decodes a granted credit delta.
func ParseWindowUpdate(payload []byte) (uint32, error) {
	if len(payload) != 4 {
		return 0, fmt.Errorf("window update payload must be 4 bytes, have %d", len(payload))
	}
	return uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3]), nil
}

// sanitizeDetail keeps untrusted peer text safe for logs: no control
// characters, bounded length.
func sanitizeDetail(s string) string {
	if len(s) > MaxErrorDetail {
		s = s[:MaxErrorDetail]
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
}
