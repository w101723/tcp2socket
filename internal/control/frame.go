package control

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame is a decoded control frame. Payload content is opaque at this layer;
// SessionID and Sequence semantics belong to the handshake and heartbeat code.
type Frame struct {
	Type      Type
	Flags     uint16 // reserved; decode rejects nonzero
	SessionID [16]byte
	Sequence  uint32
	Payload   []byte
}

// Parse decodes exactly one frame from buf. The payload length is validated
// before any allocation, the payload is copied, and trailing bytes are
// rejected to surface stream desynchronization early.
func Parse(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, fmt.Errorf("%w: header needs %d bytes, have %d", ErrTruncated, HeaderSize, len(buf))
	}
	f, total, err := parseHeader(buf)
	if err != nil {
		return Frame{}, err
	}
	if len(buf) < total {
		return Frame{}, fmt.Errorf("%w: frame needs %d bytes, have %d", ErrTruncated, total, len(buf))
	}
	if len(buf) > total {
		return Frame{}, fmt.Errorf("%w: %d bytes after a %d-byte frame", ErrTrailingData, len(buf)-total, total)
	}
	if total > HeaderSize {
		f.Payload = append([]byte(nil), buf[HeaderSize:total]...)
	}
	return f, nil
}

// ReadFrame decodes one frame from r. The declared length is validated before
// any payload allocation; a short stream yields ErrTruncated.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, fmt.Errorf("%w: header: %v", ErrTruncated, err)
		}
		return Frame{}, err
	}
	f, total, err := parseHeader(header[:])
	if err != nil {
		return Frame{}, err
	}
	if total > HeaderSize {
		payload := make([]byte, total-HeaderSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return Frame{}, fmt.Errorf("%w: payload: %v", ErrTruncated, err)
			}
			return Frame{}, err
		}
		f.Payload = payload
	}
	return f, nil
}

// WriteFrame encodes f and writes it as a single buffer.
func WriteFrame(w io.Writer, f Frame) error {
	buf, err := Encode(f)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// AppendFrame encodes f and appends it to dst, for batched writers.
func AppendFrame(dst []byte, f Frame) ([]byte, error) {
	buf, err := Encode(f)
	if err != nil {
		return dst, err
	}
	return append(dst, buf...), nil
}

// headerFor tests: builds a raw header with explicit fields.
func headerFor(t Type, flags uint16, plen uint32, session [16]byte, sequence uint32) []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], Magic)
	buf[4] = ProtocolVersion
	buf[5] = byte(t)
	binary.BigEndian.PutUint16(buf[6:8], flags)
	binary.BigEndian.PutUint32(buf[8:12], plen)
	copy(buf[12:28], session[:])
	binary.BigEndian.PutUint32(buf[28:32], sequence)
	return buf
}
