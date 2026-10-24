package mux

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Encode serializes f into a freshly allocated buffer: 38-byte header,
// payload, 4-byte CRC32C trailer. All header rules are validated before any
// byte is written, so an invalid frame never reaches the wire.
func Encode(f Frame) ([]byte, error) {
	if err := validate(f); err != nil {
		return nil, err
	}
	buf := make([]byte, HeaderSize+len(f.Payload)+TrailerSize)
	fillHeader(buf, f)
	copy(buf[HeaderSize:], f.Payload)
	binary.BigEndian.PutUint32(buf[len(buf)-4:], Checksum(buf[:len(buf)-4]))
	return buf, nil
}

// AppendEncode appends the encoding of f to dst without allocating when dst
// has capacity.
func AppendEncode(dst []byte, f Frame) ([]byte, error) {
	if err := validate(f); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize+len(f.Payload)+TrailerSize)...)
	fillHeader(dst[start:], f)
	copy(dst[start+HeaderSize:], f.Payload)
	body := dst[start : len(dst)-TrailerSize]
	binary.BigEndian.PutUint32(dst[len(dst)-4:], Checksum(body))
	return dst, nil
}

func fillHeader(buf []byte, f Frame) {
	copy(buf[0:4], Magic)
	buf[4] = ProtocolVersion
	buf[5] = byte(f.Type)
	binary.BigEndian.PutUint16(buf[6:8], f.Flags)
	// reserved 8:10 stays zero
	binary.BigEndian.PutUint32(buf[10:14], uint32(len(f.Payload)))
	copy(buf[14:30], f.Epoch[:])
	binary.BigEndian.PutUint32(buf[30:34], f.StreamID)
	binary.BigEndian.PutUint32(buf[34:38], f.Sequence)
}

func validate(f Frame) error {
	if !f.Type.valid() {
		return fmt.Errorf("%w: 0x%02x", ErrUnknownFrameType, uint8(f.Type))
	}
	if f.Flags != 0 {
		return fmt.Errorf("%w: flags=0x%04x", ErrReservedFields, f.Flags)
	}
	if len(f.Payload) > MaxFramePayload {
		return fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(f.Payload), MaxFramePayload)
	}
	if f.Type.sessionScoped() && f.StreamID != 0 {
		return fmt.Errorf("%w: %s must use stream_id 0, got %d", ErrReservedFields, f.Type, f.StreamID)
	}
	return nil
}

// Parse decodes exactly one frame from buf. The declared length is validated
// inside parseHeader before any payload copy; the CRC is verified over the
// whole frame; trailing bytes are rejected to surface stream desync.
func Parse(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, fmt.Errorf("%w: header needs %d bytes, have %d", ErrTruncated, HeaderSize, len(buf))
	}
	f, total, err := parseHeader(buf[:HeaderSize])
	if err != nil {
		return Frame{}, err
	}
	if len(buf) < total {
		return Frame{}, fmt.Errorf("%w: frame needs %d bytes, have %d", ErrTruncated, total, len(buf))
	}
	if len(buf) > total {
		return Frame{}, fmt.Errorf("%w: %d bytes after a %d-byte frame", ErrTrailingData, len(buf)-total, total)
	}
	if got := Checksum(buf[:total-TrailerSize]); got != binary.BigEndian.Uint32(buf[total-TrailerSize:total]) {
		return Frame{}, fmt.Errorf("%w: computed 0x%08x", ErrCRC, got)
	}
	if total > HeaderSize+TrailerSize {
		f.Payload = append([]byte(nil), buf[HeaderSize:total-TrailerSize]...)
	}
	return f, nil
}

// ReadFrame decodes one frame from r. The declared length is validated
// before any payload allocation; a short stream yields ErrTruncated.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Frame{}, fmt.Errorf("%w: header: %v", ErrTruncated, err)
		}
		return Frame{}, err
	}
	f, total, err := parseHeader(header[:])
	if err != nil {
		return Frame{}, err
	}
	// payload + trailer, bounded by MaxFramePayload + TrailerSize
	rest := make([]byte, total-HeaderSize)
	if _, err := io.ReadFull(r, rest); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Frame{}, fmt.Errorf("%w: body: %v", ErrTruncated, err)
		}
		return Frame{}, err
	}
	want := binary.BigEndian.Uint32(rest[len(rest)-4:])
	full := make([]byte, 0, total)
	full = append(full, header[:]...)
	full = append(full, rest[:len(rest)-4]...)
	if got := Checksum(full); got != want {
		return Frame{}, fmt.Errorf("%w: computed 0x%08x", ErrCRC, got)
	}
	if n := total - HeaderSize - TrailerSize; n > 0 {
		f.Payload = rest[:n]
	}
	return f, nil
}

// WriteFrame encodes f and writes it as a single buffer. A nil-error short
// write is io.ErrShortWrite (same contract as control.WriteFrame).
func WriteFrame(w io.Writer, f Frame) error {
	buf, err := Encode(f)
	if err != nil {
		return err
	}
	n, err := w.Write(buf)
	if err == nil && n != len(buf) {
		return io.ErrShortWrite
	}
	return err
}
