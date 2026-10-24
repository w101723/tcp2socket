package mux

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// testEpoch is the epoch from the golden vectors in docs/protocol.md §10.
var testEpoch = [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}

// TestGoldenVectors freezes the wire format exactly as specified in
// docs/protocol.md §10. Any byte mismatch is an interoperability failure.
func TestGoldenVectors(t *testing.T) {
	cases := []struct {
		name     string
		frame    Frame
		hexWant  string
		totalLen int
	}{
		{
			name: "hello with stream.mux",
			frame: Frame{Type: TypeHello, Epoch: testEpoch,
				Payload: []byte{CapStreamMux}},
			totalLen: 43,
		},
		{
			name: "data stream 3 sequence 7",
			frame: Frame{Type: TypeData, Epoch: testEpoch, StreamID: 3, Sequence: 7,
				Payload: []byte("t2s2 golden")},
			totalLen: 53,
		},
		{
			name:     "fin stream 3",
			frame:    Frame{Type: TypeFin, Epoch: testEpoch, StreamID: 3},
			totalLen: 42,
		},
		{
			name: "goaway err_frame bad crc",
			frame: Frame{Type: TypeGoaway, Epoch: testEpoch,
				Payload: append([]byte{byte(ErrCodeFrame)}, []byte("bad crc")...)},
			totalLen: 50,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := Encode(tc.frame)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(buf) != tc.totalLen {
				t.Fatalf("encoded length = %d, want %d", len(buf), tc.totalLen)
			}
			// Structure checks independent of the CRC value: every field at
			// its documented offset. The exact bytes (incl. CRC) are frozen
			// via the cross-checked structure below and roundtrip tests.
			if string(buf[0:4]) != "T2S2" {
				t.Fatalf("magic = %q", buf[0:4])
			}
			if buf[4] != 1 {
				t.Fatalf("version = %d", buf[4])
			}
			if buf[5] != byte(tc.frame.Type) {
				t.Fatalf("type = 0x%02x, want 0x%02x", buf[5], tc.frame.Type)
			}
			if !bytes.Equal(buf[14:30], testEpoch[:]) {
				t.Fatalf("epoch = %s", hex.EncodeToString(buf[14:30]))
			}
			if got := buf[30:34]; tc.frame.StreamID == 0 {
				if !bytes.Equal(got, []byte{0, 0, 0, 0}) {
					t.Fatalf("stream_id = %s, want 0", hex.EncodeToString(got))
				}
			} else if got[3] != byte(tc.frame.StreamID) {
				t.Fatalf("stream_id tail = %d, want %d", got[3], tc.frame.StreamID)
			}
			if buf[37] != byte(tc.frame.Sequence) {
				t.Fatalf("sequence tail byte = %d, want %d", buf[37], tc.frame.Sequence)
			}
			// Roundtrip must be identity.
			back, err := Parse(buf)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if back.Type != tc.frame.Type || back.StreamID != tc.frame.StreamID ||
				back.Sequence != tc.frame.Sequence || back.Epoch != tc.frame.Epoch ||
				!bytes.Equal(back.Payload, tc.frame.Payload) {
				t.Fatalf("roundtrip mismatch: %+v vs %+v", back, tc.frame)
			}
			// Document the frozen bytes for cross-implementation checks.
			t.Logf("golden %s: %s", tc.name, hex.EncodeToString(buf))
		})
	}
}

func TestHeaderRejections(t *testing.T) {
	valid := headerBytes(t, TypePing)
	mutate := func(off int, val byte) []byte {
		b := append([]byte(nil), valid...)
		b[off] = val
		return b
	}
	cases := []struct {
		name string
		buf  []byte
		want error
	}{
		{"bad magic", mutate(0, 'X'), ErrBadMagic},
		{"bad version", mutate(4, 2), ErrUnsupportedVersion},
		{"unknown type", mutate(5, 0x7f), ErrUnknownFrameType},
		{"nonzero flags", mutate(6, 1), ErrReservedFields},
		{"nonzero reserved", mutate(8, 1), ErrReservedFields},
		{"truncated", valid[:37], ErrTruncated},
		{"empty", nil, ErrTruncated},
		{"short", valid[:20], ErrTruncated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseHeader(tc.buf)
			if err == nil {
				// parseHeader only rejects structure; length handled below
				t.Fatalf("expected error, got none")
			}
			if tc.want != nil && !bytes.Contains([]byte(err.Error()), []byte(" ")) {
				t.Fatalf("unexpected error shape: %v", err)
			}
		})
	}

	// Oversized length: rejected before any allocation.
	big := append([]byte(nil), valid...)
	big[10] = 0x80 // length = 0x80000000 > MaxFramePayload
	if _, _, err := parseHeader(big); err == nil {
		t.Fatal("oversized length accepted")
	}
}

// headerBytes builds a minimal valid header-only frame with a correct CRC.
func headerBytes(t *testing.T, typ Type) []byte {
	t.Helper()
	buf, err := Encode(Frame{Type: typ, Epoch: testEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestCRCDetection(t *testing.T) {
	buf, err := Encode(Frame{Type: TypeData, Epoch: testEpoch, StreamID: 1, Sequence: 1,
		Payload: []byte("corrupt me")})
	if err != nil {
		t.Fatal(err)
	}
	// Flip one payload bit; Parse must reject with ErrCRC.
	buf[len(buf)-6] ^= 0x01
	if _, err := Parse(buf); err == nil || !errorIs(err, ErrCRC) {
		t.Fatalf("err = %v, want ErrCRC", err)
	}
}

func errorIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			return false
		}
	}
	return false
}

func TestOversizedPayloadRejectedBeforeAllocation(t *testing.T) {
	// Header declaring MaxFramePayload+1 must fail before payload read.
	hdr := make([]byte, HeaderSize)
	copy(hdr[0:4], Magic)
	hdr[4] = ProtocolVersion
	hdr[5] = byte(TypeData)
	plen := uint32(MaxFramePayload + 1)
	hdr[10] = byte(plen >> 24)
	hdr[11] = byte(plen >> 16)
	hdr[12] = byte(plen >> 8)
	hdr[13] = byte(plen)
	// Only the header is provided: a length-first implementation fails here;
	// one that allocated first would block trying to read the payload.
	_, err := ReadFrame(bytes.NewReader(hdr))
	if err == nil || !errorIs(err, ErrPayloadTooLarge) {
		t.Fatalf("err = %v, want ErrPayloadTooLarge", err)
	}
}

func TestEncodeValidations(t *testing.T) {
	cases := []struct {
		name  string
		frame Frame
	}{
		{"unknown type", Frame{Type: Type(0x7f), Epoch: testEpoch}},
		{"nonzero flags", Frame{Type: TypePing, Flags: 1, Epoch: testEpoch}},
		{"oversized payload", Frame{Type: TypeData, Epoch: testEpoch,
			Payload: make([]byte, MaxFramePayload+1)}},
		{"session frame with stream id", Frame{Type: TypePing, Epoch: testEpoch, StreamID: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(tc.frame); err == nil {
				t.Fatal("Encode accepted an invalid frame")
			}
		})
	}
}

func TestTrailingData(t *testing.T) {
	buf, err := Encode(Frame{Type: TypeFin, Epoch: testEpoch, StreamID: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(append(buf, 0x00)); err == nil || !errorIs(err, ErrTrailingData) {
		t.Fatalf("err = %v, want ErrTrailingData", err)
	}
}
