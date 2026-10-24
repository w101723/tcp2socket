package mux

import (
	"bytes"
	"testing"
)

// FuzzDecodeFrame feeds arbitrary bytes to ReadFrame/Parse: no panic, no
// unbounded allocation, no hang. A framing error must be returned for
// anything that is not a valid frame; a valid frame must roundtrip.
func FuzzDecodeFrame(f *testing.F) {
	// Seed with the golden vectors and their mutations.
	for _, seed := range []Frame{
		{Type: TypeHello, Epoch: testEpoch, Payload: []byte{CapStreamMux}},
		{Type: TypeData, Epoch: testEpoch, StreamID: 3, Sequence: 7, Payload: []byte("t2s2 golden")},
		{Type: TypeFin, Epoch: testEpoch, StreamID: 3},
		{Type: TypeGoaway, Epoch: testEpoch, Payload: []byte{byte(ErrCodeFrame), 'b', 'a', 'd'}},
	} {
		if buf, err := Encode(seed); err == nil {
			f.Add(buf)
			f.Add(buf[:len(buf)/2])  // truncated half
			f.Add(append(buf, 0x00)) // trailing byte
			corrupt := append([]byte(nil), buf...)
			corrupt[len(corrupt)-2] ^= 0xff // CRC mismatch
			f.Add(corrupt)
		}
	}
	f.Add([]byte{})
	f.Add([]byte("T2S2"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Path 1: streaming decode.
		frame, err := ReadFrame(bytes.NewReader(data))
		if err != nil {
			return // framing errors are fine; hangs and panics are not
		}
		// A frame that decodes must re-encode losslessly and parse back.
		buf, err := Encode(frame)
		if err != nil {
			t.Fatalf("decoded frame rejected by Encode: %+v: %v", frame, err)
		}
		back, err := Parse(buf)
		if err != nil {
			t.Fatalf("re-encoded frame rejected by Parse: %v", err)
		}
		if back.Type != frame.Type || back.StreamID != frame.StreamID ||
			back.Sequence != frame.Sequence || back.Epoch != frame.Epoch ||
			!bytes.Equal(back.Payload, frame.Payload) {
			t.Fatalf("lossy roundtrip: %+v vs %+v", back, frame)
		}

		// Path 2: buffer decode of exactly one frame.
		if _, err := Parse(data); err == nil && len(data) > 0 {
			// Both paths must agree on acceptance.
			if len(data) != HeaderSize+len(frame.Payload)+TrailerSize {
				t.Fatalf("accepted frame of %d bytes with %d-byte payload", len(data), len(frame.Payload))
			}
		}
	})
}

// FuzzTargetDescriptor: OPEN payload parser against arbitrary bytes.
func FuzzTargetDescriptor(f *testing.F) {
	f.Add([]byte{NetworkTCP, 14}, []byte("127.0.0.1:8999"))
	f.Add([]byte{NetworkUnix, 12}, []byte("/tmp/app.sock"))
	f.Add([]byte{0x09, 1}, []byte("x"))
	f.Add([]byte{NetworkTCP}, []byte{})
	f.Fuzz(func(t *testing.T, head []byte, tail []byte) {
		payload := append(append([]byte{}, head...), tail...)
		if tg, err := ParseTarget(payload); err == nil {
			// A parsed target must re-encode to the same payload.
			p2, err := tg.Encode()
			if err != nil {
				t.Fatalf("parsed target rejected by Encode: %v", err)
			}
			if !bytes.Equal(p2, payload) {
				t.Fatalf("target roundtrip mismatch: %x vs %x", p2, payload)
			}
		}
	})
}

// FuzzErrorPayload: error payload parser against arbitrary bytes.
func FuzzErrorPayload(f *testing.F) {
	f.Add([]byte{byte(ErrCodeFrame), 'b', 'a', 'd', ' ', 'c', 'r', 'c'})
	f.Add([]byte{byte(ErrCodeDial)})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if e, err := ParseErrorPayload(data); err == nil {
			if len(data) < 1 || len(data)-1 > MaxErrorDetail {
				t.Fatalf("accepted out-of-bounds error payload: %d bytes", len(data))
			}
			p2, err := e.Encode()
			if err != nil {
				t.Fatalf("parsed error rejected by Encode: %v", err)
			}
			if !bytes.Equal(p2, data) {
				t.Fatalf("error roundtrip mismatch: %x vs %x", p2, data)
			}
		}
	})
}
