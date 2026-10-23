package control

import (
	"bytes"
	"testing"
)

// FuzzDecodeFrame hammers Parse and ReadFrame with arbitrary bytes. Neither
// may panic, block, or allocate proportionally to an unvalidated length.
func FuzzDecodeFrame(f *testing.F) {
	valid, _ := Encode(Frame{Type: TypeHello, SessionID: testSession, Sequence: 1, Payload: []byte("hi")})
	f.Add(valid)
	ack, _ := Encode(Frame{Type: TypeHelloAck, SessionID: testSession, Sequence: 42})
	f.Add(ack)
	f.Add([]byte("T2S1\x01\x01\x00\x00\x00\x00\x10\x00"))
	f.Add([]byte("T2S1\x01\x07\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("T2S1\x02\x01\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("T2S1\x01\x01\x00\x01\xff\xff\xff\xff"))
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = Parse(data)
		_, _ = ReadFrame(bytes.NewReader(data))
	})
}
