package control

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

var testSession = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// TestHeaderLayout freezes the wire format: any change here is a protocol
// change and must be a deliberate, versioned one.
func TestHeaderLayout(t *testing.T) {
	f := Frame{Type: TypeHello, SessionID: testSession, Sequence: 0x01020304, Payload: []byte("abc")}
	buf, err := Encode(f)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{
		'T', '2', 'S', '1', // magic
		0x01,       // version
		0x01,       // type hello
		0x00, 0x00, // flags
		0x00, 0x00, 0x00, 0x03, // payload length
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, // session id
		0x01, 0x02, 0x03, 0x04, // sequence
		'a', 'b', 'c',
	}
	if !bytes.Equal(buf, want) {
		t.Fatalf("wire bytes changed:\n got %x\nwant %x", buf, want)
	}
}

func TestRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		typ     Type
		payload []byte
	}{
		{TypeHello, nil},
		{TypeHelloAck, []byte("caps")},
		{TypePing, nil},
		{TypePong, nil},
		{TypeError, []byte("version mismatch")},
		{TypeClose, []byte{}},
		{TypePing, make([]byte, MaxPayloadSize)},
		{TypePong, make([]byte, MaxPayloadSize-1)},
	} {
		f := Frame{Type: tt.typ, SessionID: testSession, Sequence: 7, Payload: tt.payload}
		buf, err := Encode(f)
		if err != nil {
			t.Fatalf("encode %s: %v", tt.typ, err)
		}
		got, err := Parse(buf)
		if err != nil {
			t.Fatalf("parse %s: %v", tt.typ, err)
		}
		if got.Type != f.Type || got.Sequence != f.Sequence || got.SessionID != f.SessionID {
			t.Fatalf("%s: header mismatch: %+v", tt.typ, got)
		}
		if !bytes.Equal(got.Payload, f.Payload) {
			t.Fatalf("%s: payload mismatch", tt.typ)
		}
		rf, err := ReadFrame(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("read %s: %v", tt.typ, err)
		}
		if rf.Type != f.Type || rf.Sequence != f.Sequence || !bytes.Equal(rf.Payload, f.Payload) {
			t.Fatalf("%s: streaming mismatch: %+v", tt.typ, rf)
		}
	}
}

func TestTruncatedFrame(t *testing.T) {
	f := Frame{Type: TypePong, SessionID: testSession, Sequence: 9, Payload: []byte("0123456789")}
	buf, err := Encode(f)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(buf); i++ {
		if _, err := Parse(buf[:i]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("prefix %d: err = %v, want ErrTruncated", i, err)
		}
	}
	if _, err := Parse(buf); err != nil {
		t.Fatalf("full frame rejected: %v", err)
	}
}

// TestOversizedPayload verifies the length check happens before any payload
// read: a huge declared length must fail even when no further bytes exist.
func TestOversizedPayload(t *testing.T) {
	for _, plen := range []uint32{MaxPayloadSize + 1, 0xFFFFFFFF} {
		header := headerFor(TypePing, 0, plen, testSession, 1)
		if _, err := Parse(header); !errors.Is(err, ErrPayloadTooLarge) {
			t.Fatalf("plen %d: err = %v, want ErrPayloadTooLarge", plen, err)
		}
		// One trailing byte only: must still be the length check, not EOF.
		if _, err := ReadFrame(bytes.NewReader(append(header, 0x00))); !errors.Is(err, ErrPayloadTooLarge) {
			t.Fatalf("stream plen %d: err = %v, want ErrPayloadTooLarge", plen, err)
		}
	}
	// Exactly at the limit is accepted.
	ok := append(headerFor(TypePing, 0, MaxPayloadSize, testSession, 1), make([]byte, MaxPayloadSize)...)
	if _, err := Parse(ok); err != nil {
		t.Fatalf("max-size payload rejected: %v", err)
	}
}

func TestHeaderRejections(t *testing.T) {
	base := headerFor(TypeHello, 0, 0, testSession, 0)
	cases := []struct {
		name string
		buf  []byte
		want error
	}{
		{"bad magic", func() []byte { b := append([]byte(nil), base...); b[0] = 'X'; return b }(), ErrBadMagic},
		{"version zero", func() []byte { b := append([]byte(nil), base...); b[4] = 0; return b }(), ErrUnsupportedVersion},
		{"version two", func() []byte { b := append([]byte(nil), base...); b[4] = 2; return b }(), ErrUnsupportedVersion},
		{"type zero", func() []byte { b := append([]byte(nil), base...); b[5] = 0; return b }(), ErrUnknownFrameType},
		{"type seven", func() []byte { b := append([]byte(nil), base...); b[5] = 7; return b }(), ErrUnknownFrameType},
		{"type 0xff", func() []byte { b := append([]byte(nil), base...); b[5] = 0xff; return b }(), ErrUnknownFrameType},
		{"flag bit", func() []byte { b := append([]byte(nil), base...); b[7] = 1; return b }(), ErrReservedFlags},
		{"flag high", func() []byte { b := append([]byte(nil), base...); b[6] = 0x80; return b }(), ErrReservedFlags},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse(tc.buf); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if _, err := ReadFrame(bytes.NewReader(tc.buf)); !errors.Is(err, tc.want) {
				t.Fatalf("stream err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTrailingData(t *testing.T) {
	f := Frame{Type: TypeClose, SessionID: testSession}
	buf, _ := Encode(f)
	if _, err := Parse(append(buf, 0x00)); !errors.Is(err, ErrTrailingData) {
		t.Fatalf("err = %v, want ErrTrailingData", err)
	}
}

func TestEncodeRejects(t *testing.T) {
	if _, err := Encode(Frame{Type: Type(0x42)}); !errors.Is(err, ErrUnknownFrameType) {
		t.Fatalf("unknown type accepted: %v", err)
	}
	if _, err := Encode(Frame{Type: TypePing, Flags: 1}); !errors.Is(err, ErrReservedFlags) {
		t.Fatalf("nonzero flags accepted: %v", err)
	}
	if _, err := Encode(Frame{Type: TypePing, Payload: make([]byte, MaxPayloadSize+1)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized payload accepted: %v", err)
	}
}

// TestReadFramePartialWrites verifies the streaming decoder assembles frames
// arriving in arbitrary small chunks.
func TestReadFramePartialWrites(t *testing.T) {
	frame, _ := Encode(Frame{Type: TypeHelloAck, SessionID: testSession, Sequence: 3, Payload: bytes.Repeat([]byte{0xA5}, 1000)})
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan Frame, 1)
	errCh := make(chan error, 1)
	go func() {
		f, err := ReadFrame(server)
		if err != nil {
			errCh <- err
			return
		}
		done <- f
	}()
	go func() {
		for i := 0; i < len(frame); i += 7 {
			end := i + 7
			if end > len(frame) {
				end = len(frame)
			}
			client.Write(frame[i:end])
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case f := <-done:
		if f.Type != TypeHelloAck || f.Sequence != 3 || len(f.Payload) != 1000 {
			t.Fatalf("decoded frame: %+v", f)
		}
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("ReadFrame did not complete")
	}
}

func TestReadFrameTruncatedStream(t *testing.T) {
	f := Frame{Type: TypePing, Payload: []byte("1234")}
	buf, _ := Encode(f)
	for i := 0; i < len(buf); i++ {
		_, err := ReadFrame(bytes.NewReader(buf[:i]))
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("stream prefix %d: err = %v, want ErrTruncated", i, err)
		}
	}
}

func TestAppendFrame(t *testing.T) {
	dst := make([]byte, 0, 128)
	a := Frame{Type: TypePing, Sequence: 1, Payload: []byte("a")}
	b := Frame{Type: TypePong, Sequence: 2, Payload: []byte("b")}
	dst, err := AppendFrame(dst, a)
	if err != nil {
		t.Fatal(err)
	}
	dst, err = AppendFrame(dst, b)
	if err != nil {
		t.Fatal(err)
	}
	singleA, _ := Encode(a)
	if len(singleA) != HeaderSize+1 {
		t.Fatalf("unexpected frame size %d", len(singleA))
	}
	// Parse rejects trailing data, so split at the encoded boundary.
	first, err := Parse(dst[:len(singleA)])
	if err != nil {
		t.Fatal(err)
	}
	second, err := Parse(dst[len(singleA):])
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 1 || second.Sequence != 2 {
		t.Fatalf("batch decode mismatch: %d %d", first.Sequence, second.Sequence)
	}
	_ = io.Discard
}
