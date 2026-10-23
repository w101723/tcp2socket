package control

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

var (
	testID    = [16]byte{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0}
	staleID   = [16]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	testCaps  = []byte{CapHeartbeat}
	quietTime = 2 * time.Second
)

type handshakeOutcome struct {
	result Result
	err    error
}

func runRespond(rw io.ReadWriteCloser, supported []byte, timeout time.Duration) <-chan handshakeOutcome {
	out := make(chan handshakeOutcome, 1)
	go func() {
		res, err := HandshakeRespond(rw, supported, timeout)
		out <- handshakeOutcome{res, err}
	}()
	return out
}

func expectFailure(t *testing.T, outcome <-chan handshakeOutcome, msg string) handshakeOutcome {
	t.Helper()
	select {
	case o := <-outcome:
		if o.err == nil {
			t.Fatalf("%s: handshake unexpectedly succeeded: %+v", msg, o.result)
		}
		return o
	case <-time.After(quietTime):
		t.Fatalf("%s: handshake did not fail within deadline", msg)
		return handshakeOutcome{}
	}
}

func TestHandshakeSuccess(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	responder := runRespond(b, testCaps, 2*time.Second)

	res, err := HandshakeInitiate(a, testID, testCaps, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if res.SessionID != testID || !bytes.Equal(res.Capabilities, testCaps) {
		t.Fatalf("initiator result: %+v", res)
	}
	o := expectSuccess(t, responder)
	if o.result.SessionID != testID || !bytes.Equal(o.result.Capabilities, testCaps) {
		t.Fatalf("responder result: %+v", o.result)
	}
}

func expectSuccess(t *testing.T, outcome <-chan handshakeOutcome) handshakeOutcome {
	t.Helper()
	select {
	case o := <-outcome:
		if o.err != nil {
			t.Fatalf("handshake failed: %v", o.err)
		}
		return o
	case <-time.After(quietTime):
		t.Fatal("handshake did not complete within deadline")
		return handshakeOutcome{}
	}
}

func TestHandshakeSuccessNoCapabilities(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	responder := runRespond(b, nil, 2*time.Second)

	res, err := HandshakeInitiate(a, testID, nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Capabilities) != 0 {
		t.Fatalf("capabilities = %v, want empty", res.Capabilities)
	}
	expectSuccess(t, responder)
}

// TestUnsupportedVersion: a peer speaking a different version must fail the
// handshake on both sides; the responder still answers with a structured
// ERROR its own version can encode.
func TestUnsupportedVersion(t *testing.T) {
	t.Run("responder side", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		responder := runRespond(b, testCaps, 2*time.Second)

		hello := headerFor(TypeHello, 0, 0, testID, 0)
		hello[4] = 2 // foreign version
		if _, err := a.Write(hello); err != nil {
			t.Fatal(err)
		}
		// The responder writes a best-effort ERROR; read it concurrently so
		// the synchronous pipe does not deadlock the failure path.
		errFrame := make(chan Frame, 1)
		readErr := make(chan error, 1)
		go func() {
			f, err := ReadFrame(a)
			if err != nil {
				readErr <- err
				return
			}
			errFrame <- f
		}()
		o := expectFailure(t, responder, "version 2")
		if !errors.Is(o.err, ErrHandshakeFailed) || !errors.Is(o.err, ErrUnsupportedVersion) {
			t.Fatalf("err = %v", o.err)
		}
		// The fake peer must receive the structured ERROR.
		var f Frame
		select {
		case f = <-errFrame:
		case err := <-readErr:
			t.Fatal(err)
		case <-time.After(quietTime):
			t.Fatal("no ERROR frame received")
		}
		if f.Type != TypeError {
			t.Fatalf("frame type = %s", f.Type)
		}
		remote, err := ParseErrorPayload(f.Payload)
		if err != nil || remote.Code != ErrCodeProtocol {
			t.Fatalf("remote error = %+v, %v", remote, err)
		}
	})

	t.Run("initiator side", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		errCh := make(chan error, 1)
		go func() {
			_, err := HandshakeInitiate(a, testID, testCaps, 2*time.Second)
			errCh <- err
		}()
		// Read the HELLO, reply with a foreign-version header.
		if _, err := ReadFrame(b); err != nil {
			t.Fatal(err)
		}
		ack := headerFor(TypeHelloAck, 0, 0, testID, 0)
		ack[4] = 2
		if _, err := b.Write(ack); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrHandshakeFailed) || !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("err = %v", err)
			}
		case <-time.After(quietTime):
			t.Fatal("initiator did not fail")
		}
	})
}

// TestUnknownRequiredCapability: the requested capability is well formed but
// outside the responder's permission; both sides fail without READY.
func TestUnknownRequiredCapability(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	responder := runRespond(b, testCaps, 2*time.Second)

	_, err := HandshakeInitiate(a, testID, []byte{0x7F}, 2*time.Second)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != ErrCodeUnsupportedCapability {
		t.Fatalf("err = %v, want remote unsupported_capability", err)
	}
	o := expectFailure(t, responder, "unknown capability")
	if !errors.Is(o.err, ErrCapabilityUnsupported) {
		t.Fatalf("responder err = %v", o.err)
	}
}

// TestOldSessionID: a replayed ACK from a previous session must not complete a
// new handshake, and post-handshake frames from a foreign session are refused.
func TestOldSessionID(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	go func() {
		if _, err := ReadFrame(b); err != nil {
			return
		}
		// Replay an ACK carrying the stale session ID.
		_ = WriteFrame(b, Frame{Type: TypeHelloAck, SessionID: staleID})
	}()
	_, err := HandshakeInitiate(a, testID, testCaps, 2*time.Second)
	if !errors.Is(err, ErrSessionIDMismatch) {
		t.Fatalf("err = %v, want ErrSessionIDMismatch", err)
	}

	foreign := Frame{Type: TypePing, SessionID: staleID, Sequence: 1}
	if err := CheckSession(testID, foreign); !errors.Is(err, ErrSessionIDMismatch) {
		t.Fatalf("CheckSession err = %v", err)
	}
	if err := CheckSession(testID, Frame{Type: TypePing, SessionID: testID}); err != nil {
		t.Fatalf("own frame rejected: %v", err)
	}
}

func TestHandshakePeerEOF(t *testing.T) {
	t.Run("initiator sees EOF", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		responder := runRespond(b, testCaps, 2*time.Second)
		_ = b.Close() // vanish before replying
		// The initiator's write may fail or the read may hit EOF; both are failures.
		go func() { <-time.After(quietTime); a.Close() }()
		o := expectFailure(t, responder, "responder")
		_ = o
		_, err := HandshakeInitiate(a, testID, testCaps, 2*time.Second)
		if err == nil {
			t.Fatal("initiator succeeded after peer EOF")
		}
	})

	t.Run("responder sees EOF", func(t *testing.T) {
		a, b := net.Pipe()
		responder := runRespond(b, testCaps, 2*time.Second)
		_ = a.Close()
		expectFailure(t, responder, "responder EOF")
	})
}

func TestHandshakeTimeout(t *testing.T) {
	t.Run("initiator timeout", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close() // nobody responds
		_, err := HandshakeInitiate(a, testID, testCaps, 50*time.Millisecond)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("err = %v, want deadline exceeded", err)
		}
	})
	t.Run("responder timeout", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		responder := runRespond(b, testCaps, 50*time.Millisecond)
		o := expectFailure(t, responder, "responder timeout")
		if !errors.Is(o.err, os.ErrDeadlineExceeded) {
			t.Fatalf("err = %v, want deadline exceeded", o.err)
		}
	})
	t.Run("no deadline support fails closed", func(t *testing.T) {
		rw := struct {
			io.Reader
			io.Writer
		}{bytes.NewReader(nil), io.Discard}
		if _, err := HandshakeInitiate(rw, testID, testCaps, time.Second); !errors.Is(err, ErrHandshakeFailed) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestHandshakeWrongFirstFrame(t *testing.T) {
	t.Run("responder receives ping", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		responder := runRespond(b, testCaps, 2*time.Second)
		if err := WriteFrame(a, Frame{Type: TypePing, SessionID: testID}); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = ReadFrame(a) }() // drain the best-effort ERROR
		o := expectFailure(t, responder, "ping first")
		if !errors.Is(o.err, ErrHandshakeFailed) {
			t.Fatalf("err = %v", o.err)
		}
	})
	t.Run("initiator receives hello", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		errCh := make(chan error, 1)
		go func() {
			_, err := HandshakeInitiate(a, testID, testCaps, 2*time.Second)
			errCh <- err
		}()
		if _, err := ReadFrame(b); err != nil {
			t.Fatal(err)
		}
		if err := WriteFrame(b, Frame{Type: TypeHello, SessionID: testID}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrHandshakeFailed) {
				t.Fatalf("err = %v", err)
			}
		case <-time.After(quietTime):
			t.Fatal("initiator did not fail")
		}
	})
}

func TestCapabilityStructure(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		wantErr error
	}{
		{"zero code", []byte{0x00}, ErrHandshakeFailed},
		{"duplicate", []byte{CapHeartbeat, CapHeartbeat}, ErrHandshakeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			responder := runRespond(b, testCaps, 2*time.Second)
			go func() { _, _ = ReadFrame(a) }() // drain the best-effort ERROR
			if _, err := HandshakeInitiate(a, testID, tc.payload, 2*time.Second); err == nil {
				t.Fatal("initiator accepted malformed capability list")
			}
			expectFailure(t, responder, tc.name)
		})
	}
	if _, err := parseCapabilityList([]byte{0x00}); err == nil {
		t.Fatal("zero code accepted")
	}
	if _, err := parseCapabilityList([]byte{1, 1}); err == nil {
		t.Fatal("duplicate accepted")
	}
	if caps, err := parseCapabilityList([]byte{0x7F}); err != nil || len(caps) != 1 {
		t.Fatalf("well-formed unknown code rejected structurally: %v", err)
	}
}

func TestErrorPayloadRoundTrip(t *testing.T) {
	p := encodeErrorPayload(ErrCodeUnauthorized, "not allowed")
	remote, err := ParseErrorPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	if remote.Code != ErrCodeUnauthorized || remote.Detail != "not allowed" {
		t.Fatalf("remote = %+v", remote)
	}
	if _, err := ParseErrorPayload(nil); err == nil {
		t.Fatal("empty ERROR payload accepted")
	}
}

func FuzzParseErrorPayload(f *testing.F) {
	f.Add([]byte{ErrCodeProtocol, 'd', 'e'})
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseErrorPayload(data)
	})
}

func FuzzParseCapabilityList(f *testing.F) {
	f.Add([]byte{CapHeartbeat})
	f.Add([]byte{0x00})
	f.Add([]byte{1, 1, 2})
	f.Add([]byte(nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = parseCapabilityList(data)
	})
}
