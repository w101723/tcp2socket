package socks5

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
)

func TestTruncatedRequestPrefixes(t *testing.T) {
	packet := []byte{5, 1, 0, 1, 127, 0, 0, 1, 0, 80}
	for i := 0; i < len(packet); i++ {
		if _, err := ReadRequest(bytes.NewReader(packet[:i])); err == nil {
			t.Fatalf("accepted truncated request at %d bytes", i)
		}
	}
}

func TestReplyForDialError(t *testing.T) {
	for _, tt := range []struct {
		err   error
		reply byte
	}{
		{context.DeadlineExceeded, ReplyTTLExpired},
		{&net.OpError{Err: syscall.ECONNREFUSED}, ReplyConnectionRefused},
		{&net.OpError{Err: syscall.ENETUNREACH}, ReplyNetworkUnreachable},
		{&net.OpError{Err: syscall.EHOSTUNREACH}, ReplyHostUnreachable},
		{errors.New("other"), ReplyGeneralFailure},
	} {
		if got := ReplyForDialError(tt.err); got != tt.reply {
			t.Fatalf("error=%v got=%d want=%d", tt.err, got, tt.reply)
		}
	}
}

type memoryStream struct {
	io.Reader
	bytes.Buffer
	closed bool
}

func (s *memoryStream) Read(p []byte) (int, error) { return s.Reader.Read(p) }
func (s *memoryStream) Close() error               { s.closed = true; return nil }

func TestUnsupportedCommandClosesStream(t *testing.T) {
	packet := []byte{5, 1, 0, 5, 2, 0, 1, 127, 0, 0, 1, 0, 80}
	s := &memoryStream{Reader: bytes.NewReader(packet)}
	if err := Serve(context.Background(), s, &mockDialer{err: errors.New("unexpected dial")}, Options{}); err == nil {
		t.Fatal("unsupported command accepted")
	}
	want := []byte{5, 0, 5, ReplyCommandNotSupported, 0, 1, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(s.Bytes(), want) || !s.closed {
		t.Fatalf("reply=%x closed=%v", s.Bytes(), s.closed)
	}
}
