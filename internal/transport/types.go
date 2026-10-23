package transport

import (
	"context"
	"io"
	"net"
	"time"
)

// Stream.Close must unblock pending reads and writes.
type Stream interface {
	io.ReadWriteCloser
}

type CloseWriter interface{ CloseWrite() error }
type CloseReader interface{ CloseRead() error }
type DeadlineSetter interface {
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

type Target struct {
	Network string
	Address string
}

type Dialer interface {
	DialContext(context.Context, Target) (Stream, error)
}

type DirectDialer struct{ Timeout time.Duration }

func (d DirectDialer) DialContext(ctx context.Context, target Target) (Stream, error) {
	dialer := net.Dialer{Timeout: d.Timeout}
	return dialer.DialContext(ctx, target.Network, target.Address)
}
