package socks5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"syscall"
	"time"

	"tcp2socket/internal/proxy"
	"tcp2socket/internal/transport"
)

const (
	Version5 = 0x05

	AuthNoAuth       = 0x00
	AuthNoAcceptable = 0xff

	CmdConnect = 0x01

	AtypIPv4   = 0x01
	AtypDomain = 0x03
	AtypIPv6   = 0x04

	ReplySucceeded               = 0x00
	ReplyGeneralFailure          = 0x01
	ReplyConnectionNotAllowed    = 0x02
	ReplyNetworkUnreachable      = 0x03
	ReplyHostUnreachable         = 0x04
	ReplyConnectionRefused       = 0x05
	ReplyTTLExpired              = 0x06
	ReplyCommandNotSupported     = 0x07
	ReplyAddressTypeNotSupported = 0x08
)

type Options struct {
	Verbose         bool
	StopOnTargetEOF bool
	IdleTimeout     time.Duration
	OnConnected     func(target string)
	OnClosed        func(target string, res proxy.Result)
}

type Request struct {
	Command byte
	Address string
}

type RequestError struct {
	Reply byte
	Err   error
}

func (e *RequestError) Error() string { return e.Err.Error() }
func (e *RequestError) Unwrap() error { return e.Err }

func Serve(ctx context.Context, stream transport.Stream, dialer transport.Dialer, opts Options) error {
	defer stream.Close()

	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		_ = stream.Close()
	})
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()

	if err := Negotiate(stream); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}

	req, err := ReadRequest(stream)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var reqErr *RequestError
		if errors.As(err, &reqErr) {
			_ = WriteReply(stream, reqErr.Reply)
		} else {
			_ = WriteReply(stream, ReplyGeneralFailure)
		}
		return err
	}

	if req.Command != CmdConnect {
		_ = WriteReply(stream, ReplyCommandNotSupported)
		return fmt.Errorf("unsupported SOCKS command: %d", req.Command)
	}

	dst, err := dialer.DialContext(ctx, transport.Target{
		Network: "tcp",
		Address: req.Address,
	})
	if err != nil {
		reply := ReplyForDialError(err)
		_ = WriteReply(stream, reply)
		return fmt.Errorf("dial %s: %w", req.Address, err)
	}
	defer dst.Close()

	if err := WriteReply(stream, ReplySucceeded); err != nil {
		return fmt.Errorf("write SOCKS success reply: %w", err)
	}

	if opts.OnConnected != nil {
		opts.OnConnected(req.Address)
	}

	res := proxy.Relay(ctx, stream, dst, proxy.RelayOptions{
		IdleTimeout:     opts.IdleTimeout,
		StopOnTargetEOF: opts.StopOnTargetEOF,
	})

	if opts.OnClosed != nil {
		opts.OnClosed(req.Address, res)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return res.Err
}

func Negotiate(rw io.ReadWriter) error {
	var header [2]byte
	if _, err := io.ReadFull(rw, header[:]); err != nil {
		return fmt.Errorf("read SOCKS greeting: %w", err)
	}
	if header[0] != Version5 {
		return fmt.Errorf("unsupported SOCKS version: %d", header[0])
	}
	if header[1] == 0 {
		if _, err := rw.Write([]byte{Version5, AuthNoAcceptable}); err != nil {
			return fmt.Errorf("write SOCKS method selection: %w", err)
		}
		return errors.New("SOCKS greeting contains no authentication methods")
	}

	methods := make([]byte, header[1])
	if _, err := io.ReadFull(rw, methods); err != nil {
		return fmt.Errorf("read SOCKS authentication methods: %w", err)
	}
	for _, method := range methods {
		if method == AuthNoAuth {
			if _, err := rw.Write([]byte{Version5, AuthNoAuth}); err != nil {
				return fmt.Errorf("write SOCKS method selection: %w", err)
			}
			return nil
		}
	}

	if _, err := rw.Write([]byte{Version5, AuthNoAcceptable}); err != nil {
		return fmt.Errorf("write SOCKS method selection: %w", err)
	}
	return errors.New("SOCKS client does not offer no-authentication")
}

func ReadRequest(r io.Reader) (Request, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Request{}, fmt.Errorf("read SOCKS request header: %w", err)
	}
	if header[0] != Version5 {
		return Request{}, &RequestError{
			Reply: ReplyGeneralFailure,
			Err:   fmt.Errorf("unsupported SOCKS version: %d", header[0]),
		}
	}
	if header[2] != 0 {
		return Request{}, &RequestError{
			Reply: ReplyGeneralFailure,
			Err:   fmt.Errorf("invalid SOCKS reserved byte: %d", header[2]),
		}
	}

	host, err := readAddress(r, header[3])
	if err != nil {
		return Request{}, err
	}
	var portBytes [2]byte
	if _, err := io.ReadFull(r, portBytes[:]); err != nil {
		return Request{}, fmt.Errorf("read SOCKS destination port: %w", err)
	}

	return Request{
		Command: header[1],
		Address: net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes[:])))),
	}, nil
}

func readAddress(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case AtypIPv4:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS IPv4 address: %w", err)
		}
		return net.IP(address).String(), nil
	case AtypIPv6:
		address := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS IPv6 address: %w", err)
		}
		return net.IP(address).String(), nil
	case AtypDomain:
		var length [1]byte
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return "", fmt.Errorf("read SOCKS domain length: %w", err)
		}
		if length[0] == 0 {
			return "", &RequestError{
				Reply: ReplyGeneralFailure,
				Err:   errors.New("SOCKS domain must not be empty"),
			}
		}
		address := make([]byte, length[0])
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS domain: %w", err)
		}
		return string(address), nil
	default:
		return "", &RequestError{
			Reply: ReplyAddressTypeNotSupported,
			Err:   fmt.Errorf("unsupported SOCKS address type: %d", atyp),
		}
	}
}

func WriteReply(w io.Writer, reply byte) error {
	_, err := w.Write([]byte{Version5, reply, 0x00, AtypIPv4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	return err
}

func ReplyForDialError(err error) byte {
	if errors.Is(err, context.DeadlineExceeded) {
		return ReplyTTLExpired
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ReplyTTLExpired
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return ReplyConnectionRefused
		}
		if errors.Is(opErr.Err, syscall.ENETUNREACH) {
			return ReplyNetworkUnreachable
		}
		if errors.Is(opErr.Err, syscall.EHOSTUNREACH) {
			return ReplyHostUnreachable
		}
	}

	return ReplyGeneralFailure
}
