package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	socksVersion5 = 0x05

	socksNoAuth       = 0x00
	socksNoAcceptable = 0xff

	socksConnect = 0x01

	socksATYPIPv4   = 0x01
	socksATYPDomain = 0x03
	socksATYPIPv6   = 0x04

	socksReplySucceeded               = 0x00
	socksReplyGeneralFailure          = 0x01
	socksReplyConnectionNotAllowed    = 0x02
	socksReplyNetworkUnreachable      = 0x03
	socksReplyHostUnreachable         = 0x04
	socksReplyConnectionRefused       = 0x05
	socksReplyTTLExpired              = 0x06
	socksReplyCommandNotSupported     = 0x07
	socksReplyAddressTypeNotSupported = 0x08
)

type socksRequest struct {
	command byte
	address string
}

type socksRequestError struct {
	reply byte
	err   error
}

func (e *socksRequestError) Error() string {
	return e.err.Error()
}

func (e *socksRequestError) Unwrap() error {
	return e.err
}

func runSocksStdio(dialTimeout time.Duration, verbose bool) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return serveSocksStdio(ctx, &stdioConn{reader: os.Stdin, writer: os.Stdout}, dialTimeout, verbose)
}

func serveSocksStdio(ctx context.Context, stdio net.Conn, dialTimeout time.Duration, verbose bool) error {
	if err := negotiateSocks5(stdio); err != nil {
		return err
	}

	request, err := readSocksRequest(stdio)
	if err != nil {
		var requestErr *socksRequestError
		if errors.As(err, &requestErr) {
			_ = writeSocksReply(stdio, requestErr.reply)
		} else {
			_ = writeSocksReply(stdio, socksReplyGeneralFailure)
		}
		return err
	}
	if request.command != socksConnect {
		_ = writeSocksReply(stdio, socksReplyCommandNotSupported)
		return fmt.Errorf("unsupported SOCKS command: %d", request.command)
	}

	dialer := net.Dialer{Timeout: dialTimeout}
	dst, err := dialer.DialContext(ctx, "tcp", request.address)
	if err != nil {
		reply := socksReplyForDialError(err)
		_ = writeSocksReply(stdio, reply)
		return fmt.Errorf("dial %s: %w", request.address, err)
	}
	defer dst.Close()

	if err := writeSocksReply(stdio, socksReplySucceeded); err != nil {
		return fmt.Errorf("write SOCKS success reply: %w", err)
	}

	if verbose {
		log.Printf("connected stdio SOCKS5 -> %s", request.address)
		defer log.Printf("closed stdio SOCKS5 -> %s", request.address)
	}

	proxy(stdio, dst)
	return nil
}

func negotiateSocks5(rw io.ReadWriter) error {
	var header [2]byte
	if _, err := io.ReadFull(rw, header[:]); err != nil {
		return fmt.Errorf("read SOCKS greeting: %w", err)
	}
	if header[0] != socksVersion5 {
		return fmt.Errorf("unsupported SOCKS version: %d", header[0])
	}
	if header[1] == 0 {
		if _, err := rw.Write([]byte{socksVersion5, socksNoAcceptable}); err != nil {
			return fmt.Errorf("write SOCKS method selection: %w", err)
		}
		return errors.New("SOCKS greeting contains no authentication methods")
	}

	methods := make([]byte, header[1])
	if _, err := io.ReadFull(rw, methods); err != nil {
		return fmt.Errorf("read SOCKS authentication methods: %w", err)
	}
	for _, method := range methods {
		if method == socksNoAuth {
			if _, err := rw.Write([]byte{socksVersion5, socksNoAuth}); err != nil {
				return fmt.Errorf("write SOCKS method selection: %w", err)
			}
			return nil
		}
	}

	if _, err := rw.Write([]byte{socksVersion5, socksNoAcceptable}); err != nil {
		return fmt.Errorf("write SOCKS method selection: %w", err)
	}
	return errors.New("SOCKS client does not offer no-authentication")
}

func readSocksRequest(r io.Reader) (socksRequest, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return socksRequest{}, fmt.Errorf("read SOCKS request header: %w", err)
	}
	if header[0] != socksVersion5 {
		return socksRequest{}, &socksRequestError{
			reply: socksReplyGeneralFailure,
			err:   fmt.Errorf("unsupported SOCKS version: %d", header[0]),
		}
	}
	if header[2] != 0 {
		return socksRequest{}, &socksRequestError{
			reply: socksReplyGeneralFailure,
			err:   fmt.Errorf("invalid SOCKS reserved byte: %d", header[2]),
		}
	}

	host, err := readSocksAddress(r, header[3])
	if err != nil {
		return socksRequest{}, err
	}
	var portBytes [2]byte
	if _, err := io.ReadFull(r, portBytes[:]); err != nil {
		return socksRequest{}, fmt.Errorf("read SOCKS destination port: %w", err)
	}

	return socksRequest{
		command: header[1],
		address: net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes[:])))),
	}, nil
}

func readSocksAddress(r io.Reader, atyp byte) (string, error) {
	switch atyp {
	case socksATYPIPv4:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS IPv4 address: %w", err)
		}
		return net.IP(address).String(), nil
	case socksATYPIPv6:
		address := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS IPv6 address: %w", err)
		}
		return net.IP(address).String(), nil
	case socksATYPDomain:
		var length [1]byte
		if _, err := io.ReadFull(r, length[:]); err != nil {
			return "", fmt.Errorf("read SOCKS domain length: %w", err)
		}
		if length[0] == 0 {
			return "", &socksRequestError{
				reply: socksReplyGeneralFailure,
				err:   errors.New("SOCKS domain must not be empty"),
			}
		}
		address := make([]byte, length[0])
		if _, err := io.ReadFull(r, address); err != nil {
			return "", fmt.Errorf("read SOCKS domain: %w", err)
		}
		return string(address), nil
	default:
		return "", &socksRequestError{
			reply: socksReplyAddressTypeNotSupported,
			err:   fmt.Errorf("unsupported SOCKS address type: %d", atyp),
		}
	}
}

func writeSocksReply(w io.Writer, reply byte) error {
	_, err := w.Write([]byte{socksVersion5, reply, 0x00, socksATYPIPv4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	return err
}

func socksReplyForDialError(err error) byte {
	if errors.Is(err, context.DeadlineExceeded) {
		return socksReplyTTLExpired
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return socksReplyTTLExpired
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return socksReplyConnectionRefused
		}
		if errors.Is(opErr.Err, syscall.ENETUNREACH) {
			return socksReplyNetworkUnreachable
		}
		if errors.Is(opErr.Err, syscall.EHOSTUNREACH) {
			return socksReplyHostUnreachable
		}
	}

	return socksReplyGeneralFailure
}

type stdioConn struct {
	reader io.Reader
	writer io.Writer
}

func (c *stdioConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *stdioConn) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *stdioConn) Close() error {
	return nil
}

func (c *stdioConn) LocalAddr() net.Addr {
	return stdioAddr("stdio-local")
}

func (c *stdioConn) RemoteAddr() net.Addr {
	return stdioAddr("stdio-remote")
}

func (c *stdioConn) SetDeadline(time.Time) error {
	return nil
}

func (c *stdioConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *stdioConn) SetWriteDeadline(time.Time) error {
	return nil
}

type stdioAddr string

func (a stdioAddr) Network() string {
	return "stdio"
}

func (a stdioAddr) String() string {
	return string(a)
}
