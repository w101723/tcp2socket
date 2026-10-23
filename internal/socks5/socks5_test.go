package socks5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"tcp2socket/internal/transport"
)

type readWriter struct {
	io.Reader
	io.Writer
}

func TestNegotiate(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		wantReply []byte
		wantErr   bool
	}{
		{
			name:      "selects no authentication",
			input:     []byte{0x05, 0x02, 0x02, 0x00},
			wantReply: []byte{0x05, 0x00},
		},
		{
			name:      "rejects unsupported authentication",
			input:     []byte{0x05, 0x01, 0x02},
			wantReply: []byte{0x05, 0xff},
			wantErr:   true,
		},
		{
			name:      "rejects zero methods",
			input:     []byte{0x05, 0x00},
			wantReply: []byte{0x05, 0xff},
			wantErr:   true,
		},
		{
			name:    "rejects SOCKS4",
			input:   []byte{0x04, 0x01, 0x00},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Negotiate(&readWriter{Reader: bytes.NewReader(tt.input), Writer: &output})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Negotiate() error = %v, want error = %v", err, tt.wantErr)
			}
			if !bytes.Equal(output.Bytes(), tt.wantReply) {
				t.Fatalf("reply = %x, want %x", output.Bytes(), tt.wantReply)
			}
		})
	}
}

func TestReadRequest(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		wantCommand byte
		wantAddress string
		wantReply   byte
		wantErr     bool
	}{
		{
			name:        "IPv4 connect",
			input:       []byte{0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1, 0x23, 0x28},
			wantCommand: CmdConnect,
			wantAddress: "127.0.0.1:9000",
		},
		{
			name:        "IPv6 connect",
			input:       append([]byte{0x05, 0x01, 0x00, 0x04}, append(net.ParseIP("2001:db8::1").To16(), 0x01, 0xbb)...),
			wantCommand: CmdConnect,
			wantAddress: "[2001:db8::1]:443",
		},
		{
			name:        "domain connect",
			input:       append([]byte{0x05, 0x01, 0x00, 0x03, 0x0b}, append([]byte("example.com"), 0x01, 0xbb)...),
			wantCommand: CmdConnect,
			wantAddress: "example.com:443",
		},
		{
			name:      "unsupported address type",
			input:     []byte{0x05, 0x01, 0x00, 0x09},
			wantReply: ReplyAddressTypeNotSupported,
			wantErr:   true,
		},
		{
			name:      "invalid reserved byte",
			input:     []byte{0x05, 0x01, 0x01, 0x01, 127, 0, 0, 1, 0x00, 0x50},
			wantReply: ReplyGeneralFailure,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := ReadRequest(bytes.NewReader(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ReadRequest() error = %v, want error = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var requestErr *RequestError
				if !errors.As(err, &requestErr) {
					t.Fatalf("error type = %T, want *RequestError", err)
				}
				if requestErr.Reply != tt.wantReply {
					t.Fatalf("reply = %d, want %d", requestErr.Reply, tt.wantReply)
				}
				return
			}
			if request.Command != tt.wantCommand || request.Address != tt.wantAddress {
				t.Fatalf("request = %#v, want command=%d address=%q", request, tt.wantCommand, tt.wantAddress)
			}
		})
	}
}

func TestWriteReply(t *testing.T) {
	var output bytes.Buffer
	if err := WriteReply(&output, ReplyConnectionRefused); err != nil {
		t.Fatalf("WriteReply() error = %v", err)
	}
	want := []byte{0x05, ReplyConnectionRefused, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("reply = %x, want %x", output.Bytes(), want)
	}
}

func TestServeConnect(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()

	targetDone := make(chan error, 1)
	go func() {
		conn, err := target.Accept()
		if err != nil {
			targetDone <- err
			return
		}
		defer conn.Close()
		_, err = io.Copy(conn, conn)
		targetDone <- err
	}()

	server, client := net.Pipe()
	serverDone := make(chan error, 1)
	dialer := &transport.DirectDialer{Timeout: time.Second}
	go func() {
		serverDone <- Serve(context.Background(), server, dialer, Options{})
	}()

	if _, err := client.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	assertRead(t, client, []byte{0x05, 0x00})

	host, portString, err := net.SplitHostPort(target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatal(err)
	}
	ip := net.ParseIP(host).To4()
	request := append([]byte{0x05, 0x01, 0x00, 0x01}, ip...)
	request = append(request, byte(port>>8), byte(port))
	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}
	assertRead(t, client, []byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	payload := []byte("SOCKS5 relay payload")
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}
	assertRead(t, client, payload)

	_ = client.Close()
	waitForResult(t, targetDone)
	waitForResult(t, serverDone)
}

// TestShortPackets tests short packets / EOF during handshake.
func TestShortPackets(t *testing.T) {
	shortInputs := [][]byte{
		{},
		{0x05},
		{0x05, 0x01},
		{0x05, 0x01, 0x00, 0x01},
		{0x05, 0x01, 0x00, 0x01, 127, 0, 0},
		{0x05, 0x01, 0x00, 0x03},
		{0x05, 0x01, 0x00, 0x03, 0x05, 'a', 'b'},
	}

	for i, input := range shortInputs {
		t.Run(fmt.Sprintf("short_input_%d", i), func(t *testing.T) {
			server, client := net.Pipe()
			go func() {
				_ = client.Close()
			}()
			_ = Serve(context.Background(), server, &transport.DirectDialer{}, Options{})
		})

		t.Run(fmt.Sprintf("negotiate_short_%d", i), func(t *testing.T) {
			var out bytes.Buffer
			_ = Negotiate(&readWriter{Reader: bytes.NewReader(input), Writer: &out})
		})

		t.Run(fmt.Sprintf("read_request_short_%d", i), func(t *testing.T) {
			_, _ = ReadRequest(bytes.NewReader(input))
		})
	}
}

// TestDialRefused tests dial failure handling and proper SOCKS reply code.
type mockDialer struct {
	err error
}

func (m *mockDialer) DialContext(ctx context.Context, target transport.Target) (transport.Stream, error) {
	return nil, m.err
}

func TestDialRefused(t *testing.T) {
	server, client := net.Pipe()
	dialer := &mockDialer{
		err: &net.OpError{
			Op:  "dial",
			Net: "tcp",
			Err: syscall.ECONNREFUSED,
		},
	}

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- Serve(context.Background(), server, dialer, Options{})
	}()

	// 1. Greeting
	if _, err := client.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatal(err)
	}
	assertRead(t, client, []byte{0x05, 0x00})

	// 2. Connect to 127.0.0.1:1234
	req := []byte{0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1, 0x04, 0xd2}
	if _, err := client.Write(req); err != nil {
		t.Fatal(err)
	}

	// 3. Must receive ConnectionRefused reply (0x05)
	assertRead(t, client, []byte{0x05, ReplyConnectionRefused, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	err := <-serverDone
	if err == nil {
		t.Fatalf("expected dial error, got nil")
	}
}

// TestHandshakeCancel verifies context cancellation aborts Serve handshake.
func TestHandshakeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server, client := net.Pipe()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, server, &transport.DirectDialer{}, Options{})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
			t.Logf("Serve returned error on cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not exit on context cancellation")
	}
}

func FuzzNegotiate(f *testing.F) {
	f.Add([]byte{0x05, 0x01, 0x00})
	f.Add([]byte{0x05, 0x02, 0x02, 0x00})
	f.Add([]byte{0x04, 0x01, 0x00})
	f.Add([]byte{0x05, 0x00})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var out bytes.Buffer
		_ = Negotiate(&readWriter{Reader: bytes.NewReader(data), Writer: &out})
	})
}

func FuzzReadRequest(f *testing.F) {
	f.Add([]byte{0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1, 0x23, 0x28})
	f.Add([]byte{0x05, 0x01, 0x00, 0x03, 0x07, 'e', 'x', 'a', 'm', 'p', 'l', 'e', 0x01, 0xbb})
	f.Add([]byte{0x05, 0x01, 0x00, 0x09})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadRequest(bytes.NewReader(data))
	})
}

func assertRead(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read = %x, want %x", got, want)
	}
}

func waitForResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection to close")
	}
}
