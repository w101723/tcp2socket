package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestNegotiateSocks5(t *testing.T) {
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
			err := negotiateSocks5(&readWriter{Reader: bytes.NewReader(tt.input), Writer: &output})
			if (err != nil) != tt.wantErr {
				t.Fatalf("negotiateSocks5() error = %v, want error = %v", err, tt.wantErr)
			}
			if !bytes.Equal(output.Bytes(), tt.wantReply) {
				t.Fatalf("reply = %x, want %x", output.Bytes(), tt.wantReply)
			}
		})
	}
}

func TestReadSocksRequest(t *testing.T) {
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
			wantCommand: socksConnect,
			wantAddress: "127.0.0.1:9000",
		},
		{
			name:        "IPv6 connect",
			input:       append([]byte{0x05, 0x01, 0x00, 0x04}, append(net.ParseIP("2001:db8::1").To16(), 0x01, 0xbb)...),
			wantCommand: socksConnect,
			wantAddress: "[2001:db8::1]:443",
		},
		{
			name:        "domain connect",
			input:       append([]byte{0x05, 0x01, 0x00, 0x03, 0x0b}, append([]byte("example.com"), 0x01, 0xbb)...),
			wantCommand: socksConnect,
			wantAddress: "example.com:443",
		},
		{
			name:      "unsupported address type",
			input:     []byte{0x05, 0x01, 0x00, 0x09},
			wantReply: socksReplyAddressTypeNotSupported,
			wantErr:   true,
		},
		{
			name:      "invalid reserved byte",
			input:     []byte{0x05, 0x01, 0x01, 0x01, 127, 0, 0, 1, 0x00, 0x50},
			wantReply: socksReplyGeneralFailure,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := readSocksRequest(bytes.NewReader(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("readSocksRequest() error = %v, want error = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var requestErr *socksRequestError
				if !errors.As(err, &requestErr) {
					t.Fatalf("error type = %T, want *socksRequestError", err)
				}
				if requestErr.reply != tt.wantReply {
					t.Fatalf("reply = %d, want %d", requestErr.reply, tt.wantReply)
				}
				return
			}
			if request.command != tt.wantCommand || request.address != tt.wantAddress {
				t.Fatalf("request = %#v, want command=%d address=%q", request, tt.wantCommand, tt.wantAddress)
			}
		})
	}
}

func TestWriteSocksReply(t *testing.T) {
	var output bytes.Buffer
	if err := writeSocksReply(&output, socksReplyConnectionRefused); err != nil {
		t.Fatalf("writeSocksReply() error = %v", err)
	}
	want := []byte{0x05, socksReplyConnectionRefused, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	if !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("reply = %x, want %x", output.Bytes(), want)
	}
}

func TestServeSocksStdioConnect(t *testing.T) {
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
	go func() {
		serverDone <- serveSocksStdio(context.Background(), server, time.Second, false)
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

type readWriter struct {
	io.Reader
	io.Writer
}
