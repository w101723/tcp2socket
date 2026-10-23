package integration

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// performSocks5Handshake completes SOCKS5 no-auth greeting and connect to dstAddr
func performSocks5Handshake(r io.Reader, w io.Writer, dstAddr string) error {
	// Greeting: VER 5, NMETHODS 1, METHOD 0 (NO_AUTH)
	if _, err := w.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fmt.Errorf("write greeting: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(r, resp); err != nil {
		return fmt.Errorf("read greeting response: %w", err)
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		return fmt.Errorf("greeting rejected: %x", resp)
	}

	host, portStr, err := net.SplitHostPort(dstAddr)
	if err != nil {
		return fmt.Errorf("split host port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("parse port: %w", err)
	}

	ip := net.ParseIP(host).To4()
	if ip == nil {
		return fmt.Errorf("only IPv4 supported in test helper")
	}

	req := append([]byte{0x05, 0x01, 0x00, 0x01}, ip...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := w.Write(req); err != nil {
		return fmt.Errorf("write connect request: %w", err)
	}

	reply := make([]byte, 10)
	if _, err := io.ReadFull(r, reply); err != nil {
		return fmt.Errorf("read connect reply: %w", err)
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("socks connect failed with rep=%d", reply[1])
	}
	return nil
}

// TestSocksStdioRealOSPipe tests SOCKS5 over real os.Pipe (standard IO)
func TestSocksStdioRealOSPipe(t *testing.T) {
	// Start target echo server
	targetPort := getFreeTCPPort(t)
	targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	targetLn, err := net.Listen("tcp", targetAddr)
	if err != nil {
		t.Fatalf("failed to listen on target: %v", err)
	}
	defer targetLn.Close()

	go func() {
		for {
			c, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	// Create real OS pipes
	clientStdinReader, clientStdinWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdin pipe: %v", err)
	}
	defer clientStdinReader.Close()
	defer clientStdinWriter.Close()

	clientStdoutReader, clientStdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	defer clientStdoutReader.Close()
	defer clientStdoutWriter.Close()

	proc := startProcess(t, "socks-stdio")
	proc.Cmd.Stdin = clientStdinReader
	proc.Cmd.Stdout = clientStdoutWriter
	var stderrBuf bytes.Buffer
	proc.Cmd.Stderr = &stderrBuf

	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start socks-stdio: %v", err)
	}

	// Handshake over OS pipe
	if err := performSocks5Handshake(clientStdoutReader, clientStdinWriter, targetAddr); err != nil {
		t.Fatalf("socks handshake failed: %v", err)
	}

	// Send data and receive echoed data
	payload := []byte("socks5-real-os-pipe-data-payload-12345")
	if _, err := clientStdinWriter.Write(payload); err != nil {
		t.Fatalf("write payload failed: %v", err)
	}

	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(clientStdoutReader, buf); err != nil {
		t.Fatalf("read payload failed: %v", err)
	}

	if !bytes.Equal(buf, payload) {
		t.Fatalf("echoed data mismatch: got %q, want %q", string(buf), string(payload))
	}

	// Close stdin to signal end of stream
	_ = clientStdinWriter.Close()

	// In current baseline, socks-stdio uses stdioConn without closeWrite/context cancellation on EOF.
	// We wait up to 2s; if it exits cleanly, great. If not, WaitTimed cleanly terminates child.
	if err := proc.WaitTimed(2 * time.Second); err != nil {
		t.Fatalf("socks-stdio process did not exit boundedly on stdin close: %v (known baseline limitation)", err)
	}
}

// TestSocksHandshakeWaitingSIGTERM verifies that socks-stdio terminates cleanly on SIGTERM during handshake
func TestSocksHandshakeWaitingSIGTERM(t *testing.T) {
	proc := startProcess(t, "socks-stdio")
	stdinPipe, err := proc.Cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe failed: %v", err)
	}
	defer stdinPipe.Close()

	var stderrBuf bytes.Buffer
	proc.Cmd.Stderr = &stderrBuf

	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start socks-stdio: %v", err)
	}

	// Give it a moment to enter handshake read
	time.Sleep(100 * time.Millisecond)

	// Send SIGTERM while blocked in handshake
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	if err := proc.WaitTimed(2 * time.Second); err != nil {
		t.Fatalf("socks-stdio hung and did not terminate upon SIGTERM during handshake: %v", err)
	}
}

// TestDelayedResponseNoTruncation verifies that a slow target (500ms delay) is not truncated
func TestDelayedResponseNoTruncation(t *testing.T) {
	targetPort := getFreeTCPPort(t)
	targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	targetLn, err := net.Listen("tcp", targetAddr)
	if err != nil {
		t.Fatalf("failed to listen on target: %v", err)
	}
	defer targetLn.Close()

	expectedResponse := []byte("delayed-response-after-500ms-done!")

	go func() {
		c, err := targetLn.Accept()
		if err != nil {
			return
		}
		defer c.Close()

		reqBuf := make([]byte, 1024)
		n, _ := c.Read(reqBuf)
		if n > 0 {
			// Delay response by 500ms
			time.Sleep(500 * time.Millisecond)
			_, _ = c.Write(expectedResponse)
		}
	}()

	proc := startProcess(t, "connect", targetAddr)
	stdinPipe, err := proc.Cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe failed: %v", err)
	}
	var stdoutBuf bytes.Buffer
	proc.Cmd.Stdout = &stdoutBuf

	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start connect: %v", err)
	}

	// Send client request and immediately close stdin (half close)
	_, _ = stdinPipe.Write([]byte("get-data\n"))
	_ = stdinPipe.Close()

	if err := proc.WaitTimed(3 * time.Second); err != nil {
		t.Fatalf("connect timed out or failed: %v", err)
	}

	if !bytes.Equal(stdoutBuf.Bytes(), expectedResponse) {
		t.Fatalf("response truncated or mismatch! Got %q, want %q", stdoutBuf.String(), string(expectedResponse))
	}
}

// TestTargetEOFStdinOpenProcessExit verifies that when the target closes connection,
// the CLI process exits with code 0 and non-empty output even if stdin is kept open.
// Covers both 'connect' and 'socks-stdio' modes.
func TestTargetEOFStdinOpenProcessExit(t *testing.T) {
	cases := []struct {
		name    string
		isSocks bool
	}{
		{name: "ConnectMode", isSocks: false},
		{name: "SocksStdioMode", isSocks: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			targetPort := getFreeTCPPort(t)
			targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)
			targetLn, err := net.Listen("tcp", targetAddr)
			if err != nil {
				t.Fatalf("failed to listen on target: %v", err)
			}
			defer targetLn.Close()

			targetMsg := []byte("msg-from-target-then-eof\n")

			go func() {
				c, err := targetLn.Accept()
				if err != nil {
					return
				}
				if tc.isSocks {
					// In socks mode, target conn receives data forwarded by socks-stdio
				}
				// Write message and close target side
				_, _ = c.Write(targetMsg)
				_ = c.Close()
			}()

			// Setup OS pipe for stdin to simulate an open stdin stream (e.g. interactive user / SSH pipe)
			stdinReader, stdinWriter, err := os.Pipe()
			if err != nil {
				t.Fatalf("pipe failed: %v", err)
			}
			defer stdinReader.Close()
			defer stdinWriter.Close()

			stdoutReader, stdoutWriter, err := os.Pipe()
			if err != nil {
				t.Fatalf("pipe failed: %v", err)
			}
			defer stdoutReader.Close()
			defer stdoutWriter.Close()

			var proc *Subprocess
			if tc.isSocks {
				proc = startProcess(t, "socks-stdio")
			} else {
				proc = startProcess(t, "connect", targetAddr)
			}

			proc.Cmd.Stdin = stdinReader
			proc.Cmd.Stdout = stdoutWriter
			var stderrBuf bytes.Buffer
			proc.Cmd.Stderr = &stderrBuf

			if err := proc.Start(); err != nil {
				t.Fatalf("start failed: %v", err)
			}

			if tc.isSocks {
				// Perform SOCKS5 handshake over open pipes
				if err := performSocks5Handshake(stdoutReader, stdinWriter, targetAddr); err != nil {
					t.Fatalf("handshake failed: %v", err)
				}
			}

			// Read target's message from stdout
			receivedBuf := make([]byte, len(targetMsg))
			if _, err := io.ReadFull(stdoutReader, receivedBuf); err != nil {
				t.Fatalf("failed to read expected target output: %v", err)
			}
			if !bytes.Equal(receivedBuf, targetMsg) {
				t.Fatalf("output mismatch: got %q, want %q", string(receivedBuf), string(targetMsg))
			}

			// Do NOT close stdinWriter here. Target has sent EOF.
			// The process should notice the target's EOF and exit with exit code 0 boundedly.
			if err := proc.WaitTimed(2 * time.Second); err != nil {
				t.Fatalf("process hung waiting for stdin when target sent EOF: %v (exit code not 0)", err)
			}

			if proc.ExitState == nil || proc.ExitState.ExitCode() != 0 {
				code := -1
				if proc.ExitState != nil {
					code = proc.ExitState.ExitCode()
				}
				t.Fatalf("expected exit code 0, got %d (stderr: %q)", code, stderrBuf.String())
			}
		})
	}
}

// TestProxySIGTERMBoundedShutdown verifies bounded graceful shutdown on SIGTERM
// with existing active connections (using -shutdown-timeout 100ms if supported).
// First performs a roundtrip to confirm connection is fully established before sending SIGTERM.
func TestProxySIGTERMBoundedShutdown(t *testing.T) {
	targetPort := getFreeTCPPort(t)
	targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	targetLn, err := net.Listen("tcp", targetAddr)
	if err != nil {
		t.Fatalf("failed to listen on target: %v", err)
	}
	defer targetLn.Close()

	go func() {
		for {
			c, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	proxyPort := getFreeTCPPort(t)
	proxyAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)

	var args []string
	if isFlagSupported("shutdown-timeout") {
		args = []string{"-shutdown-timeout", "100ms", "-l", ":" + fmt.Sprintf("%d", proxyPort), "-t", targetAddr}
	} else {
		args = []string{"-l", ":" + fmt.Sprintf("%d", proxyPort), "-t", targetAddr}
	}

	proc := startProcess(t, args...)
	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}

	if err := waitForEndpoint("tcp", proxyAddr, 2*time.Second); err != nil {
		t.Fatalf("proxy failed to start listening: %v", err)
	}

	// Open an active connection
	conn, err := net.DialTimeout("tcp", proxyAddr, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer conn.Close()

	// Roundtrip confirmation: write data and verify echo before signaling
	confirmMsg := []byte("connection-established-check\n")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(confirmMsg); err != nil {
		t.Fatalf("failed to write confirmation data: %v", err)
	}
	replyBuf := make([]byte, len(confirmMsg))
	if _, err := io.ReadFull(conn, replyBuf); err != nil {
		t.Fatalf("failed to read confirmation reply: %v", err)
	}
	if !bytes.Equal(replyBuf, confirmMsg) {
		t.Fatalf("confirmation mismatch: got %q, want %q", replyBuf, confirmMsg)
	}

	// Send SIGTERM to proxy now that connection is confirmed active
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	if err := proc.WaitTimed(2 * time.Second); err != nil {
		t.Fatalf("proxy did not exit boundedly on SIGTERM while connections were open: %v", err)
	}
}

// TestActiveUnixSocketNotOverwritten verifies that a proxy refuses to overwrite an active Unix socket
func TestActiveUnixSocketNotOverwritten(t *testing.T) {
	sockPath := tempUnixPath(t)

	// Step 1: Start a real listening Unix server at sockPath
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to create active listening socket: %v", err)
	}
	defer ln.Close()

	// Step 2: Attempt to start tcp2socket with -l unix://sockPath
	targetPort := getFreeTCPPort(t)
	targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)

	proc := startProcess(t, "-l", "unix://"+sockPath, "-t", targetAddr)
	var stderrBuf bytes.Buffer
	proc.Cmd.Stderr = &stderrBuf

	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start process: %v", err)
	}

	// Process should exit immediately with error (must be bounded)
	err = proc.WaitTimed(2 * time.Second)
	if err == nil {
		t.Fatalf("expected error when trying to listen on active socket, but process exited with 0")
	}

	// Verify original active socket was not destroyed or hijacked
	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("active unix socket was hijacked or removed: %v", err)
	}
	_ = conn.Close()
}

// TestExpectedNewProxyFlags verifies presence and defaults of new proxy flags from PR01-03:
// max-connections (128), max-pending-accept (64), shutdown-timeout (10s)
func TestExpectedNewProxyFlags(t *testing.T) {
	proc := startProcess(t, "-h")
	var stdoutBuf, stderrBuf bytes.Buffer
	proc.Cmd.Stdout = &stdoutBuf
	proc.Cmd.Stderr = &stderrBuf

	if err := proc.Start(); err != nil {
		t.Fatalf("failed to run -h: %v", err)
	}

	if err := proc.WaitTimed(2 * time.Second); err != nil {
		t.Fatalf("timed out waiting for -h: %v", err)
	}

	helpText := stdoutBuf.String() + stderrBuf.String()

	flags := []struct {
		name         string
		defaultValue string
	}{
		{"max-connections", "128"},
		{"max-pending-accept", "64"},
		{"shutdown-timeout", "10s"},
	}

	for _, f := range flags {
		t.Run(f.name, func(t *testing.T) {
			if !strings.Contains(helpText, "-"+f.name) {
				t.Fatalf("flag -%s not found in help output", f.name)
			}
			if !strings.Contains(helpText, f.defaultValue) {
				t.Fatalf("flag -%s default value %s not found in help output", f.name, f.defaultValue)
			}
		})
	}
}
