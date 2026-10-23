package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCLIHelpAndExitCodes tests flags, help text, exit code 2 on missing/invalid arguments
func TestCLIHelpAndExitCodes(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantExit   int
		wantSubstr string
	}{
		{
			name:       "HelpFlag",
			args:       []string{"-h"},
			wantExit:   0,
			wantSubstr: "tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy",
		},
		{
			name:       "HelpLongFlag",
			args:       []string{"--help"},
			wantExit:   0,
			wantSubstr: "tcp2socket - TCP / Unix Socket / STDIO bidirectional proxy",
		},
		{
			name:       "ProxyMissingArgs",
			args:       []string{},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ProxyMissingTarget",
			args:       []string{"-l", ":8080"},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ProxyMissingListen",
			args:       []string{"-t", ":8080"},
			wantExit:   2,
			wantSubstr: "Options:",
		},
		{
			name:       "ConnectMissingTarget",
			args:       []string{"connect"},
			wantExit:   2,
			wantSubstr: "tcp2socket connect",
		},
		{
			name:       "SocksStdioExtraArg",
			args:       []string{"socks-stdio", "extra"},
			wantExit:   2,
			wantSubstr: "tcp2socket socks-stdio",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := boundedCommand(t, tc.args...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			err := cmd.Run()

			exitCode := 0
			if err != nil {
				if exitErr, ok := err.(*exec.ExitError); ok {
					exitCode = exitErr.ExitCode()
				} else {
					t.Fatalf("command failed with non-exit error: %v", err)
				}
			}

			if exitCode != tc.wantExit {
				t.Fatalf("expected exit code %d, got %d (stderr: %q)", tc.wantExit, exitCode, stderr.String())
			}

			combined := stdout.String() + stderr.String()
			if !strings.Contains(combined, tc.wantSubstr) {
				t.Fatalf("expected output to contain %q, got: %q", tc.wantSubstr, combined)
			}
		})
	}
}

// TestConnectStdoutNoPollution ensures connect mode and shorthand mode never pollute stdout
func TestConnectStdoutNoPollution(t *testing.T) {
	// Start an echo server on TCP
	targetPort := getFreeTCPPort(t)
	targetAddr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	ln, err := net.Listen("tcp", targetAddr)
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	runCases := []struct {
		name string
		args []string
	}{
		{name: "ConnectSubcommand", args: []string{"connect", "-v", "tcp://" + targetAddr}},
		{name: "ShorthandURI", args: []string{"tcp://" + targetAddr}},
		{name: "ShorthandHostPort", args: []string{targetAddr}},
	}

	for _, tc := range runCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := boundedCommand(t, tc.args...)
			stdinPipe, err := cmd.StdinPipe()
			if err != nil {
				t.Fatalf("StdinPipe failed: %v", err)
			}
			var stdoutBuf, stderrBuf bytes.Buffer
			cmd.Stdout = &stdoutBuf
			cmd.Stderr = &stderrBuf

			if err := cmd.Start(); err != nil {
				t.Fatalf("process start failed: %v", err)
			}

			testPayload := []byte("hello-clean-stdout-verify-12345\n")
			_, _ = stdinPipe.Write(testPayload)
			_ = stdinPipe.Close()

			waitDone := make(chan error, 1)
			go func() {
				waitDone <- cmd.Wait()
			}()

			select {
			case err := <-waitDone:
				if err != nil {
					t.Fatalf("process exited with error: %v, stderr: %s", err, stderrBuf.String())
				}
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("process timed out waiting for exit")
			}

			// Verify stdout contains ONLY the echoed payload, byte-for-byte!
			if !bytes.Equal(stdoutBuf.Bytes(), testPayload) {
				t.Fatalf("stdout was polluted! Expected %q, got %q", string(testPayload), stdoutBuf.String())
			}
		})
	}
}

// TestConnectUnixShorthand tests connect mode with unix socket shorthand
func TestConnectUnixShorthand(t *testing.T) {
	sockPath := tempUnixPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on unix socket: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	cmd := boundedCommand(t, sockPath)
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe failed: %v", err)
	}
	var stdoutBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("process start failed: %v", err)
	}

	testPayload := []byte("unix-shorthand-test\n")
	_, _ = stdinPipe.Write(testPayload)
	_ = stdinPipe.Close()

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("process exited with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("process timed out")
	}

	if !bytes.Equal(stdoutBuf.Bytes(), testPayload) {
		t.Fatalf("expected stdout %q, got %q", string(testPayload), stdoutBuf.String())
	}
}

// TestProxyCombinations tests TCP->TCP, TCP->Unix, Unix->TCP, Unix->Unix combinations
// using both explicit (-listen/-target) and short flags (-l/-t).
func TestProxyCombinations(t *testing.T) {
	combos := []struct {
		name          string
		listenNet     string
		targetNet     string
		listenPrefix  string
		targetPrefix  string
		useShortFlags bool
	}{
		{
			name:          "TCP_to_TCP_ShortFlags",
			listenNet:     "tcp",
			targetNet:     "tcp",
			useShortFlags: true,
		},
		{
			name:          "TCP_to_Unix_LongFlags",
			listenNet:     "tcp",
			targetNet:     "unix",
			useShortFlags: false,
		},
		{
			name:          "Unix_to_TCP_ShortFlags",
			listenNet:     "unix",
			targetNet:     "tcp",
			useShortFlags: true,
		},
		{
			name:          "Unix_to_Unix_LongFlags",
			listenNet:     "unix",
			targetNet:     "unix",
			useShortFlags: false,
		},
	}

	for _, tc := range combos {
		t.Run(tc.name, func(t *testing.T) {
			// Setup Target echo server
			var targetListenAddr, targetDialAddr string
			if tc.targetNet == "tcp" {
				port := getFreeTCPPort(t)
				targetListenAddr = fmt.Sprintf("127.0.0.1:%d", port)
				targetDialAddr = targetListenAddr
			} else {
				targetListenAddr = tempUnixPath(t)
				targetDialAddr = targetListenAddr
			}

			targetLn, err := net.Listen(tc.targetNet, targetListenAddr)
			if err != nil {
				t.Fatalf("failed to listen on target %s: %v", targetListenAddr, err)
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

			// Setup Listen endpoint for proxy
			var proxyListenArg, clientDialAddr string
			if tc.listenNet == "tcp" {
				port := getFreeTCPPort(t)
				proxyListenArg = fmt.Sprintf("tcp://127.0.0.1:%d", port)
				clientDialAddr = fmt.Sprintf("127.0.0.1:%d", port)
			} else {
				uPath := tempUnixPath(t)
				proxyListenArg = "unix://" + uPath
				clientDialAddr = uPath
			}

			var targetArg string
			if tc.targetNet == "tcp" {
				targetArg = "tcp://" + targetDialAddr
			} else {
				targetArg = "unix://" + targetDialAddr
			}

			var args []string
			if tc.useShortFlags {
				args = []string{"-l", proxyListenArg, "-t", targetArg}
			} else {
				args = []string{"--listen", proxyListenArg, "--target", targetArg}
			}

			proc := startProcess(t, args...)
			if err := proc.Start(); err != nil {
				t.Fatalf("failed to start proxy: %v", err)
			}

			if err := waitForEndpoint(tc.listenNet, clientDialAddr, 2*time.Second); err != nil {
				t.Fatalf("proxy did not start listening: %v", err)
			}

			// Connect client and roundtrip data
			clientConn, err := net.DialTimeout(tc.listenNet, clientDialAddr, 1*time.Second)
			if err != nil {
				t.Fatalf("failed to dial proxy: %v", err)
			}
			defer clientConn.Close()

			msg := []byte("proxy-test-" + tc.name)
			if _, err := clientConn.Write(msg); err != nil {
				t.Fatalf("failed to write: %v", err)
			}

			buf := make([]byte, len(msg))
			_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.ReadFull(clientConn, buf); err != nil {
				t.Fatalf("failed to read echoed data: %v", err)
			}

			if !bytes.Equal(buf, msg) {
				t.Fatalf("echo mismatch: got %q, want %q", string(buf), string(msg))
			}
		})
	}
}

// TestStreaming1MiBHash tests 1MiB continuous stream through proxy and verifies SHA-256
func TestStreaming1MiBHash(t *testing.T) {
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
	proc := startProcess(t, "-l", ":"+fmt.Sprintf("%d", proxyPort), "-t", targetAddr)
	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}

	if err := waitForEndpoint("tcp", proxyAddr, 2*time.Second); err != nil {
		t.Fatalf("proxy did not start listening: %v", err)
	}

	clientConn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	defer clientConn.Close()

	const totalSize = 1024 * 1024                           // 1MiB
	chunk := bytes.Repeat([]byte("0123456789abcdef"), 1024) // 16KB chunk

	sendHasher := sha256.New()
	recvHasher := sha256.New()

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer goroutine
	go func() {
		defer wg.Done()
		sent := 0
		for sent < totalSize {
			toSend := len(chunk)
			if totalSize-sent < toSend {
				toSend = totalSize - sent
			}
			sendHasher.Write(chunk[:toSend])
			_, err := clientConn.Write(chunk[:toSend])
			if err != nil {
				t.Errorf("write error: %v", err)
				return
			}
			sent += toSend
		}
		// Half close if supported
		if cw, ok := clientConn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()

	// Reader goroutine
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		received := 0
		for received < totalSize {
			_ = clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, err := clientConn.Read(buf)
			if n > 0 {
				recvHasher.Write(buf[:n])
				received += n
			}
			if err != nil {
				if err == io.EOF && received == totalSize {
					break
				}
				t.Errorf("read error: %v (received %d/%d)", err, received, totalSize)
				break
			}
		}
	}()

	wg.Wait()

	sendSum := hex.EncodeToString(sendHasher.Sum(nil))
	recvSum := hex.EncodeToString(recvHasher.Sum(nil))
	if sendSum != recvSum {
		t.Fatalf("hash mismatch! Sent SHA256: %s, Recv SHA256: %s", sendSum, recvSum)
	}
}

// TestConcurrent8Streams tests 8 parallel streams transferring data simultaneously
func TestConcurrent8Streams(t *testing.T) {
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
	proc := startProcess(t, "-l", ":"+fmt.Sprintf("%d", proxyPort), "-t", targetAddr)
	if err := proc.Start(); err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}

	if err := waitForEndpoint("tcp", proxyAddr, 2*time.Second); err != nil {
		t.Fatalf("proxy did not start listening: %v", err)
	}

	const concurrency = 8
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("stream %d dial failed: %w", streamID, err)
				return
			}
			defer conn.Close()

			payload := fmt.Sprintf("stream-data-%02d-%s", streamID, strings.Repeat("x", 4096))
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

			if _, err := conn.Write([]byte(payload)); err != nil {
				errCh <- fmt.Errorf("stream %d write failed: %w", streamID, err)
				return
			}

			readBuf := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, readBuf); err != nil {
				errCh <- fmt.Errorf("stream %d read failed: %w", streamID, err)
				return
			}

			if string(readBuf) != payload {
				errCh <- fmt.Errorf("stream %d payload mismatch", streamID)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent stream failure: %v", err)
	}
}

// TestUnixSocketRefuseRegularFileAndSymlink verifies non-socket files and symlinks are protected
func TestUnixSocketRefuseRegularFileAndSymlink(t *testing.T) {
	t.Run("RefuseRegularFile", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "t2s_reg_*.txt")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		filePath := tempFile.Name()
		_ = tempFile.Close()
		defer os.Remove(filePath)

		targetPort := getFreeTCPPort(t)
		cmd := boundedCommand(t, "-l", "unix://"+filePath, "-t", fmt.Sprintf("127.0.0.1:%d", targetPort))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err = cmd.Run()

		if err == nil {
			t.Fatalf("expected error when listening on regular file path, got success")
		}

		if !strings.Contains(stderr.String(), "refusing to remove non-socket path") {
			t.Fatalf("expected 'refusing to remove non-socket path' in stderr, got: %q", stderr.String())
		}

		// Ensure file was NOT deleted!
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Fatalf("regular file was deleted unexpectedly")
		}
	})

	t.Run("RefuseSymlink", func(t *testing.T) {
		targetFile, err := os.CreateTemp("", "t2s_symlink_target_*.txt")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		targetPath := targetFile.Name()
		_ = targetFile.Close()
		defer os.Remove(targetPath)

		symlinkPath := filepath.Join(os.TempDir(), fmt.Sprintf("t2s_symlink_%d_%d.sock", os.Getpid(), time.Now().UnixNano()))
		if err := os.Symlink(targetPath, symlinkPath); err != nil {
			t.Fatalf("failed to create symlink: %v", err)
		}
		defer os.Remove(symlinkPath)

		targetPort := getFreeTCPPort(t)
		cmd := boundedCommand(t, "-l", "unix://"+symlinkPath, "-t", fmt.Sprintf("127.0.0.1:%d", targetPort))
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		err = cmd.Run()

		if err == nil {
			t.Fatalf("expected error when listening on symlink path, got success")
		}

		if !strings.Contains(stderr.String(), "refusing to remove non-socket path") {
			t.Fatalf("expected 'refusing to remove non-socket path' in stderr, got: %q", stderr.String())
		}

		// Ensure symlink and target were NOT deleted
		if _, err := os.Lstat(symlinkPath); os.IsNotExist(err) {
			t.Fatalf("symlink was deleted unexpectedly")
		}
		if _, err := os.Stat(targetPath); os.IsNotExist(err) {
			t.Fatalf("symlink target was deleted unexpectedly")
		}
	})
}
