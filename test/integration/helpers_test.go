package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

var unixCounter int64 = 0

// getFreeTCPPort asks kernel for an unused TCP port
func getFreeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free TCP port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// tempUnixPath returns a unique absolute path in os.TempDir() for a unix domain socket
func tempUnixPath(t *testing.T) string {
	t.Helper()
	id := atomic.AddInt64(&unixCounter, 1)
	p := filepath.Join(os.TempDir(), fmt.Sprintf("t2s_test_%d_%d_%d.sock", os.Getpid(), time.Now().UnixNano(), id))
	t.Cleanup(func() {
		_ = os.Remove(p)
	})
	return p
}

// Subprocess manages a running tcp2socket child process with cleanup guarantees.
// Synchronization uses a closed done channel so that multiple Wait/WaitTimed/Stop calls
// can safely observe completion without channel consumption races or deadlocks.
type Subprocess struct {
	Cmd       *exec.Cmd
	Cancel    context.CancelFunc
	done      chan struct{}
	waitErr   error
	ExitState *os.ProcessState
	mu        sync.Mutex
	stopOnce  sync.Once
}

// startProcess runs binPath with args, registering a cleanup hook on t
func startProcess(t *testing.T, args ...string) *Subprocess {
	return startProcessWithTimeout(t, 10*time.Second, args...)
}

// startProcessWithTimeout runs binPath with a specific overall timeout
func startProcessWithTimeout(t *testing.T, timeout time.Duration, args ...string) *Subprocess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	cmd := exec.CommandContext(ctx, binPath, args...)

	sub := &Subprocess{
		Cmd:    cmd,
		Cancel: cancel,
		done:   make(chan struct{}),
	}

	t.Cleanup(func() {
		sub.Stop()
	})

	return sub
}

// Start launches the command and tracks exit in background
func (s *Subprocess) Start() error {
	if err := s.Cmd.Start(); err != nil {
		return err
	}
	go func() {
		err := s.Cmd.Wait()
		s.mu.Lock()
		s.waitErr = err
		if s.Cmd.ProcessState != nil {
			s.ExitState = s.Cmd.ProcessState
		}
		s.mu.Unlock()
		close(s.done)
	}()
	return nil
}

// Signal sends an OS signal to the child process
func (s *Subprocess) Signal(sig os.Signal) error {
	if s.Cmd.Process == nil {
		return fmt.Errorf("process not started")
	}
	return s.Cmd.Process.Signal(sig)
}

// WaitTimed waits for the process to exit within timeout.
// If it times out, kills and waits for completion, returning a timeout error.
// Can be called multiple times safely.
func (s *Subprocess) WaitTimed(timeout time.Duration) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.waitErr
	case <-time.After(timeout):
		if s.Cmd.Process != nil {
			_ = s.Cmd.Process.Kill()
			<-s.done
		}
		return fmt.Errorf("process timed out after %v", timeout)
	}
}

// Stop cleanly terminates the child process.
// Safe to call multiple times or after WaitTimed has already completed.
func (s *Subprocess) Stop() {
	s.stopOnce.Do(func() {
		s.Cancel()
		select {
		case <-s.done:
			return
		default:
		}

		if s.Cmd.Process != nil {
			_ = s.Cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-s.done:
				return
			case <-time.After(500 * time.Millisecond):
				_ = s.Cmd.Process.Kill()
				<-s.done
			}
		}
	})
}

// waitForEndpoint tries connecting to a network endpoint until success or timeout
func waitForEndpoint(network, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout(network, address, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			time.Sleep(30 * time.Millisecond)
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s://%s", network, address)
}

// isFlagSupported checks if the binary supports a given command-line flag
func isFlagSupported(flagName string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, "-h")
	out, _ := cmd.CombinedOutput()
	return strings.Contains(string(out), flagName)
}

func boundedCommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return exec.CommandContext(ctx, binPath, args...)
}
