package proxy

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

func prepareUnixSocket(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat unix socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove non-socket path: %s", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("refusing to remove socket not owned by current user: %s", path)
	}
	conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("unix socket already active: %s", path)
	}
	// Only a refused connection proves a stale listener; timeouts and permission
	// errors must not authorize deleting another service's endpoint.
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("cannot confirm stale unix socket %s: %w", path, dialErr)
	}
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(info, current) {
		return fmt.Errorf("unix socket changed during inspection: %s", path)
	}
	return os.Remove(path)
}

func listen(network, address string, mode os.FileMode) (net.Listener, func(), error) {
	noop := func() {}
	if network != "unix" {
		ln, err := net.Listen(network, address)
		return ln, noop, err
	}
	if err := prepareUnixSocket(address); err != nil {
		return nil, noop, err
	}
	ln, err := net.Listen("unix", address)
	if err != nil {
		return nil, noop, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	info, err := os.Lstat(address)
	if err != nil {
		_ = ln.Close()
		return nil, noop, err
	}
	cleanup := func() {
		current, err := os.Lstat(address)
		if err == nil && os.SameFile(info, current) {
			_ = os.Remove(address)
		}
	}
	if err := os.Chmod(address, mode); err != nil {
		_ = ln.Close()
		cleanup()
		return nil, noop, err
	}
	return ln, cleanup, nil
}
