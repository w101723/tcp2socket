package transport

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
)

// Stdio owns its handles. Close interrupts pipe I/O; no deadline capability is advertised.
type Stdio struct {
	reader  io.ReadCloser
	writer  io.WriteCloser
	once    sync.Once
	err     error
	initErr error
	cleanup []func() error
}

func NewStdio(reader io.ReadCloser, writer io.WriteCloser) *Stdio {
	s := &Stdio{reader: reader, writer: writer}
	if f, ok := reader.(*os.File); ok {
		prepared, cleanup, err := pollableFile(f)
		if err != nil {
			s.initErr = err
		} else {
			s.reader = prepared
			s.cleanup = append(s.cleanup, cleanup)
		}
	}
	if f, ok := writer.(*os.File); ok {
		prepared, cleanup, err := pollableFile(f)
		if err != nil {
			s.initErr = errors.Join(s.initErr, err)
		} else {
			s.writer = prepared
			s.cleanup = append(s.cleanup, cleanup)
		}
	}
	return s
}

// Inherited blocking descriptors are not registered with Go's poller. Duplicate
// and register them before I/O so Close can interrupt an idle pipe or terminal.
func pollableFile(original *os.File) (*os.File, func() error, error) {
	fd := int(original.Fd())
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFL, 0)
	if errno != 0 {
		return nil, nil, errno
	}
	duplicate, err := syscall.Dup(fd)
	if err != nil {
		return nil, nil, err
	}
	syscall.CloseOnExec(duplicate)
	if err := syscall.SetNonblock(duplicate, true); err != nil {
		_ = syscall.Close(duplicate)
		return nil, nil, err
	}
	file := os.NewFile(uintptr(duplicate), original.Name())
	cleanup := func() error {
		var err error
		if flags&syscall.O_NONBLOCK == 0 {
			err = syscall.SetNonblock(fd, false)
		}
		return errors.Join(err, original.Close())
	}
	return file, cleanup, nil
}

func (s *Stdio) Read(p []byte) (int, error) {
	if s.initErr != nil {
		return 0, s.initErr
	}
	return s.reader.Read(p)
}
func (s *Stdio) Write(p []byte) (int, error) {
	if s.initErr != nil {
		return 0, s.initErr
	}
	return s.writer.Write(p)
}
func (s *Stdio) Close() error {
	s.once.Do(func() {
		s.err = errors.Join(s.reader.Close(), s.writer.Close())
		for _, cleanup := range s.cleanup {
			s.err = errors.Join(s.err, cleanup())
		}
	})
	return s.err
}
