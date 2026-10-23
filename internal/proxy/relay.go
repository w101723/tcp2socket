package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"tcp2socket/internal/transport"
)

type RelayOptions struct {
	IdleTimeout time.Duration
	// STDIO has no independent read-side EOF notification to its caller.
	StopOnTargetEOF bool
}

type Result struct {
	SourceToTarget int64
	TargetToSource int64
	Duration       time.Duration
	Reason         string
	Err            error
}

type copyResult struct {
	direction int
	n         int64
	err       error
}

type activityReader struct {
	io.Reader
	touch func()
}

func (r activityReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.touch()
	}
	return n, err
}

type activityWriter struct {
	io.Writer
	touch func()
}

func (w activityWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		w.touch()
	}
	return n, err
}

// Relay retains the response direction after source EOF. All streams must honor
// the Stream.Close contract so cancellation can join both copy goroutines.
func Relay(ctx context.Context, source, target transport.Stream, opts RelayOptions) (result Result) {
	start := time.Now()
	defer func() { result.Duration = time.Since(start) }()
	closeBoth := func() { _ = source.Close(); _ = target.Close() }
	defer closeBoth()
	if err := ctx.Err(); err != nil {
		return Result{Reason: "canceled", Err: err}
	}

	var last atomic.Int64
	last.Store(0)
	touch := func() { last.Store(time.Since(start).Nanoseconds()) }
	done := make(chan copyResult, 2)
	copyOne := func(direction int, dst, src transport.Stream) {
		var n int64
		var err error
		if opts.IdleTimeout > 0 {
			n, err = io.CopyBuffer(activityWriter{dst, touch}, activityReader{src, touch}, make([]byte, 32*1024))
		} else {
			n, err = io.Copy(dst, src)
		}
		if err == nil {
			if cw, ok := dst.(transport.CloseWriter); ok {
				err = cw.CloseWrite()
			}
		}
		done <- copyResult{direction, n, err}
	}
	go copyOne(0, target, source)
	go copyOne(1, source, target)

	var timer *time.Timer
	var idle <-chan time.Time
	if opts.IdleTimeout > 0 {
		timer = time.NewTimer(opts.IdleTimeout)
		idle = timer.C
		defer timer.Stop()
	}
	remaining := 2
	stopping := false
	cancel := ctx.Done()
	stop := func(reason string, err error) {
		if stopping {
			return
		}
		stopping = true
		result.Reason, result.Err = reason, err
		cancel, idle = nil, nil
		closeBoth()
	}
	for remaining > 0 {
		select {
		case c := <-done:
			remaining--
			if c.direction == 0 {
				result.SourceToTarget = c.n
			} else {
				result.TargetToSource = c.n
			}
			if c.err != nil && !stopping {
				stop("io_error", fmt.Errorf("relay direction %d: %w", c.direction, c.err))
			} else if c.direction == 1 && opts.StopOnTargetEOF && !stopping {
				stop("target_eof", nil)
			}
		case <-cancel:
			stop("canceled", ctx.Err())
		case <-idle:
			left := opts.IdleTimeout - (time.Since(start) - time.Duration(last.Load()))
			if left > 0 {
				timer.Reset(left)
			} else {
				stop("idle_timeout", ErrIdleTimeout)
			}
		}
	}
	if result.Reason == "" {
		result.Reason = "eof"
	}
	return result
}

var ErrIdleTimeout = errors.New("connection idle timeout")
