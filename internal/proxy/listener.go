package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"tcp2socket/internal/transport"
)

type ServerConfig struct {
	ListenNetwork, ListenAddress     string
	Target                           transport.Target
	Dialer                           transport.Dialer
	UnixMode                         os.FileMode
	MaxConnections, MaxPendingAccept int
	ShutdownTimeout, IdleTimeout     time.Duration
	OnEvent                          func(Event)
}

type Event struct {
	ID            uint64
	Event, Reason string
	Source        string
	Result        Result
}

type accepted struct {
	conn net.Conn
	id   uint64
}

func ListenAndServe(ctx context.Context, cfg ServerConfig) error {
	if cfg.MaxConnections <= 0 || cfg.MaxPendingAccept < 0 || cfg.ShutdownTimeout < 0 || cfg.IdleTimeout < 0 {
		return errors.New("invalid proxy resource limits or timeout")
	}
	if cfg.Dialer == nil {
		return errors.New("proxy dialer is required")
	}
	ln, cleanup, err := listen(cfg.ListenNetwork, cfg.ListenAddress, cfg.UnixMode)
	if err != nil {
		return err
	}
	defer cleanup()
	stopListening := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stopListening()
	defer ln.Close()

	workCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queue := make(chan accepted, cfg.MaxPendingAccept)
	var workers sync.WaitGroup
	var ids atomic.Uint64
	var eventMu sync.Mutex
	emit := func(e Event) {
		if cfg.OnEvent != nil {
			eventMu.Lock()
			cfg.OnEvent(e)
			eventMu.Unlock()
		}
	}
	for i := 0; i < cfg.MaxConnections; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range queue {
				func() {
					defer item.conn.Close()
					e := Event{ID: item.id, Event: "closed", Source: item.conn.RemoteAddr().String()}
					if ctx.Err() != nil {
						e.Reason = "shutdown"
						emit(e)
						return
					}
					start := time.Now()
					dst, err := cfg.Dialer.DialContext(workCtx, cfg.Target)
					if err != nil {
						e.Reason = "dial_error"
						e.Result = Result{Reason: e.Reason, Err: err, Duration: time.Since(start)}
						emit(e)
						return
					}
					e.Result = Relay(workCtx, item.conn, dst, RelayOptions{IdleTimeout: cfg.IdleTimeout})
					e.Reason = e.Result.Reason
					emit(e)
				}()
			}
		}()
	}
	emit(Event{Event: "listening", Source: ln.Addr().String()})
	var acceptErr error
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil {
				acceptErr = fmt.Errorf("accept: %w", err)
			}
			break
		}
		item := accepted{conn: conn, id: ids.Add(1)}
		select {
		case queue <- item:
		default:
			_ = conn.Close()
			emit(Event{ID: item.id, Event: "rejected", Reason: "capacity", Source: conn.RemoteAddr().String()})
		}
	}
	_ = ln.Close()
	close(queue)
	finished := make(chan struct{})
	go func() { workers.Wait(); close(finished) }()
	timer := time.NewTimer(cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
		cancel()
		<-finished
	}
	return acceptErr
}
