package s3client

import (
	"context"
	"net"
	"sync"
)

type connLimiter struct {
	mu      sync.Mutex
	limit   int
	current int
	waiters []chan struct{}
}

func newConnLimiter(limit int) *connLimiter {
	return &connLimiter{limit: limit}
}

func (l *connLimiter) acquire(ctx context.Context) error {
	if l == nil {
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		l.mu.Lock()
		noLimit := l.limit <= 0
		if noLimit || l.current < l.limit {
			l.current++
			l.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		l.waiters = append(l.waiters, ch)
		l.mu.Unlock()

		select {
		case <-ch:
			// retry acquisition with updated limits/state
		case <-ctx.Done():
			l.removeWaiter(ch)
			return ctx.Err()
		}
	}
}

func (l *connLimiter) release() {
	if l == nil {
		return
	}

	l.mu.Lock()
	if l.current > 0 {
		l.current--
	}
	if l.limit <= 0 || l.current < l.limit {
		l.notifyWaitersLocked()
	}
	l.mu.Unlock()
}

func (l *connLimiter) setLimit(limit int) {
	if l == nil {
		return
	}
	if limit < 0 {
		limit = 0
	}

	l.mu.Lock()
	changed := l.limit != limit
	l.limit = limit
	if changed && (limit <= 0 || l.current < limit) {
		l.notifyWaitersLocked()
	}
	l.mu.Unlock()
}

func (l *connLimiter) currentConnections() int {
	if l == nil {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

func (l *connLimiter) wrapConn(conn net.Conn) net.Conn {
	if l == nil {
		return conn
	}
	return &limitedConn{Conn: conn, release: l.release}
}

func (l *connLimiter) notifyWaitersLocked() {
	if len(l.waiters) == 0 {
		return
	}
	waiters := l.waiters
	l.waiters = nil
	for _, w := range waiters {
		close(w)
	}
}

func (l *connLimiter) removeWaiter(target chan struct{}) {
	if l == nil {
		return
	}
	l.mu.Lock()
	for i, ch := range l.waiters {
		if ch == target {
			l.waiters = append(l.waiters[:i], l.waiters[i+1:]...)
			break
		}
	}
	l.mu.Unlock()
}

type limitedConn struct {
	net.Conn
	release func()
}

func (c *limitedConn) Close() error {
	err := c.Conn.Close()
	if c.release != nil {
		c.release()
		c.release = nil
	}
	return err
}

