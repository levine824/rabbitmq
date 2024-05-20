package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed performs any operation on the closed pool will return this error.
	ErrPoolClosed = errors.New("rabbitmq: connection pool is closed")

	// ErrPoolTimeout timed out waiting to get a connection from the pool.
	ErrPoolTimeout = errors.New("rabbitmq: connection pool timeout")

	// ErrPoolExhausted is returned when the maximum number of connections in the pool has been reached.
	ErrPoolExhausted = errors.New("rabbitmq: connection exhausted")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was not found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	Totals uint32 // number of total connections in the pool
	Idles  uint32 // number of idle connections in the pool
	Stales uint32 // number of stale connections removed from the pool
}

// Pool is the interface that must be implemented by a connection pool.
type Pool interface {
	Get(context.Context) (*Connection, error)
	Put(context.Context, *Connection)

	Stats() *Stats

	Close() error
}

type ConnectionPoolConfig struct {
	Dial func() (*Connection, error)

	MinIdle     int
	MaxIdle     int
	MaxOpen     int
	MaxIdleTime time.Duration
	MaxLifeTime time.Duration
	PoolTimeout time.Duration
}

type ConnectionPool struct {
	cfg *ConnectionPoolConfig

	queue chan struct{}

	m sync.Mutex

	connections     []*Connection
	idleConnections []*Connection

	numOpen int
	numIdle int

	stats Stats

	closed int32
}

func NewConnectionPool(cfg *ConnectionPoolConfig) *ConnectionPool {
	p := &ConnectionPool{
		cfg:             cfg,
		queue:           make(chan struct{}, cfg.MaxOpen),
		connections:     make([]*Connection, cfg.MaxOpen),
		idleConnections: make([]*Connection, cfg.MaxIdle),
	}

	p.m.Lock()
	p.checkMinIdle()
	p.m.Unlock()

	return p
}

// Get returns a single connection by either opening a new connection or
// returning an existing connection from the connection pool. Connection will
// block until either a connection is returned or ctx is canceled.
func (p *ConnectionPool) Get(ctx context.Context) (*Connection, error) {
	if p.isClosed() {
		return nil, ErrPoolClosed
	}

	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	for {
		p.m.Lock()
		conn, err := p.popIdle()
		p.m.Unlock()

		if err != nil {
			p.freeTurn()
			return nil, err
		}

		if conn == nil {
			break
		}

		if !p.isHealthyConnection(conn) {
			atomic.AddUint32(&p.stats.Stales, 1)
			_ = p.remove(conn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return conn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	conn, err := p.newConnection()
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return conn, nil
}

// Put puts the connection back to the pool or closes it.
func (p *ConnectionPool) Put(_ context.Context, c *Connection) {
	var shouldClose bool

	p.m.Lock()
	if p.cfg.MaxIdle == 0 || p.numIdle < p.cfg.MaxIdle {
		c.SetReturnedAt(time.Now())
		p.idleConnections = append(p.idleConnections, c)
		p.numIdle++
	} else {
		shouldClose = true
	}
	p.m.Unlock()

	p.freeTurn()

	if shouldClose {
		_ = p.remove(c)
	}
}

// Remove removes the connection from the pool and closes it.
func (p *ConnectionPool) Remove(_ context.Context, c *Connection) {
	_ = p.remove(c)
}

// Stats returns a statistic that contains pool state information and accumulated
// stats.
func (p *ConnectionPool) Stats() *Stats {
	p.m.Lock()
	totals := p.numOpen
	idles := p.numIdle
	p.m.Unlock()

	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),
		Totals:   uint32(totals),
		Idles:    uint32(idles),
		Stales:   atomic.LoadUint32(&p.stats.Stales),
	}
}

// Close returns the first error occurred to close the pool.
func (p *ConnectionPool) Close() error {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return ErrPoolClosed
	}

	var firstErr error

	p.m.Lock()
	for _, conn := range p.connections {
		if err := p.closeConnection(conn, Abort); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.queue = nil
	p.connections = nil
	p.idleConnections = nil
	p.numOpen = 0
	p.numIdle = 0
	p.m.Unlock()

	return firstErr
}

func (p *ConnectionPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.cfg.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *ConnectionPool) freeTurn() {
	<-p.queue
}

func (p *ConnectionPool) popIdle() (*Connection, error) {
	if p.isClosed() {
		return nil, ErrPoolClosed
	}

	n := len(p.idleConnections)
	if n == 0 {
		return nil, nil
	}

	conn := p.idleConnections[0]
	copy(p.idleConnections, p.idleConnections[1:])
	p.idleConnections = p.idleConnections[:n-1]
	p.numIdle--

	p.checkMinIdle()

	return conn, nil
}

func (p *ConnectionPool) checkMinIdle() {
	if p.cfg.MinIdle == 0 {
		return
	}

	for p.numOpen < p.cfg.MaxOpen && p.numIdle < p.cfg.MinIdle {
		select {
		case p.queue <- struct{}{}:
			p.numOpen++
			p.numIdle++
			go func() {
				err := p.addIdle()
				if err != nil && err != ErrPoolClosed {
					p.m.Lock()
					p.numOpen--
					p.numIdle--
					p.m.Unlock()
				}

				p.freeTurn()
			}()
		default:
			return
		}
	}
}

func (p *ConnectionPool) addIdle() error {
	conn, err := p.openConnection()
	if err != nil {
		return err
	}

	if p.isClosed() {
		_ = p.closeConnection(conn, Abort)
		return ErrPoolClosed
	}

	p.m.Lock()
	defer p.m.Unlock()

	p.connections = append(p.connections, conn)
	p.idleConnections = append(p.idleConnections, conn)

	return nil
}

func (p *ConnectionPool) newConnection() (*Connection, error) {
	if p.isClosed() {
		return nil, ErrPoolClosed
	}

	p.m.Lock()
	if p.cfg.MaxOpen > 0 && p.numOpen >= p.cfg.MaxOpen {
		p.m.Unlock()
		return nil, ErrPoolExhausted
	}
	p.m.Unlock()

	conn, err := p.openConnection()
	if err != nil {
		return nil, err
	}

	p.m.Lock()
	defer p.m.Unlock()

	if p.cfg.MaxOpen > 0 && p.numOpen >= p.cfg.MaxOpen {
		_ = p.closeConnection(conn, Abort)
		return nil, ErrPoolExhausted
	}

	p.connections = append(p.connections, conn)
	p.numOpen++

	return conn, nil
}

func (p *ConnectionPool) openConnection() (*Connection, error) {
	conn, err := p.cfg.Dial()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (p *ConnectionPool) remove(c *Connection) error {
	p.removeConnection(c)
	return p.closeConnection(c, Immediate)
}

func (p *ConnectionPool) removeConnection(c *Connection) {
	p.m.Lock()
	defer p.m.Unlock()
	for i, v := range p.connections {
		if v == c {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			p.numOpen--
			break
		}
	}
}

func (p *ConnectionPool) closeConnection(c *Connection, s CloseStrategy) error {
	return c.close(s)
}

func (p *ConnectionPool) isClosed() bool {
	return int(atomic.LoadInt32(&p.closed)) == 1
}

func (p *ConnectionPool) isHealthyConnection(c *Connection) bool {
	now := time.Now()

	if p.cfg.MaxIdleTime > 0 && now.Sub(c.returnedAt) > p.cfg.MaxIdleTime {
		return false
	}

	if p.cfg.MaxLifeTime > 0 && now.Sub(c.createdAt) > p.cfg.MaxLifeTime {
		return false
	}

	return true
}
