package rabbitmq

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrConnectionClosed performs any operation on the closed connection will return this error.
	ErrConnectionClosed = errors.New("rabbitmq: connection is closed")

	// ErrConnectionWaitTimeout timed out waiting to get a channel from the connection.
	ErrConnectionWaitTimeout = errors.New("rabbitmq: connection wait timeout")
)

// Connection wraps an AMQP connection and caches AMQP channels.
type Connection struct {
	amqConnection *amqp.Connection
	createdAt     time.Time

	pool       *ConnectionPool
	returnedAt time.Time

	maxOpen int // 0 means unlimited
	maxIdle int // 0 means defaultMaxIdle
	// Total time waited for a channel.
	waitTimeout time.Duration

	m sync.Mutex // protects following fields
	// Next key to use in channels.
	nextChannel int
	channels    map[int]*Channel
	// number of opened channels
	numOpen int
	// size is maxIdle
	idleChannels chan *Channel

	closed int32 // atomic

	finalClosed int // 1 means fully closed
}

const (
	defaultMaxIdle     = 2
	defaultWaitTimeout = time.Second
)

type ConnectionConfig struct {
	MaxOpen     int
	MaxIdle     int
	WaitTimeout time.Duration
}

// Dial accepts a string in the AMQP URI format and returns a new connection with default config.
func Dial(url string) (*Connection, error) {
	return DialWithConfig(url, &ConnectionConfig{
		MaxIdle:     defaultMaxIdle,
		WaitTimeout: defaultWaitTimeout,
	})
}

func DialWithConfig(url string, cfg *ConnectionConfig) (*Connection, error) {
	c, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	conn := &Connection{
		amqConnection: c,
		createdAt:     time.Now(),
		maxOpen:       cfg.MaxOpen,
		maxIdle:       cfg.MaxIdle,
		waitTimeout:   cfg.WaitTimeout,
		channels:      make(map[int]*Channel, cfg.MaxOpen),
		idleChannels:  make(chan *Channel, cfg.MaxIdle),
	}

	go conn.init()

	return conn, nil
}

func (c *Connection) init() {
	for {
		ch, err := c.openChannel()
		if err != nil {
			return
		}

		select {
		case c.idleChannels <- ch:
		default:
			_ = c.removeChannel(ch)
			return
		}
	}
}

// Channel gets or creates a channel from the connection.
func (c *Connection) Channel(ctx context.Context) (*Channel, error) {
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	var timer *time.Timer
	timer = timers.Get().(*time.Timer)
	timer.Reset(c.waitTimeout)

	for {
		select {
		case <-timer.C:
			timers.Put(timer)
			return nil, ErrConnectionWaitTimeout
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			timers.Put(timer)
			return nil, ctx.Err()
		default:
			ch, err := c.channel()
			if err != nil {
				if !timer.Stop() {
					<-timer.C
				}
				timers.Put(timer)
				return nil, err
			}

			if ch != nil {
				if !timer.Stop() {
					<-timer.C
				}
				timers.Put(timer)
				return ch, nil
			}
		}
	}
}

func (c *Connection) channel() (*Channel, error) {
	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	select {
	case ch := <-c.idleChannels:
		return ch, nil
	default:
	}

	return c.openChannel()
}

func (c *Connection) openChannel() (*Channel, error) {
	ch, err := c.allocateChannel()
	if err != nil {
		return nil, err
	}

	if ch == nil {
		return nil, nil
	}

	if err := ch.open(); err != nil {
		c.releaseChannel(ch.id)
		return nil, err
	}

	return ch, nil
}

func (c *Connection) allocateChannel() (*Channel, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.isClosed() {
		return nil, ErrConnectionClosed
	}

	if c.maxOpen > 0 && c.numOpen >= c.maxOpen {
		return nil, nil
	}

	id := c.nextChannel
	c.nextChannel++

	ch := newChannel(c, id)

	c.channels[ch.id] = ch
	c.numOpen++

	return ch, nil
}

func (c *Connection) releaseChannel(id int) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.isClosed() {
		return
	}

	delete(c.channels, id)
	c.numOpen--
}

// CloseChannel puts the channel back or closes the channel only when the number
// of idle channels is greater than maxIdle.
func (c *Connection) CloseChannel(ch *Channel) {
	if c.isClosed() {
		c.m.Lock()
		if c.finalClosed == 1 {
			c.m.Unlock()
			return
		}
		c.deleteChannel(ch)
		if c.numOpen == 0 {
			close(c.idleChannels)
		}
		c.m.Unlock()
		return
	}

	select {
	case c.idleChannels <- ch:
		return
	default:
		_ = c.removeChannel(ch)
	}
}

func (c *Connection) removeChannel(ch *Channel) error {
	c.m.Lock()
	c.deleteChannel(ch)
	c.m.Unlock()
	return c.closeChannel(ch)
}

func (c *Connection) deleteChannel(ch *Channel) {
	if _, ok := c.channels[ch.id]; ok {
		delete(c.channels, ch.id)
		c.numOpen--
	}
}

func (c *Connection) closeChannel(ch *Channel) error {
	return ch.close()
}

// CloseStrategy determines how to close the connection.
type CloseStrategy int

const (
	// Immediate allows no permit for the channel and waits for all channels returned,
	// then close the connection.
	Immediate CloseStrategy = iota
	// Abort forces closing Connection now.
	Abort
)

// Close closes the connection or put it back to the pool.
func (c *Connection) Close() {
	if c.pool == nil {
		_ = c.close(Abort)
	}

	c.pool.Put(context.Background(), c)
}

// close waits for all channels returned to close the connection or close it now
// depending on strategy. And prevents all requests for the channel.
func (c *Connection) close(s CloseStrategy) error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return ErrConnectionClosed
	}

	if s == Abort {
		return c.finalClose()
	}

	go func() {
		for {
			ch, ok := <-c.idleChannels
			if ok {
				c.m.Lock()
				c.deleteChannel(ch)
				if c.numOpen == 0 {
					c.m.Unlock()
					break
				}
				c.m.Unlock()
			}

			break
		}

		_ = c.finalClose()
	}()

	return nil
}

func (c *Connection) finalClose() error {
	c.m.Lock()
	defer c.m.Unlock()

	c.finalClosed = 1

	err := c.amqConnection.Close()

	c.pool = nil
	c.amqConnection = nil
	c.nextChannel = 0
	c.channels = nil
	c.numOpen = 0
	c.idleChannels = nil

	return err
}

func (c *Connection) SetReturnedAt(t time.Time) {
	c.returnedAt = t
}

func (c *Connection) SetPool(p *ConnectionPool) {
	c.pool = p
}

func (c *Connection) isClosed() bool {
	return int(atomic.LoadInt32(&c.closed)) == 1
}
