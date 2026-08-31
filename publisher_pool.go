package huma

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// publisherChannelPool manages a pool of AMQP channels for publishing.
type publisherChannelPool struct {
	conn        *amqp091.Connection
	rawConn     net.Conn
	mu          sync.Mutex
	channels    chan *amqp091.Channel
	size        int
	maxPoolSize int32
	closed      bool

	currentPoolSize atomic.Int32
}

func newPublisherChannelPool(
	ctx context.Context,
	conn *amqp091.Connection,
	rawConn net.Conn,
	size, maxPoolSize int,
) (*publisherChannelPool, error) {
	_ = ctx
	pool := &publisherChannelPool{
		conn:            conn,
		rawConn:         rawConn,
		channels:        make(chan *amqp091.Channel, size),
		size:            size,
		maxPoolSize:     int32(maxPoolSize),
		currentPoolSize: atomic.Int32{},
	}

	for range size {
		ch, err := conn.Channel()
		if err != nil {
			return nil, errors.Join(err, pool.close(ctx))
		}
		if err := ch.Confirm(false); err != nil {
			return nil, errors.Join(err, ch.Close(), pool.close(ctx))
		}
		pool.channels <- ch
		pool.currentPoolSize.Add(1)
	}

	return pool, nil
}

func (p *publisherChannelPool) interrupt(ctx context.Context) error {
	_ = ctx
	if p.rawConn == nil {
		return nil
	}
	return errors.Join(p.rawConn.SetDeadline(time.Now()), p.rawConn.Close())
}

// get pops a channel from the pool. The caller must return it with put after use.
// If no channel is available, a new one is created up to maxPoolSize, then the call
// blocks until one is returned or ctx is canceled.
func (p *publisherChannelPool) get(ctx context.Context) (*amqp091.Channel, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("get publisher channel: %w", err)
		}
		if ch, ok, err := p.tryTakeChannel(); err != nil {
			return nil, err
		} else if ok && !ch.IsClosed() {
			return ch, nil
		} else if ok {
			p.currentPoolSize.Add(-1)
		}

		if p.currentPoolSize.Load() < p.maxPoolSize {
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return nil, errors.New("publisher pool is closed")
			}
			if p.currentPoolSize.Load() < p.maxPoolSize {
				ch, err := p.conn.Channel()
				if err != nil {
					p.mu.Unlock()
					return nil, err
				}
				if err := ch.Confirm(false); err != nil {
					p.mu.Unlock()
					return nil, errors.Join(err, ch.Close())
				}
				p.currentPoolSize.Add(1)
				p.mu.Unlock()
				return ch, nil
			}
			p.mu.Unlock()
		}

		select {
		case ch, ok := <-p.channels:
			if !ok {
				return nil, errors.New("publisher pool is closed")
			}
			if ch.IsClosed() {
				p.currentPoolSize.Add(-1)
				continue
			}
			return ch, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for available publisher channel: %w", ctx.Err())
		}
	}
}

func (p *publisherChannelPool) tryTakeChannel() (*amqp091.Channel, bool, error) {
	select {
	case ch, ok := <-p.channels:
		if !ok {
			return nil, false, errors.New("publisher pool is closed")
		}
		return ch, true, nil
	default:
		return nil, false, nil
	}
}

// put returns a channel to the pool. If the pool is full or closed, the channel is closed.
func (p *publisherChannelPool) put(ctx context.Context, ch *amqp091.Channel) error {
	_ = ctx
	if ch == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || ch.IsClosed() {
		p.currentPoolSize.Add(-1)
		if ch.IsClosed() {
			return nil
		}
		return ch.Close()
	}

	select {
	case p.channels <- ch:
	default:
		p.currentPoolSize.Add(-1)
		return ch.Close()
	}
	return nil
}

// close drains and closes all channels in the pool.
func (p *publisherChannelPool) close(ctx context.Context) error {
	_ = ctx
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.channels)
	p.mu.Unlock()

	var errs []error
	for ch := range p.channels {
		p.currentPoolSize.Add(-1)
		if err := ch.Close(); err != nil && !errors.Is(err, amqp091.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
