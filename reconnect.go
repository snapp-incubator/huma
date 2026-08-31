package huma

import (
	"context"
	"errors"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func (s *SDK) monitorConnection(
	ctx context.Context,
	channelClose <-chan *amqp091.Error,
	connClose <-chan *amqp091.Error,
) {
	for channelClose != nil || connClose != nil {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-channelClose:
			if !ok {
				channelClose = nil
				continue
			}
			s.handleCloseError(ctx, err)
			return
		case err, ok := <-connClose:
			if !ok {
				connClose = nil
				continue
			}
			s.handleCloseError(ctx, err)
			return
		}
	}
}

func (s *SDK) handleCloseError(ctx context.Context, err *amqp091.Error) {
	if err == nil {
		s.logger.Infow("@huma.reconnect",
			"message", "Connection/Channel closed gracefully",
			"error", err)
		return
	}

	s.logger.Errorw("@huma.reconnect",
		"message", "Connection/Channel closed, starting reconnection",
		"error", err)

	if !s.reconnecting.CompareAndSwap(false, true) {
		return
	}

	s.wg.Go(func() {
		s.reconnect(ctx)
	})
}

func (s *SDK) reconnect(ctx context.Context) {
	defer s.reconnecting.Store(false)

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			s.logger.Infof("Attempting to reconnect...")

			s.publisherMu.Lock()
			oldPublisherPool := s.publisherPool
			s.publisherPool = nil
			s.publisherMu.Unlock()
			if oldPublisherPool != nil {
				if err := oldPublisherPool.close(ctx); err != nil {
					s.logger.Errorw("@huma.reconnect", "message", "Failed to close publisher pool", "error", err)
				}
			}
			if s.channel != nil {
				if err := s.channel.Close(); err != nil && !errors.Is(err, amqp091.ErrClosed) {
					s.logger.Errorw("@huma.reconnect", "message", "Failed to close channel", "error", err)
				}
			}
			if s.conn != nil {
				if err := s.conn.Close(); err != nil && !errors.Is(err, amqp091.ErrClosed) {
					s.logger.Errorw("@huma.reconnect", "message", "Failed to close connection", "error", err)
				}
			}

			conn, err := amqp091.DialConfig(s.connectionURL, s.connectionConfig)
			if err != nil {
				s.logger.Errorw("@huma.reconnect",
					"message", "Reconnect failed",
					"reconnect_interval", s.reconnectInterval,
					"error", err)
				if !s.waitReconnect(ctx) {
					return
				}
				continue
			}
			if err := ctx.Err(); err != nil {
				if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, amqp091.ErrClosed) {
					s.logger.Errorw("@huma.reconnect", "message", "Failed to close canceled connection", "error", closeErr)
				}
				return
			}

			ch, err := conn.Channel()
			if err != nil {
				if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, amqp091.ErrClosed) {
					s.logger.Errorw("@huma.reconnect", "message", "Failed to close connection", "error", closeErr)
				}
				s.logger.Errorw("@huma.reconnect",
					"message", "Reconnect channel failed",
					"reconnect_interval", s.reconnectInterval,
					"error", err)
				if !s.waitReconnect(ctx) {
					return
				}
				continue
			}
			channelClose := ch.NotifyClose(make(chan *amqp091.Error, 1))
			connClose := conn.NotifyClose(make(chan *amqp091.Error, 1))

			s.conn = conn
			s.channel = ch

			if s.qosConfigured {
				err = ch.Qos(s.qosPrefetchCount, s.qosPrefetchSize, s.qosGlobal)
				if err != nil {
					s.closeReconnectResources(ctx, ch, conn)
					s.logger.Errorw("@huma.reconnect",
						"message", "Reconnect qos setup failed",
						"reconnect_interval", s.reconnectInterval,
						"error", err)
					if !s.waitReconnect(ctx) {
						return
					}
					continue
				}
			}

			if len(s.declaredQueues) > 0 {
				err = s.declareQueuesLocked(ctx, s.declaredQueues, false)
				if err != nil {
					s.closeReconnectResources(ctx, ch, conn)
					s.logger.Errorw("@huma.reconnect",
						"message", "Reconnect topology setup failed",
						"reconnect_interval", s.reconnectInterval,
						"error", err)
					if !s.waitReconnect(ctx) {
						return
					}
					continue
				}
			}

			s.logger.Infof("Attempting to create new publisher pool...")

			pubPool, err := newPublisherChannelPool(
				ctx,
				conn,
				s.connectionState.current(ctx),
				s.publisherPoolSize,
				s.publisherMaxPoolSize,
			)
			if err != nil {
				s.closeReconnectResources(ctx, ch, conn)
				s.logger.Errorw("@huma.reconnect",
					"message", "Reconnect channel failed. Failed to create publisher channel pool",
					"reconnect_interval", s.reconnectInterval,
					"error", err)
				if !s.waitReconnect(ctx) {
					return
				}
				continue
			}

			s.publisherMu.Lock()
			s.publisherPool = pubPool
			s.publisherMu.Unlock()

			s.logger.Infof("Reconnected successfully.")

			s.metrics.IncReconnects(ctx)

			if len(s.queues) > 0 {
				err = s.startWithStoreQueuesFlag(ctx, s.queues, false, true)
				if err != nil {
					s.logger.Errorw("@huma.reconnect",
						"message", "Reconnect channel failed. Failed to consume the queues",
						"reconnect_interval", s.reconnectInterval,
						"queues", s.queues,
						"error", err)
					if !s.waitReconnect(ctx) {
						return
					}
					continue
				}
			}

			s.wg.Go(func() {
				s.monitorConnection(ctx, channelClose, connClose)
			})

			return
		}
	}
}

func (s *SDK) waitReconnect(ctx context.Context) bool {
	timer := time.NewTimer(s.reconnectInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *SDK) closeReconnectResources(ctx context.Context, ch *amqp091.Channel, conn *amqp091.Connection) {
	if err := ch.Close(); err != nil && !errors.Is(err, amqp091.ErrClosed) {
		s.logger.Errorw("@huma.reconnect", "message", "Failed to close channel", "error", err)
	}
	if err := conn.Close(); err != nil && !errors.Is(err, amqp091.ErrClosed) {
		s.logger.Errorw("@huma.reconnect", "message", "Failed to close connection", "error", err)
	}
	_ = ctx
}
