package huma

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultPublisherPoolSize = 5
	defaultPublisherPoolWait = 20 * time.Millisecond
	defaultReconnectDelay    = 5 * time.Second
	defaultDialTimeout       = 30 * time.Second
	retryCountHeader         = "x-huma-redelivery-count"
)

// NewSDK is the huma factory method.
func NewSDK(ctx context.Context, cfg SDKConfig) (RabbitMQKit, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if cfg.Addr == "" {
		return nil, errors.New("RabbitMQ address is required")
	}
	if cfg.ReconnectDelay < 0 {
		return nil, errors.New("reconnect delay cannot be negative")
	}
	if cfg.DialTimeout < 0 {
		return nil, errors.New("dial timeout cannot be negative")
	}
	if cfg.PublisherPoolSize < 0 || cfg.PublisherMaxPoolSize < 0 {
		return nil, errors.New("publisher pool sizes cannot be negative")
	}
	if cfg.PublisherPoolWait < 0 {
		return nil, errors.New("publisher pool wait cannot be negative")
	}

	poolSize := cfg.PublisherPoolSize
	if poolSize == 0 {
		poolSize = defaultPublisherPoolSize
	}
	poolMaxSize := cfg.PublisherMaxPoolSize
	if poolMaxSize == 0 {
		poolMaxSize = poolSize * 2
	}
	if poolMaxSize < poolSize {
		return nil, errors.New("publisher maximum pool size cannot be smaller than initial pool size")
	}
	poolWait := cfg.PublisherPoolWait
	if poolWait == 0 {
		poolWait = defaultPublisherPoolWait
	}
	reconnectDelay := cfg.ReconnectDelay
	if reconnectDelay == 0 {
		reconnectDelay = defaultReconnectDelay
	}

	sdkCtx, cancel := context.WithCancel(ctx)
	scheme := "amqp"
	if cfg.TLSConfig != nil {
		scheme = "amqps"
	}

	connectionURL := (&url.URL{
		Scheme: scheme,
		Host:   cfg.Addr,
		User:   url.UserPassword(cfg.Username, cfg.Password),
	}).String()

	connectionConfig := amqp091.Config{
		Heartbeat: cfg.Heartbeat,
		Vhost:     cfg.VHost,
		Properties: amqp091.Table{
			"connection_name": cfg.ConnectionName,
		},
	}
	if cfg.TLSConfig != nil {
		connectionConfig.TLSClientConfig = cfg.TLSConfig.Clone()
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = defaultDialTimeout
	}
	dialer := &net.Dialer{Timeout: dialTimeout}
	connectionState := &connectionState{}
	connectionConfig.Dial = func(network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(sdkCtx, network, address)
		if err != nil {
			return nil, err
		}
		deadline := time.Now().Add(dialTimeout)
		if contextDeadline, ok := sdkCtx.Deadline(); ok && contextDeadline.Before(deadline) {
			deadline = contextDeadline
		}
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, errors.Join(err, conn.Close())
		}
		connectionState.set(sdkCtx, conn)
		return conn, nil
	}

	conn, err := amqp091.DialConfig(connectionURL, connectionConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		cancel()
		return nil, errors.Join(err, conn.Close())
	}
	channelClose := ch.NotifyClose(make(chan *amqp091.Error, 1))
	connClose := conn.NotifyClose(make(chan *amqp091.Error, 1))

	pubPool, err := newPublisherChannelPool(ctx, conn, connectionState.current(ctx), poolSize, poolMaxSize)
	if err != nil {
		cancel()
		return nil, errors.Join(
			fmt.Errorf("initialize publisher pool: %w", err),
			ch.Close(),
			conn.Close(),
		)
	}

	var m Metrics
	if cfg.EnableMetrics {
		ns := cfg.MetricsNamespace
		if ns == "" {
			ns = "huma"
		}
		m, err = newPrometheusMetrics(ctx, cfg.MetricsRegisterer, ns, cfg.MetricLabelName, cfg.MetricLabelValue)
		if err != nil {
			cancel()
			return nil, errors.Join(err, pubPool.close(ctx), ch.Close(), conn.Close())
		}
	} else {
		m = &noOpMetrics{}
	}

	sdk := &SDK{
		conn:                 conn,
		channel:              ch,
		ctx:                  sdkCtx,
		cancel:               cancel,
		logger:               &NoOpLogger{},
		reconnectInterval:    reconnectDelay,
		metrics:              m,
		connectionConfig:     connectionConfig,
		connectionURL:        connectionURL,
		connectionState:      connectionState,
		publisherPoolSize:    poolSize,
		publisherPoolWait:    poolWait,
		publisherMaxPoolSize: poolMaxSize,
		publisherPool:        pubPool,
		enableTracing:        cfg.EnableTracing,
		extractContext:       cfg.ExtractContext,
		injectHeaders:        cfg.InjectHeaders,
	}

	sdk.wg.Go(func() {
		sdk.monitorConnection(sdkCtx, channelClose, connClose)
	})

	return sdk, nil
}

// SetLogger sets the logger implementation.
func (s *SDK) SetLogger(logger Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// SetQos sets the Quality of Service on the consume channel.
func (s *SDK) SetQos(ctx context.Context, prefetchCount, prefetchSize int, global bool) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.channel.Qos(prefetchCount, prefetchSize, global)
	if err != nil {
		s.logger.Errorw("@huma.SetQos", "message", "Failed to set qos", "error", err)
		return err
	}

	s.qosConfigured = true
	s.qosPrefetchCount = prefetchCount
	s.qosPrefetchSize = prefetchSize
	s.qosGlobal = global

	return nil
}

// DeclareQueues declares queues on RabbitMQ and stores them for reconnect replay.
func (s *SDK) DeclareQueues(ctx context.Context, queues ...QueueConfig) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.declareQueuesLocked(ctx, queues, true)
}

func (s *SDK) declareQueuesLocked(ctx context.Context, queues []QueueConfig, store bool) error {
	for _, q := range queues {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.logger.Infow("@huma.DeclareQueues", "message", "Declaring queue", "queue_name", q.Name)

		if err := q.validate(); err != nil {
			return err
		}

		var args amqp091.Table

		if q.QueueType == Quorum {
			if args == nil {
				args = amqp091.Table{}
			}
			args[amqp091.QueueTypeArg] = amqp091.QueueTypeQuorum
		}
		if q.RoutingKey == "" {
			q.RoutingKey = RoutingKeyName(q.Name)
		}

		if q.EnableDLQ {
			dlxName := string(q.Name) + ".DLX"
			dlqName := string(q.Name) + ".DLQ"

			err := s.channel.ExchangeDeclare(dlxName, "direct", true, false, false, false, nil)
			if err != nil {
				return fmt.Errorf("failed to declare DLX %s: %w", dlxName, err)
			}

			_, err = s.channel.QueueDeclare(dlqName, true, false, false, false, nil)
			if err != nil {
				return fmt.Errorf("failed to declare DLQ %s: %w", dlqName, err)
			}

			err = s.channel.QueueBind(dlqName, dlqName, dlxName, false, nil)
			if err != nil {
				return fmt.Errorf("failed to bind DLQ %s to DLX %s: %w", dlqName, dlxName, err)
			}

			if args == nil {
				args = amqp091.Table{}
			}
			args["x-dead-letter-exchange"] = dlxName
			args["x-dead-letter-routing-key"] = dlqName
		}

		if q.IsDelayQueue {
			switch q.DelayStrategy {
			case DelayDLXTTL:
				delayQueueName := getDelayQueueName(ctx, string(q.Name))
				err := s.channel.ExchangeDeclare(
					string(q.Exchange),
					amqp091.ExchangeDirect,
					true,
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					return fmt.Errorf("failed to declare DLX+TTL exchange: %s: %w", q.Exchange, err)
				}

				delayArgs := amqp091.Table{
					"x-message-ttl":             int32(q.DLXTTL.Milliseconds()),
					"x-dead-letter-exchange":    string(q.Exchange),
					"x-dead-letter-routing-key": string(q.RoutingKey),
				}
				if q.QueueType == Quorum {
					delayArgs[amqp091.QueueTypeArg] = amqp091.QueueTypeQuorum
				}
				_, err = s.channel.QueueDeclare(string(delayQueueName), true, false, false, false, delayArgs)
				if err != nil {
					return fmt.Errorf("failed to declare DLX+TTL delay queue %s: %w", delayQueueName, err)
				}
				s.logger.Infow("@huma.DeclareQueues",
					"message", "Declared DLX+TTL delay queue",
					"delay_queue_name", delayQueueName,
					"ttl_ms", q.DLXTTL.Milliseconds(),
				)
			}
		}

		if q.ConsumerTimeout > 0 {
			if args == nil {
				args = amqp091.Table{}
			}
			args["x-consumer-timeout"] = q.ConsumerTimeout.Milliseconds()
		}

		_, err := s.channel.QueueDeclare(string(q.Name), q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, args)
		if err != nil {
			s.logger.Errorw("@huma.DeclareQueues",
				"message", "Failed to declare queue",
				"queue_name", q.Name,
				"error", err)
			return fmt.Errorf("failed to declare a queue %s: %w", q.Name, err)
		}

		if q.IsDelayQueue {
			err := s.channel.QueueBind(string(q.Name), string(q.RoutingKey), string(q.Exchange), false, nil)
			if err != nil {
				s.logger.Errorw("@huma.DeclareQueues",
					"message", "Failed to bind queue to exchange",
					"queue_name", q.Name,
					"queue_exchange", q.Exchange,
					"error", err)
				return fmt.Errorf("failed to bind a delayed queue to the delayed exchange %s: %w", q.Name, err)
			}
		}
	}

	if store {
		s.declaredQueues = append(s.declaredQueues, cloneQueueConfigs(ctx, queues)...)
	}

	return nil
}

// Start starts the consumers and workers for each queue.
func (s *SDK) Start(ctx context.Context, queues []QueueConfig) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	return s.startWithStoreQueuesFlag(ctx, queues, true, false)
}

func (s *SDK) startWithStoreQueuesFlag(ctx context.Context, queues []QueueConfig, store, channelLocked bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, q := range queues {
		if err := q.validate(); err != nil {
			return err
		}
		if q.Handler == nil {
			return fmt.Errorf("queue %s: handler is required", q.Name)
		}
		if q.NumWorkers <= 0 {
			return fmt.Errorf("queue %s: number of workers must be positive", q.Name)
		}
	}

	if store {
		s.mu.Lock()
		if len(s.queues) > 0 {
			s.queues = append(s.queues, cloneQueueConfigs(ctx, queues)...)
		} else {
			s.queues = cloneQueueConfigs(ctx, queues)
		}
		s.mu.Unlock()
	}

	for _, q := range queues {
		if q.DummyMessageFrequency == 0 {
			q.DummyMessageFrequency = time.Minute
		}

		if !channelLocked {
			s.mu.Lock()
		}
		msgs, err := s.channel.Consume(
			string(q.Name),
			q.ConsumerTag,
			false,
			q.Exclusive,
			false,
			q.NoWait,
			nil,
		)
		if !channelLocked {
			s.mu.Unlock()
		}
		if err != nil {
			s.logger.Errorw("@huma.Start",
				"message", "Consume queue encountered an error",
				"queue", q,
				"error", err)
			return fmt.Errorf("consume on rabbitmq channel encounter error: %w", err)
		}

		for range q.NumWorkers {
			s.wg.Go(func() {
				s.consume(s.ctx, msgs, q)
			})
		}
	}

	return nil
}

func (s *SDK) consume(ctx context.Context, msgs <-chan amqp091.Delivery, q QueueConfig) {
	var heartbeatTicker *time.Ticker
	var heartbeatC <-chan time.Time
	if q.DummyMessageEnabled {
		heartbeatTicker = time.NewTicker(q.DummyMessageFrequency)
		heartbeatC = heartbeatTicker.C
		defer heartbeatTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Infow("@huma.consume",
				"message", "Stopping consumer",
				"queue_name", q.Name)
			return
		case d, ok := <-msgs:
			if !ok {
				return
			}

			if q.DummyMessageEnabled && isHeartbeatDummyMessage(ctx, d) {
				if err := d.Ack(false); err != nil {
					s.logger.Errorw("@huma.consume", "message", "Failed to acknowledge heartbeat", "error", err)
				}
				heartbeatTicker.Reset(q.DummyMessageFrequency)
				continue
			}

			messageCtx := ctx
			if s.extractContext != nil {
				messageCtx = s.extractContext(messageCtx, d)
			}

			var span trace.Span
			if s.enableTracing {
				extractedCtx := otel.GetTextMapPropagator().Extract(messageCtx, AMQPHeaderCarrier(d.Headers))
				messageCtx, span = otel.Tracer("huma").Start(
					extractedCtx,
					"huma.consume",
					trace.WithSpanKind(trace.SpanKindConsumer),
					trace.WithAttributes(
						attribute.String("queue", string(q.Name)),
					),
				)
			}

			s.metrics.IncMessagesReceived(messageCtx, string(q.Name))
			s.processDelivery(messageCtx, d, q, span)
			if span != nil {
				span.End()
			}
		case <-heartbeatC:
			if err := s.publishAMQP(ctx, "", string(q.Name), heartBeatMSG); err != nil {
				s.logger.Errorw("@huma.consume", "message", "Failed to publish heartbeat", "error", err)
			}
			heartbeatTicker.Reset(q.DummyMessageFrequency)
		}
	}
}

func (s *SDK) processDelivery(ctx context.Context, d amqp091.Delivery, q QueueConfig, span trace.Span) {
	start := time.Now()
	defer func() {
		s.metrics.ObserveProcessingDuration(ctx, string(q.Name), time.Since(start).Seconds())
		if recovered := recover(); recovered != nil {
			err := fmt.Errorf("handler panic: %v", recovered)
			if span != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			s.logger.Errorw("@huma.consume", "message", "Recovered from handler panic", "queue_name", q.Name, "error", err)
			s.handleDeliveryFailure(ctx, d, q)
		}
	}()

	handlerCtx := ctx
	cancel := func() {}
	if q.ProcessTimeout > 0 {
		handlerCtx, cancel = context.WithTimeout(ctx, q.ProcessTimeout)
	}
	defer cancel()

	if err := q.Handler(handlerCtx, q.Name, RabbitMQMsg{Delivery: d}); err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		s.logger.Errorw("@huma.consume", "message", "Handler failed", "queue_name", q.Name, "error", err)
		s.handleDeliveryFailure(ctx, d, q)
		return
	}

	if err := d.Ack(false); err != nil {
		s.logger.Errorw("@huma.consume", "message", "Failed to acknowledge message", "queue_name", q.Name, "error", err)
		return
	}
	s.metrics.IncMessagesAcked(ctx, string(q.Name))
	s.logger.Infow("@huma.consume", "message", "Message processed", "queue_name", q.Name)
}

func (s *SDK) handleDeliveryFailure(ctx context.Context, d amqp091.Delivery, q QueueConfig) {
	requeue := q.MaxRedelivery == 0
	if q.MaxRedelivery > 0 {
		retryCount := deliveryRetryCount(ctx, d.Headers)
		if retryCount < q.MaxRedelivery {
			headers := make(amqp091.Table, len(d.Headers)+1)
			for key, value := range d.Headers {
				headers[key] = value
			}
			headers[retryCountHeader] = int64(retryCount + 1)
			publishing := amqp091.Publishing{
				Headers: headers, ContentType: d.ContentType, ContentEncoding: d.ContentEncoding,
				DeliveryMode: d.DeliveryMode, Priority: d.Priority, CorrelationId: d.CorrelationId,
				ReplyTo: d.ReplyTo, Expiration: d.Expiration, MessageId: d.MessageId,
				Timestamp: d.Timestamp, Type: d.Type, UserId: d.UserId, AppId: d.AppId, Body: d.Body,
			}
			if err := s.publishAMQP(ctx, d.Exchange, d.RoutingKey, publishing); err == nil {
				if err := d.Ack(false); err != nil {
					s.logger.Errorw("@huma.consume", "message", "Failed to acknowledge retried message", "queue_name", q.Name, "error", err)
					return
				}
				s.metrics.IncMessagesNacked(ctx, string(q.Name))
				return
			} else {
				s.logger.Errorw("@huma.consume", "message", "Failed to republish message for retry", "queue_name", q.Name, "error", err)
				requeue = true
			}
		}
	}

	if err := d.Nack(false, requeue); err != nil {
		s.logger.Errorw("@huma.consume", "message", "Failed to negatively acknowledge message", "queue_name", q.Name, "error", err)
		return
	}
	s.metrics.IncMessagesNacked(ctx, string(q.Name))
}

func deliveryRetryCount(ctx context.Context, headers amqp091.Table) int {
	_ = ctx
	switch count := headers[retryCountHeader].(type) {
	case int:
		return count
	case int32:
		return int(count)
	case int64:
		return int(count)
	default:
		return 0
	}
}

// Publish publishes a non-delay message to the given exchange or queue.
//
// For delay queues, publish to the exchange. For regular queues, publish using the routing key (queue name).
func (s *SDK) Publish(ctx context.Context, exchange, routingKey string, msg *Message) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := msg.validate(); err != nil {
		return err
	}

	if s.enableTracing && trace.SpanContextFromContext(ctx).IsValid() {
		otel.GetTextMapPropagator().Inject(ctx, AMQPHeaderCarrier(msg.Headers))
	}

	headers := msg.Headers
	if s.injectHeaders != nil {
		headers = s.injectHeaders(ctx, msg.Headers)
	}
	if headers == nil {
		headers = amqp091.Table{}
	}

	err := s.publishAMQP(ctx, exchange, routingKey, amqp091.Publishing{
		Headers:     headers,
		ContentType: msg.ContentType,
		Body:        msg.Body,
		Expiration:  formatDurationToExpiration(msg.Expiration),
		Priority:    msg.Priority,
		MessageId:   msg.MessageID,
		Timestamp:   msg.Timestamp,
	})
	if err != nil {
		s.metrics.IncPublishFailed(ctx, chooseQueueNameLabel(ctx, exchange, routingKey))
		return fmt.Errorf("failed to publish msg on rabbitmq: %w", err)
	}

	s.metrics.IncPublishSuccess(ctx, chooseQueueNameLabel(ctx, exchange, routingKey))
	return nil
}

func (s *SDK) publishAMQP(ctx context.Context, exchange, routingKey string, publishing amqp091.Publishing) (err error) {
	if err := validateContext(ctx); err != nil {
		return err
	}
	poolCtx, cancel := context.WithTimeout(ctx, s.publisherPoolWait)
	defer cancel()

	s.publisherMu.RLock()
	pool := s.publisherPool
	s.publisherMu.RUnlock()
	if pool == nil {
		return errors.New("publisher pool is unavailable")
	}

	ch, err := pool.get(poolCtx)
	if err != nil {
		return fmt.Errorf("get publisher channel: %w", err)
	}
	defer func() {
		if putErr := pool.put(ctx, ch); putErr != nil {
			s.logger.Errorw("@huma.Publish", "message", "Failed to return publisher channel", "error", putErr)
		}
	}()
	if err := validateContext(ctx); err != nil {
		return err
	}

	type publishResult struct {
		confirmation *amqp091.DeferredConfirmation
		err          error
	}
	published := make(chan publishResult, 1)
	go func() {
		confirmation, publishErr := ch.PublishWithDeferredConfirmWithContext(
			ctx, exchange, routingKey, false, false, publishing,
		)
		published <- publishResult{confirmation: confirmation, err: publishErr}
	}()

	var result publishResult
	select {
	case result = <-published:
	case <-ctx.Done():
		interruptErr := pool.interrupt(ctx)
		result = <-published
		if interruptErr != nil && !errors.Is(interruptErr, net.ErrClosed) {
			s.logger.Errorw("@huma.Publish", "message", "Failed to interrupt canceled publish", "error", interruptErr)
		}
		return fmt.Errorf("publish message: %w", ctx.Err())
	}
	confirmation, err := result.confirmation, result.err
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for publisher confirmation: %w", err)
	}
	if !acked {
		return errors.New("RabbitMQ negatively acknowledged the published message")
	}
	return nil
}

// BatchPublish sends multiple messages to the same exchange/routing key.
//
// For delay queues, publish to the exchange. For regular queues, use the routing key (queue name).
func (s *SDK) BatchPublish(ctx context.Context, exchange, routingKey string, messages []*Message) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	for _, msg := range messages {
		if err := s.Publish(ctx, exchange, routingKey, msg); err != nil {
			return err
		}
	}
	return nil
}

// PublishWithDelayDLXTTL publishes a delayed message to the associated delay queue.
//
// Only use this when DelayStrategy is DelayDLXTTL.
func (s *SDK) PublishWithDelayDLXTTL(ctx context.Context, queueName string, msg *Message) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := msg.validate(); err != nil {
		return err
	}

	return s.Publish(ctx, "", getDelayQueueName(ctx, queueName), msg)
}

// Shutdown stops all workers and closes RabbitMQ channels and connection.
func (s *SDK) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	s.logger.Infof("Initiating SDK shutdown...")
	s.shutdownOnce.Do(func() {
		s.cancel()
		if err := s.connectionState.interrupt(ctx); err != nil {
			s.logger.Errorw("@huma.Shutdown", "message", "Failed to interrupt RabbitMQ connection", "error", err)
		}
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for SDK shutdown: %w", ctx.Err())
	case <-done:
	}

	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		var closeErrors []error
		s.publisherMu.Lock()
		if s.publisherPool != nil {
			closeErrors = append(closeErrors, s.publisherPool.close(ctx))
			s.publisherPool = nil
		}
		s.publisherMu.Unlock()
		if s.channel != nil {
			closeErrors = append(closeErrors, s.channel.Close())
		}
		if s.conn != nil {
			closeErrors = append(closeErrors, s.conn.Close())
		}
		for _, closeErr := range closeErrors {
			if closeErr != nil && !errors.Is(closeErr, amqp091.ErrClosed) {
				s.closeErr = errors.Join(s.closeErr, closeErr)
			}
		}
	})
	s.logger.Infof("SDK shutdown complete.")
	return s.closeErr
}

func chooseQueueNameLabel(ctx context.Context, exchange, routingKey string) string {
	_ = ctx
	if len(routingKey) > 0 {
		return routingKey
	}
	return exchange
}

func isHeartbeatDummyMessage(ctx context.Context, d amqp091.Delivery) bool {
	_ = ctx
	if typeHeader, ok := d.Headers["type"]; ok {
		if typeHeader == heartBeatMSG.Headers["type"] {
			return true
		}
	}
	return false
}

func (q QueueConfig) validate() error {
	if q.Name == "" {
		return errors.New("queue name is required")
	}
	if q.QueueType != "" && q.QueueType != Classic && q.QueueType != Quorum {
		return fmt.Errorf("queue %s: unsupported queue type %q", q.Name, q.QueueType)
	}
	if q.MaxRedelivery < -1 {
		return fmt.Errorf("queue %s: maximum redelivery cannot be less than -1", q.Name)
	}
	if q.DummyMessageFrequency < 0 {
		return fmt.Errorf("queue %s: dummy message frequency cannot be negative", q.Name)
	}
	if q.QueueType == Quorum {
		if !q.Durable {
			return fmt.Errorf("quorum queue %s must be durable", q.Name)
		}
		if q.AutoDelete {
			return fmt.Errorf("quorum queue %s cannot be auto-delete", q.Name)
		}
		if q.Exclusive {
			return fmt.Errorf("quorum queue %s cannot be exclusive", q.Name)
		}
	}
	if q.IsDelayQueue {
		if q.Exchange == "" {
			return fmt.Errorf("queue %s: delay queue exchange is required", q.Name)
		}
		if q.DelayStrategy != DelayDLXTTL {
			return fmt.Errorf("queue %s: unsupported delay strategy %d", q.Name, q.DelayStrategy)
		}
		if q.DelayStrategy == DelayDLXTTL && (q.DLXTTL <= 0 || q.DLXTTL.Milliseconds() > math.MaxInt32) {
			return fmt.Errorf("queue %s: DLX+TTL must be between 1 millisecond and 2147483647 milliseconds", q.Name)
		}
	}
	return nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

func (s *connectionState) set(ctx context.Context, conn net.Conn) {
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	if ctx.Err() != nil {
		if err := s.interrupt(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
			_ = conn.Close()
		}
	}
}

func (s *connectionState) interrupt(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	deadline := time.Now()
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	return s.conn.SetDeadline(deadline)
}

func (s *connectionState) current(ctx context.Context) net.Conn {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

func getDelayQueueName(ctx context.Context, queueName string) string {
	_ = ctx
	return fmt.Sprintf("%s.delay", queueName)
}

func cloneQueueConfigs(ctx context.Context, queues []QueueConfig) []QueueConfig {
	_ = ctx
	cloned := make([]QueueConfig, len(queues))
	copy(cloned, queues)
	return cloned
}
