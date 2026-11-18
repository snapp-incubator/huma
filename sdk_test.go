package huma_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rabbitmq/amqp091-go"
	"github.com/snapp-incubator/huma"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestNewSDKRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ctx     context.Context
		config  huma.SDKConfig
		wantErr string
	}{
		{name: "nil context", config: huma.SDKConfig{Addr: "localhost:5672"}, wantErr: "context is nil"},
		{name: "missing address", ctx: context.Background(), wantErr: "address is required"},
		{name: "negative reconnect delay", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", ReconnectDelay: -time.Second}, wantErr: "reconnect delay"},
		{name: "negative dial timeout", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", DialTimeout: -time.Second}, wantErr: "dial timeout"},
		{name: "negative pool wait", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolWait: -time.Second}, wantErr: "pool wait"},
		{name: "negative pool size", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolSize: -1}, wantErr: "pool sizes"},
		{name: "maximum below initial", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolSize: 2, PublisherMaxPoolSize: 1}, wantErr: "maximum pool size"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := huma.NewSDK(test.ctx, test.config)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewSDK() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestMessageWithHeaderInitializesHeaders(t *testing.T) {
	t.Parallel()

	var message huma.Message
	message.WithHeader("example", "value")
	if got := message.Headers["example"]; got != "value" {
		t.Fatalf("header = %v, want value", got)
	}
}

type testContextKey string

const (
	testMetricLabelName     = "app_id"
	testHeaderKey           = "x-app-id"
	testAppIDValue          = "orders-service"
	testQueue               = "orders.queue"
	testDownstreamQueue     = "downstream.queue"
	testShutdownDrainWait   = 30 * time.Millisecond
	testShutdownTimeout     = 50 * time.Millisecond
	testSDKOperationTimeout = 5 * time.Second
)

func TestPublishWithIgnoreContextCancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		useNilContext      bool
		useExpiredDeadline bool
		opts               []huma.PublishOption
		run                func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error
		wantErr            error
		wantErrSubstr      string
		wantRoutingKeys    []string
		wantBodies         []string
	}{
		{
			name: "default publish rejects canceled context",
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.Publish(ctx, "", testQueue, huma.NewMessage().WithBody([]byte("rejected")), opts...)
			},
			wantErr: context.Canceled,
		},
		{
			name: "publish ignores canceled context when option is set",
			opts: []huma.PublishOption{huma.WithIgnoreContextCancellation()},
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.Publish(ctx, "", testQueue, huma.NewMessage().WithBody([]byte("delivered")), opts...)
			},
			wantRoutingKeys: []string{testQueue},
			wantBodies:      []string{"delivered"},
		},
		{
			name:               "publish ignores expired deadline when option is set",
			useExpiredDeadline: true,
			opts:               []huma.PublishOption{huma.WithIgnoreContextCancellation()},
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.Publish(ctx, "", testQueue, huma.NewMessage().WithBody([]byte("after-deadline")), opts...)
			},
			wantRoutingKeys: []string{testQueue},
			wantBodies:      []string{"after-deadline"},
		},
		{
			name: "batch publish ignores canceled context when option is set",
			opts: []huma.PublishOption{huma.WithIgnoreContextCancellation()},
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.BatchPublish(
					ctx,
					"",
					testQueue,
					[]*huma.Message{
						huma.NewMessage().WithBody([]byte("first")),
						huma.NewMessage().WithBody([]byte("second")),
					},
					opts...,
				)
			},
			wantRoutingKeys: []string{testQueue, testQueue},
			wantBodies:      []string{"first", "second"},
		},
		{
			name: "delay publish ignores canceled context when option is set",
			opts: []huma.PublishOption{huma.WithIgnoreContextCancellation()},
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.PublishWithDelayDLXTTL(ctx, testQueue, huma.NewMessage().WithBody([]byte("delayed")), opts...)
			},
			wantRoutingKeys: []string{testQueue + ".delay"},
			wantBodies:      []string{"delayed"},
		},
		{
			name:          "nil context still returns error",
			useNilContext: true,
			opts:          []huma.PublishOption{huma.WithIgnoreContextCancellation()},
			run: func(ctx context.Context, sdk huma.RabbitMQKit, opts ...huma.PublishOption) error {
				return sdk.Publish(ctx, "", testQueue, huma.NewMessage().WithBody([]byte("nil")), opts...)
			},
			wantErrSubstr: "context is nil",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			setupCtx, setupCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
			defer setupCancel()

			srv := startFakeAMQPServer(setupCtx, t)
			registry := prometheus.NewRegistry()
			var capturedLabel atomic.Value

			sdk, err := huma.NewSDK(setupCtx, huma.SDKConfig{
				Addr:                 srv.addr(setupCtx),
				PublisherPoolSize:    1,
				PublisherMaxPoolSize: 2,
				PublisherPoolWait:    time.Second,
				EnableMetrics:        true,
				MetricsNamespace:     "huma_test",
				MetricsRegisterer:    registry,
				MetricLabelName:      testMetricLabelName,
				MetricLabelValue: func(ctx context.Context) string {
					val, _ := ctx.Value(testContextKey(testHeaderKey)).(string)
					if val != "" {
						capturedLabel.Store(val)
					}
					return val
				},
				InjectHeaders: func(ctx context.Context, headers amqp091.Table) amqp091.Table {
					out := amqp091.Table{}
					for k, v := range headers {
						out[k] = v
					}
					if val, _ := ctx.Value(testContextKey(testHeaderKey)).(string); val != "" {
						out[testHeaderKey] = val
					}
					return out
				},
			})
			if err != nil {
				t.Fatalf("NewSDK() error = %v", err)
			}
			t.Cleanup(func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
				defer shutdownCancel()
				if err := sdk.Shutdown(shutdownCtx); err != nil {
					t.Errorf("Shutdown() error = %v", err)
				}
			})

			var callCtx context.Context
			if !test.useNilContext {
				baseCtx := context.WithValue(context.Background(), testContextKey(testHeaderKey), testAppIDValue)
				if test.useExpiredDeadline {
					var cancel context.CancelFunc
					callCtx, cancel = context.WithDeadline(baseCtx, time.Now().Add(-time.Second))
					defer cancel()
				} else {
					var cancel context.CancelFunc
					callCtx, cancel = context.WithCancel(baseCtx)
					cancel()
				}
			}

			err = test.run(callCtx, sdk, test.opts...)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("publish error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if test.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrSubstr) {
					t.Fatalf("publish error = %v, want substring %q", err, test.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("publish error = %v, want nil", err)
			}

			for i, wantKey := range test.wantRoutingKeys {
				select {
				case msg := <-srv.publishedCh:
					if msg.RoutingKey != wantKey {
						t.Fatalf("published[%d].RoutingKey = %q, want %q", i, msg.RoutingKey, wantKey)
					}
					if msg.Body != test.wantBodies[i] {
						t.Fatalf("published[%d].Body = %q, want %q", i, msg.Body, test.wantBodies[i])
					}
					if gotHeader := msg.Headers[testHeaderKey]; gotHeader != testAppIDValue {
						t.Fatalf("published[%d].Headers[%q] = %v, want %q", i, testHeaderKey, gotHeader, testAppIDValue)
					}
				case <-time.After(testSDKOperationTimeout):
					t.Fatalf("timed out waiting for published message %d", i)
				}
			}
			if gotLabel, _ := capturedLabel.Load().(string); gotLabel != testAppIDValue {
				t.Fatalf("metric label value = %q, want %q", gotLabel, testAppIDValue)
			}
		})
	}
}

func TestPublishWithIgnoreContextCancellationPreservesTracing(t *testing.T) {
	setupTracer(t)
	setupCtx, setupCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
	defer setupCancel()

	srv := startFakeAMQPServer(setupCtx, t)
	sdk, err := huma.NewSDK(setupCtx, huma.SDKConfig{
		Addr:          srv.addr(setupCtx),
		EnableTracing: true,
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
		defer shutdownCancel()
		if err := sdk.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	parentCtx, span := otel.Tracer("huma-unit-test").Start(context.Background(), "publish-after-cancel")
	defer span.End()
	wantTraceID := span.SpanContext().TraceID()

	canceledCtx, cancel := context.WithCancel(parentCtx)
	cancel()

	err = sdk.Publish(
		canceledCtx,
		"",
		testQueue,
		huma.NewMessage().WithBody([]byte("traced")),
		huma.WithIgnoreContextCancellation(),
	)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case msg := <-srv.publishedCh:
		extracted := otel.GetTextMapPropagator().Extract(context.Background(), huma.AMQPHeaderCarrier(msg.Headers))
		gotSpanCtx := trace.SpanContextFromContext(extracted)
		if !gotSpanCtx.IsValid() {
			t.Fatal("extracted span context is invalid")
		}
		if gotSpanCtx.TraceID() != wantTraceID {
			t.Fatalf("extracted trace ID = %v, want %v", gotSpanCtx.TraceID(), wantTraceID)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("timed out waiting for traced message")
	}
}

func TestGracefulShutdownAllowsInFlightPublishAndAck(t *testing.T) {
	t.Parallel()

	srv := startFakeAMQPServer(context.Background(), t)
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	sdk, err := huma.NewSDK(appCtx, huma.SDKConfig{
		Addr:                 srv.addr(context.Background()),
		PublisherPoolSize:    1,
		PublisherMaxPoolSize: 2,
		PublisherPoolWait:    time.Second,
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}

	handlerStarted := make(chan struct{})
	allowFinish := make(chan struct{})
	var handlerCtxErr error
	var publishErr error

	queue := huma.QueueConfig{
		Name:       testQueue,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(ctx context.Context, _ huma.QueueName, _ huma.RabbitMQMsg) error {
			close(handlerStarted)
			<-allowFinish
			handlerCtxErr = ctx.Err()
			publishErr = sdk.Publish(ctx, "", testDownstreamQueue, huma.NewMessage().WithBody([]byte("downstream-event")))
			return publishErr
		},
	}

	if err := sdk.DeclareQueues(appCtx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(appCtx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := srv.deliver(context.Background(), testQueue, []byte("trigger")); err != nil {
		t.Fatalf("deliver() error = %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("handler did not start in time")
	}

	// Cancel the startup/signal context and begin Shutdown while the handler is still in flight.
	appCancel()
	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
		defer shutdownCancel()
		shutdownErrCh <- sdk.Shutdown(shutdownCtx)
	}()

	time.Sleep(testShutdownDrainWait)
	close(allowFinish)

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("Shutdown() timed out")
	}

	if handlerCtxErr != nil {
		t.Fatalf("handler ctx.Err() during graceful shutdown = %v, want nil", handlerCtxErr)
	}
	if publishErr != nil {
		t.Fatalf("Publish() inside handler during graceful shutdown error = %v", publishErr)
	}

	select {
	case msg := <-srv.publishedCh:
		if msg.RoutingKey != testDownstreamQueue || msg.Body != "downstream-event" {
			t.Fatalf("published message = %#v, want downstream-event on %q", msg, testDownstreamQueue)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("timed out waiting for downstream publish")
	}

	select {
	case ack := <-srv.ackEvents:
		if ack.Nacked {
			t.Fatalf("delivery was nacked (%#v), want ack", ack)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("timed out waiting for delivery ack")
	}
}

func TestGracefulShutdownRetryRepublishOnFailure(t *testing.T) {
	t.Parallel()

	srv := startFakeAMQPServer(context.Background(), t)
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	sdk, err := huma.NewSDK(appCtx, huma.SDKConfig{
		Addr:                 srv.addr(context.Background()),
		PublisherPoolSize:    1,
		PublisherMaxPoolSize: 2,
		PublisherPoolWait:    time.Second,
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}

	handlerStarted := make(chan struct{})
	allowFinish := make(chan struct{})
	var once sync.Once

	queue := huma.QueueConfig{
		Name:          testQueue,
		Durable:       true,
		MaxRedelivery: 1,
		NumWorkers:    1,
		Handler: func(context.Context, huma.QueueName, huma.RabbitMQMsg) error {
			once.Do(func() {
				close(handlerStarted)
			})
			<-allowFinish
			return errors.New("processing failed during shutdown")
		},
	}

	if err := sdk.DeclareQueues(appCtx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(appCtx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := srv.deliver(context.Background(), testQueue, []byte("retry-payload")); err != nil {
		t.Fatalf("deliver() error = %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("handler did not start in time")
	}

	appCancel()
	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testSDKOperationTimeout)
		defer shutdownCancel()
		shutdownErrCh <- sdk.Shutdown(shutdownCtx)
	}()

	time.Sleep(testShutdownDrainWait)
	close(allowFinish)

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("Shutdown() timed out")
	}

	select {
	case msg := <-srv.publishedCh:
		if msg.RoutingKey != testQueue || msg.Body != "retry-payload" {
			t.Fatalf("retry publish = %#v, want retry-payload on %q", msg, testQueue)
		}
		if gotCount := msg.Headers["x-huma-redelivery-count"]; gotCount != int64(1) {
			t.Fatalf("redelivery count header = %v, want 1", gotCount)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("timed out waiting for retry republish")
	}

	select {
	case ack := <-srv.ackEvents:
		if ack.Nacked {
			t.Fatalf("original delivery was nacked (%#v), want ack after retry republish", ack)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("timed out waiting for delivery ack")
	}
}

func TestShutdownTimeoutCancelsStuckHandler(t *testing.T) {
	t.Parallel()

	srv := startFakeAMQPServer(context.Background(), t)
	sdk, err := huma.NewSDK(context.Background(), huma.SDKConfig{
		Addr: srv.addr(context.Background()),
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}

	handlerStarted := make(chan struct{})
	handlerExited := make(chan error, 1)

	queue := huma.QueueConfig{
		Name:       testQueue,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(ctx context.Context, _ huma.QueueName, _ huma.RabbitMQMsg) error {
			close(handlerStarted)
			<-ctx.Done()
			handlerExited <- ctx.Err()
			return ctx.Err()
		},
	}

	if err := sdk.DeclareQueues(context.Background(), queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(context.Background(), []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := srv.deliver(context.Background(), testQueue, []byte("stuck")); err != nil {
		t.Fatalf("deliver() error = %v", err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("handler did not start in time")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), testShutdownTimeout)
	defer shutdownCancel()
	err = sdk.Shutdown(shutdownCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}

	select {
	case handlerErr := <-handlerExited:
		if !errors.Is(handlerErr, context.Canceled) {
			t.Fatalf("handler exited with %v, want %v", handlerErr, context.Canceled)
		}
	case <-time.After(testSDKOperationTimeout):
		t.Fatal("stuck handler was not canceled when shutdown timed out")
	}
}
