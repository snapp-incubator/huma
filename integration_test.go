package huma_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/snapp-incubator/huma"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	rabbitMQImage        = "rabbitmq:3.12.11-management-alpine"
	integrationEnv       = "HUMA_INTEGRATION"
	integrationEnabled   = "1"
	operationTimeout     = 30 * time.Second
	reconnectTimeout     = 15 * time.Second
	delayQueueTTL        = 500 * time.Millisecond
	minimumDelayObserved = 350 * time.Millisecond
)

var (
	integrationAddr     string
	integrationUsername string
	integrationPassword string
	queueCounter        atomic.Uint64
)

func TestMain(m *testing.M) {
	if os.Getenv(integrationEnv) != integrationEnabled {
		os.Exit(m.Run())
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), operationTimeout)
	container, err := rabbitmq.Run(
		setupCtx,
		rabbitMQImage,
		rabbitmq.WithAdminUsername("huma"),
		rabbitmq.WithAdminPassword("huma"),
	)
	setupCancel()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to start RabbitMQ Testcontainers module: %v\n", err)
		os.Exit(1)
	}

	connectionCtx, connectionCancel := context.WithTimeout(context.Background(), operationTimeout)
	connectionURL, err := container.AmqpURL(connectionCtx)
	connectionCancel()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to get RabbitMQ AMQP URL: %v\n", err)
		if terminateErr := terminateTestContainer(container); terminateErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to terminate RabbitMQ test container: %v\n", terminateErr)
		}
		os.Exit(1)
	}
	parsedURL, err := url.Parse(connectionURL)
	if err != nil || parsedURL.User == nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to parse RabbitMQ AMQP URL %q: %v\n", connectionURL, err)
		if terminateErr := terminateTestContainer(container); terminateErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to terminate RabbitMQ test container: %v\n", terminateErr)
		}
		os.Exit(1)
	}
	integrationAddr = parsedURL.Host
	integrationUsername = parsedURL.User.Username()
	var passwordSet bool
	integrationPassword, passwordSet = parsedURL.User.Password()
	if !passwordSet {
		_, _ = fmt.Fprintln(os.Stderr, "RabbitMQ AMQP URL does not contain a password")
		if terminateErr := terminateTestContainer(container); terminateErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to terminate RabbitMQ test container: %v\n", terminateErr)
		}
		os.Exit(1)
	}

	exitCode := m.Run()
	if err := terminateTestContainer(container); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to terminate RabbitMQ test container: %v\n", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}

func terminateTestContainer(container testcontainers.Container) error {
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	return container.Terminate(ctx)
}

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(integrationEnv) != integrationEnabled {
		t.Skipf("set %s=%s to run RabbitMQ integration tests", integrationEnv, integrationEnabled)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	t.Cleanup(cancel)
	return ctx
}

func testSDK(t *testing.T, cfg huma.SDKConfig) huma.RabbitMQKit {
	t.Helper()
	cfg.Addr = integrationAddr
	cfg.Username = integrationUsername
	cfg.Password = integrationPassword
	cfg.ConnectionName = fmt.Sprintf("huma-test-%d", queueCounter.Add(1))
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReconnectDelay == 0 {
		cfg.ReconnectDelay = 100 * time.Millisecond
	}

	sdk, err := huma.NewSDK(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		if err := sdk.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	return sdk
}

func testQueueName(t *testing.T, suffix string) huma.QueueName {
	t.Helper()
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "."))
	return huma.QueueName(fmt.Sprintf("huma.%s.%s.%d", name, suffix, queueCounter.Add(1)))
}

type receivedMessage struct {
	body        string
	contentType string
	messageID   string
	headers     amqp091.Table
}

func TestRabbitMQPublishAndBatchPublish(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	sdk := testSDK(t, huma.SDKConfig{
		PublisherPoolSize:    1,
		PublisherMaxPoolSize: 2,
		PublisherPoolWait:    time.Second,
	})

	queueName := testQueueName(t, "publish")
	received := make(chan receivedMessage, 3)
	queue := huma.QueueConfig{
		Name:       queueName,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(_ context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			received <- receivedMessage{
				body:        string(msg.Body),
				contentType: msg.ContentType,
				messageID:   msg.MessageId,
				headers:     msg.Headers,
			}
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.SetQos(ctx, 1, 0, false); err != nil {
		t.Fatalf("SetQos() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	messages := []*huma.Message{
		huma.NewMessage().WithBody([]byte("one")).WithHeader("batch", "first"),
		huma.NewMessage().WithBody([]byte("two")).WithContentType("application/custom").WithMessageID("message-two"),
		huma.NewMessage().WithJSONBody(map[string]string{"value": "three"}),
	}
	if err := sdk.BatchPublish(ctx, "", string(queueName), messages); err != nil {
		t.Fatalf("BatchPublish() error = %v", err)
	}

	want := []receivedMessage{
		{body: "one", contentType: "text/plain", headers: amqp091.Table{"batch": "first"}},
		{body: "two", contentType: "application/custom", messageID: "message-two"},
		{body: `{"value":"three"}`, contentType: "application/json"},
	}
	for _, expected := range want {
		select {
		case got := <-received:
			if got.body != expected.body || got.contentType != expected.contentType || got.messageID != expected.messageID {
				t.Fatalf("received message = %#v, want body/content type/message ID %#v", got, expected)
			}
			for key, value := range expected.headers {
				if got.headers[key] != value {
					t.Fatalf("received header %q = %v, want %v", key, got.headers[key], value)
				}
			}
		case <-ctx.Done():
			t.Fatalf("waiting for published messages: %v", ctx.Err())
		}
	}
}

func TestRabbitMQQuorumQueue(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	sdk := testSDK(t, huma.SDKConfig{})
	queueName := testQueueName(t, "quorum")
	received := make(chan string, 1)
	queue := huma.QueueConfig{
		Name:       queueName,
		QueueType:  huma.Quorum,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(_ context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			received <- string(msg.Body)
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := sdk.Publish(ctx, "", string(queueName), huma.NewMessage().WithBody([]byte("quorum message"))); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case got := <-received:
		if got != "quorum message" {
			t.Fatalf("received body = %q, want %q", got, "quorum message")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for quorum message: %v", ctx.Err())
	}
}

func TestRabbitMQBoundedRedeliveryRoutesToDLQ(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	sdk := testSDK(t, huma.SDKConfig{})
	queueName := testQueueName(t, "retry")
	dlqName := huma.QueueName(string(queueName) + ".DLQ")
	dlqDelivery := make(chan receivedMessage, 1)
	var attempts atomic.Int32

	queue := huma.QueueConfig{
		Name:          queueName,
		Durable:       true,
		EnableDLQ:     true,
		MaxRedelivery: 2,
		NumWorkers:    1,
		Handler: func(context.Context, huma.QueueName, huma.RabbitMQMsg) error {
			attempts.Add(1)
			return errors.New("expected integration test failure")
		},
	}
	dlq := huma.QueueConfig{
		Name:          dlqName,
		Durable:       true,
		MaxRedelivery: -1,
		NumWorkers:    1,
		Handler: func(_ context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			dlqDelivery <- receivedMessage{body: string(msg.Body), headers: msg.Headers}
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue, dlq); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue, dlq}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := sdk.Publish(ctx, "", string(queueName), huma.NewMessage().WithBody([]byte("retry me"))); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case got := <-dlqDelivery:
		if got.body != "retry me" {
			t.Fatalf("DLQ body = %q, want %q", got.body, "retry me")
		}
		if got.headers["x-huma-redelivery-count"] != int64(2) {
			t.Fatalf("DLQ redelivery count = %v, want 2", got.headers["x-huma-redelivery-count"])
		}
		if attempts.Load() != 3 {
			t.Fatalf("handler attempts = %d, want 3", attempts.Load())
		}
	case <-ctx.Done():
		t.Fatalf("waiting for DLQ delivery: %v", ctx.Err())
	}
}

func TestRabbitMQDelayDLXTTL(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	sdk := testSDK(t, huma.SDKConfig{})
	queueName := testQueueName(t, "delay")
	received := make(chan receivedMessage, 1)
	queue := huma.QueueConfig{
		Name:          queueName,
		Exchange:      huma.ExchangeName(string(queueName) + ".exchange"),
		Durable:       true,
		IsDelayQueue:  true,
		DelayStrategy: huma.DelayDLXTTL,
		DLXTTL:        delayQueueTTL,
		NumWorkers:    1,
		Handler: func(_ context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			received <- receivedMessage{body: string(msg.Body)}
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	started := time.Now()
	if err := sdk.PublishWithDelayDLXTTL(ctx, string(queueName), huma.NewMessage().WithBody([]byte("delayed"))); err != nil {
		t.Fatalf("PublishWithDelayDLXTTL() error = %v", err)
	}

	select {
	case got := <-received:
		if got.body != "delayed" {
			t.Fatalf("delayed body = %q, want %q", got.body, "delayed")
		}
		if elapsed := time.Since(started); elapsed < minimumDelayObserved {
			t.Fatalf("delay elapsed = %s, want at least %s", elapsed, minimumDelayObserved)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for delayed message: %v", ctx.Err())
	}
}

type integrationContextKey string

func TestRabbitMQHeaderHooks(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	var injectCalled atomic.Bool
	sdk := testSDK(t, huma.SDKConfig{
		InjectHeaders: func(_ context.Context, headers amqp091.Table) amqp091.Table {
			injectCalled.Store(true)
			result := amqp091.Table{}
			for key, value := range headers {
				result[key] = value
			}
			result["x-test-injected"] = "yes"
			return result
		},
		ExtractContext: func(ctx context.Context, msg amqp091.Delivery) context.Context {
			value, _ := msg.Headers["x-test-injected"].(string)
			return context.WithValue(ctx, integrationContextKey("header"), value)
		},
	})
	queueName := testQueueName(t, "headers")
	received := make(chan receivedMessage, 1)
	queue := huma.QueueConfig{
		Name:       queueName,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(ctx context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			received <- receivedMessage{body: string(msg.Body), headers: msg.Headers}
			if got := ctx.Value(integrationContextKey("header")); got != "yes" {
				t.Errorf("extracted context value = %v, want %q", got, "yes")
			}
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := sdk.Publish(ctx, "", string(queueName), huma.NewMessage().WithBody([]byte("headers")).WithHeader("x-test-original", "original")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case got := <-received:
		if got.body != "headers" {
			t.Fatalf("body = %q, want %q", got.body, "headers")
		}
		if got.headers["x-test-original"] != "original" || got.headers["x-test-injected"] != "yes" {
			t.Fatalf("headers = %#v, want original and injected headers", got.headers)
		}
		if !injectCalled.Load() {
			t.Fatal("InjectHeaders was not called")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for header message: %v", ctx.Err())
	}
}

func TestRabbitMQTracingPropagation(t *testing.T) {
	requireIntegration(t)
	setupTracer(t)
	ctx := testContext(t)
	sdk := testSDK(t, huma.SDKConfig{EnableTracing: true})
	queueName := testQueueName(t, "tracing")
	received := make(chan trace.SpanContext, 1)
	queue := huma.QueueConfig{
		Name:       queueName,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(ctx context.Context, _ huma.QueueName, _ huma.RabbitMQMsg) error {
			received <- trace.SpanContextFromContext(ctx)
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	parentCtx, parentSpan := otel.Tracer("huma-integration-test").Start(ctx, "publish")
	parentSpanContext := parentSpan.SpanContext()
	if err := sdk.Publish(parentCtx, "", string(queueName), huma.NewMessage().WithBody([]byte("trace"))); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	parentSpan.End()

	select {
	case got := <-received:
		if !got.IsValid() {
			t.Fatal("consumer span context is invalid")
		}
		if got.TraceID() != parentSpanContext.TraceID() {
			t.Fatalf("consumer trace ID = %v, want %v", got.TraceID(), parentSpanContext.TraceID())
		}
		if got.SpanID() == parentSpanContext.SpanID() {
			t.Fatal("consumer span should have a distinct span ID")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for traced message: %v", ctx.Err())
	}
}

func TestRabbitMQReconnectsAndRestoresConsumers(t *testing.T) {
	requireIntegration(t)
	ctx := testContext(t)
	containerCtx, containerCancel := context.WithTimeout(context.Background(), operationTimeout)
	container, err := rabbitmq.Run(
		containerCtx,
		rabbitMQImage,
		rabbitmq.WithAdminUsername(integrationUsername),
		rabbitmq.WithAdminPassword(integrationPassword),
	)
	containerCancel()
	if err != nil {
		t.Fatalf("rabbitmq.Run() error = %v", err)
	}
	t.Cleanup(func() {
		if err := terminateTestContainer(container); err != nil {
			t.Errorf("terminate reconnect test container: %v", err)
		}
	})
	connectionCtx, connectionCancel := context.WithTimeout(context.Background(), operationTimeout)
	connectionURL, err := container.AmqpURL(connectionCtx)
	connectionCancel()
	if err != nil {
		t.Fatalf("AmqpURL() error = %v", err)
	}
	parsedURL, err := url.Parse(connectionURL)
	if err != nil || parsedURL.User == nil {
		t.Fatalf("parse AMQP URL %q: %v", connectionURL, err)
	}
	password, passwordSet := parsedURL.User.Password()
	if !passwordSet {
		t.Fatal("RabbitMQ AMQP URL does not contain a password")
	}
	sdk, err := huma.NewSDK(context.Background(), huma.SDKConfig{
		Addr:                 parsedURL.Host,
		Username:             parsedURL.User.Username(),
		Password:             password,
		ConnectionName:       "huma-reconnect-test",
		ReconnectDelay:       100 * time.Millisecond,
		DialTimeout:          5 * time.Second,
		PublisherPoolSize:    1,
		PublisherMaxPoolSize: 2,
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), operationTimeout)
		defer shutdownCancel()
		if err := sdk.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	queueName := testQueueName(t, "reconnect")
	received := make(chan string, 2)
	queue := huma.QueueConfig{
		Name:       queueName,
		Durable:    true,
		NumWorkers: 1,
		Handler: func(_ context.Context, _ huma.QueueName, msg huma.RabbitMQMsg) error {
			received <- string(msg.Body)
			return nil
		},
	}
	if err := sdk.DeclareQueues(ctx, queue); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := sdk.Publish(ctx, "", string(queueName), huma.NewMessage().WithBody([]byte("before restart"))); err != nil {
		t.Fatalf("initial Publish() error = %v", err)
	}
	select {
	case got := <-received:
		if got != "before restart" {
			t.Fatalf("initial body = %q, want %q", got, "before restart")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for initial message: %v", ctx.Err())
	}

	if exitCode, _, err := container.Exec(ctx, []string{"rabbitmqctl", "stop_app"}); err != nil || exitCode != 0 {
		t.Fatalf("stop RabbitMQ application: exit code %d, error %v", exitCode, err)
	}
	if exitCode, _, err := container.Exec(ctx, []string{"rabbitmqctl", "start_app"}); err != nil || exitCode != 0 {
		t.Fatalf("start RabbitMQ application: exit code %d, error %v", exitCode, err)
	}

	reconnectDeadline := time.NewTimer(reconnectTimeout)
	defer reconnectDeadline.Stop()
	for {
		publishCtx, publishCancel := context.WithTimeout(ctx, time.Second)
		err = sdk.Publish(publishCtx, "", string(queueName), huma.NewMessage().WithBody([]byte("after restart")))
		publishCancel()
		if err == nil {
			break
		}
		select {
		case <-reconnectDeadline.C:
			t.Fatalf("SDK did not publish after RabbitMQ restart: %v", err)
		case <-ctx.Done():
			t.Fatalf("waiting for SDK reconnect: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	select {
	case got := <-received:
		if got != "after restart" {
			t.Fatalf("reconnected body = %q, want %q", got, "after restart")
		}
	case <-ctx.Done():
		t.Fatalf("waiting for message after reconnect: %v", ctx.Err())
	}
}
