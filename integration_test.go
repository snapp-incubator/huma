package huma_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snapp-incubator/huma"
)

func TestBoundedRedeliveryRoutesToDLQ(t *testing.T) {
	if os.Getenv("HUMA_INTEGRATION") != "1" {
		t.Skip("set HUMA_INTEGRATION=1 to run RabbitMQ integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := huma.NewSDK(ctx, huma.SDKConfig{
		Addr:           "127.0.0.1:5672",
		Username:       "guest",
		Password:       "guest",
		ConnectionName: "huma-integration-test",
	})
	if err != nil {
		t.Fatalf("NewSDK() error = %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := sdk.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	queueName := fmt.Sprintf("huma.integration.%d", time.Now().UnixNano())
	dlqName := queueName + ".DLQ"
	dlqDelivery := make(chan struct{}, 1)
	var attempts atomic.Int32

	queue := huma.QueueConfig{
		Name:          huma.QueueName(queueName),
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
		Name:          huma.QueueName(dlqName),
		Durable:       true,
		MaxRedelivery: -1,
		NumWorkers:    1,
		Handler: func(context.Context, huma.QueueName, huma.RabbitMQMsg) error {
			dlqDelivery <- struct{}{}
			return nil
		},
	}

	if err := sdk.DeclareQueues(ctx, queue, dlq); err != nil {
		t.Fatalf("DeclareQueues() error = %v", err)
	}
	if err := sdk.Start(ctx, []huma.QueueConfig{queue, dlq}); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := sdk.Publish(ctx, "", queueName, huma.NewMessage().WithBody([]byte("retry me"))); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case <-ctx.Done():
		t.Fatalf("waiting for DLQ delivery: %v", ctx.Err())
	case <-dlqDelivery:
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("handler attempts = %d, want 3", got)
	}
}
