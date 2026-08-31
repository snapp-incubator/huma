package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/snapp-incubator/huma"
	"go.uber.org/zap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := logger.Sync(); err != nil {
			fmt.Println("logger sync error:", err)
		}
	}()

	sdk, err := huma.NewSDK(ctx, huma.SDKConfig{
		Addr:           "127.0.0.1:5672",
		Username:       "guest",
		Password:       "guest",
		ConnectionName: "huma-example",
	})
	if err != nil {
		logger.Fatal("failed to connect", zap.Error(err))
	}

	sdk.SetLogger(logger.Sugar())

	queues := []huma.QueueConfig{
		{
			Name:    "huma.example",
			Durable: true,
			Handler: func(ctx context.Context, queueName huma.QueueName, msg huma.RabbitMQMsg) error {
				logger.Info("message received", zap.String("body", string(msg.Body)))
				return nil
			},
			NumWorkers:     2,
			ProcessTimeout: 5 * time.Second,
		},
	}

	if err := sdk.DeclareQueues(ctx, queues...); err != nil {
		logger.Fatal("failed to declare queues", zap.Error(err))
	}

	if err := sdk.Start(ctx, queues); err != nil {
		logger.Fatal("failed to start consumers", zap.Error(err))
	}

	msg := huma.NewMessage().WithBody([]byte("hello from huma"))
	if err := sdk.Publish(ctx, "", "huma.example", msg); err != nil {
		fmt.Println("publish error:", err)
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sdk.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", zap.Error(err))
	}
}
