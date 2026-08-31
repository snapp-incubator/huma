package main

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/snapp-incubator/huma"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sdk, err := huma.NewSDK(ctx, huma.SDKConfig{
		Addr:           "127.0.0.1:5672",
		Username:       "guest",
		Password:       "guest",
		ConnectionName: "huma-dlq-delay-example",
	})
	if err != nil {
		log.Fatal("failed to connect:", err)
	}

	// Queue with DLQ: failed messages (after MaxRedelivery) go to huma.orders.DLQ.
	ordersQueue := huma.QueueConfig{
		Name:           "huma.orders",
		Durable:        true,
		EnableDLQ:      true,
		MaxRedelivery:  3,
		NumWorkers:     2,
		ProcessTimeout: 5 * time.Second,
		Handler: func(ctx context.Context, queueName huma.QueueName, msg huma.RabbitMQMsg) error {
			log.Printf("processing order: %s", string(msg.Body))
			return errors.New("simulated processing error")
		},
	}

	// Delay queue using DLX+TTL (no plugin required).
	// Messages published to the delay queue are held for 10 s before routing to huma.scheduled.
	scheduledExchange := huma.ExchangeName("huma.scheduled.exchange")
	scheduledQueue := huma.QueueConfig{
		Name:           "huma.scheduled",
		Exchange:       scheduledExchange,
		RoutingKey:     "huma.scheduled",
		Durable:        true,
		IsDelayQueue:   true,
		DelayStrategy:  huma.DelayDLXTTL,
		DLXTTL:         10 * time.Second,
		NumWorkers:     1,
		ProcessTimeout: 5 * time.Second,
		Handler: func(ctx context.Context, queueName huma.QueueName, msg huma.RabbitMQMsg) error {
			log.Printf("received scheduled message: %s", string(msg.Body))
			return nil
		},
	}

	if err := sdk.DeclareQueues(ctx, ordersQueue, scheduledQueue); err != nil {
		log.Fatal("failed to declare queues:", err)
	}

	if err := sdk.Start(ctx, []huma.QueueConfig{ordersQueue, scheduledQueue}); err != nil {
		log.Fatal("failed to start consumers:", err)
	}

	// Publish to the DLQ-backed orders queue.
	orderMsg := huma.NewMessage().WithJSONBody(map[string]string{"order_id": "42"})
	if err := sdk.Publish(ctx, "", "huma.orders", orderMsg); err != nil {
		log.Println("publish error:", err)
	}

	// Publish a message that will be delivered after 10 s via DLX+TTL delay.
	delayMsg := huma.NewMessage().WithBody([]byte("scheduled task"))
	if err := sdk.PublishWithDelayDLXTTL(ctx, "huma.scheduled", delayMsg); err != nil {
		log.Println("publish with delay error:", err)
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sdk.Shutdown(shutdownCtx); err != nil {
		log.Println("SDK shutdown error:", err)
	}
}
