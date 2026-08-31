package huma

import (
	"context"
)

//go:generate go tool mockgen -package=huma -source=rabbitmqkit.go -destination=mock.go

// RabbitMQKit is the primary interface for the huma SDK.
type RabbitMQKit interface {
	DeclareQueues(ctx context.Context, queues ...QueueConfig) error
	Publish(ctx context.Context, exchange, routingKey string, msg *Message) error
	BatchPublish(ctx context.Context, exchange, routingKey string, messages []*Message) error
	PublishWithDelayDLXTTL(ctx context.Context, queueName string, msg *Message) error
	SetQos(ctx context.Context, prefetchCount, prefetchSize int, global bool) error
	Start(ctx context.Context, queues []QueueConfig) error
	Shutdown(ctx context.Context) error
	SetLogger(logger Logger)
}
