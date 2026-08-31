package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/amqp091-go"
	"github.com/snapp-incubator/huma"
)

const appIDHeader = "x-app-id"

// appIDKey is a context key for the application identifier.
type appIDKey struct{}

func appIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(appIDKey{}).(string); ok {
		return v
	}
	return ""
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sdk, err := huma.NewSDK(ctx, huma.SDKConfig{
		Addr:             "127.0.0.1:5672",
		Username:         "guest",
		Password:         "guest",
		ConnectionName:   "huma-metrics-example",
		EnableMetrics:    true,
		MetricsNamespace: "huma_example",

		// Optional extra label: add "app_id" alongside "queue_name" on every queue metric.
		MetricLabelName: "app_id",
		MetricLabelValue: func(ctx context.Context) string {
			return appIDFromContext(ctx)
		},
		InjectHeaders: func(ctx context.Context, headers amqp091.Table) amqp091.Table {
			if headers == nil {
				headers = amqp091.Table{}
			}
			headers[appIDHeader] = appIDFromContext(ctx)
			return headers
		},
		ExtractContext: func(ctx context.Context, delivery amqp091.Delivery) context.Context {
			if appID, ok := delivery.Headers[appIDHeader].(string); ok {
				return context.WithValue(ctx, appIDKey{}, appID)
			}
			return ctx
		},
	})
	if err != nil {
		log.Fatal("failed to connect:", err)
	}

	queues := []huma.QueueConfig{
		{
			Name:    "huma.metrics.example",
			Durable: true,
			Handler: func(ctx context.Context, queueName huma.QueueName, msg huma.RabbitMQMsg) error {
				log.Printf("received: %s", string(msg.Body))
				return nil
			},
			NumWorkers:     1,
			ProcessTimeout: 5 * time.Second,
		},
	}

	if err := sdk.DeclareQueues(ctx, queues...); err != nil {
		log.Fatal("failed to declare queues:", err)
	}

	if err := sdk.Start(ctx, queues); err != nil {
		log.Fatal("failed to start consumers:", err)
	}
	publishCtx := context.WithValue(ctx, appIDKey{}, "metrics-example")
	if err := sdk.Publish(publishCtx, "", "huma.metrics.example", huma.NewMessage().WithBody([]byte("metric example"))); err != nil {
		log.Println("publish error:", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:              ":2112",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       time.Minute,
	}

	go func() {
		log.Println("development metrics server listening on :2112")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("metrics server error:", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("metrics server shutdown error:", err)
	}
	if err := sdk.Shutdown(shutdownCtx); err != nil {
		log.Println("SDK shutdown error:", err)
	}
}
