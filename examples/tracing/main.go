package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/snapp-incubator/huma"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func initTracer() func(context.Context) error {
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal("failed to create trace exporter:", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown := initTracer()
	defer func() {
		if err := shutdown(context.Background()); err != nil {
			log.Println("tracer shutdown error:", err)
		}
	}()

	sdk, err := huma.NewSDK(ctx, huma.SDKConfig{
		Addr:           "127.0.0.1:5672",
		Username:       "guest",
		Password:       "guest",
		ConnectionName: "huma-tracing-example",
		EnableTracing:  true,
	})
	if err != nil {
		log.Fatal("failed to connect:", err)
	}

	queues := []huma.QueueConfig{
		{
			Name:    "huma.tracing.example",
			Durable: true,
			Handler: func(ctx context.Context, queueName huma.QueueName, msg huma.RabbitMQMsg) error {
				tracer := otel.Tracer("huma-example")
				_, span := tracer.Start(ctx, "process-message")
				defer span.End()
				log.Printf("processing message in trace context: %s", string(msg.Body))
				time.Sleep(10 * time.Millisecond)
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

	tracer := otel.Tracer("huma-example")
	publishCtx, span := tracer.Start(ctx, "publish-messages")

	for i := range 3 {
		msg := huma.NewMessage().WithBody([]byte("traced message"))
		if err := sdk.Publish(publishCtx, "", "huma.tracing.example", msg); err != nil {
			log.Printf("publish %d error: %v", i, err)
		}
	}

	span.End()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sdk.Shutdown(shutdownCtx); err != nil {
		log.Println("SDK shutdown error:", err)
	}
}
