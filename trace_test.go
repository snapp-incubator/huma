package huma_test

import (
	"context"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/snapp-incubator/huma"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	traceSDK "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setupTracer(t *testing.T) {
	t.Helper()
	tp := traceSDK.NewTracerProvider()
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("tracer provider shutdown: %v", err)
		}
	})
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

func TestAMQPHeaderCarrier_Roundtrip(t *testing.T) {
	setupTracer(t)

	ctx, span := otel.Tracer("test").Start(context.Background(), "parent-span")
	parentSpanCtx := span.SpanContext()
	span.End()

	headers := amqp091.Table{}
	otel.GetTextMapPropagator().Inject(ctx, huma.AMQPHeaderCarrier(headers))

	extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), huma.AMQPHeaderCarrier(headers))
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	if !extractedSpanCtx.IsValid() {
		t.Fatal("extracted span context should be valid")
	}
	if extractedSpanCtx.TraceID() != parentSpanCtx.TraceID() {
		t.Fatalf("trace ID mismatch: got %v, want %v", extractedSpanCtx.TraceID(), parentSpanCtx.TraceID())
	}
	if extractedSpanCtx.SpanID() != parentSpanCtx.SpanID() {
		t.Fatalf("span ID mismatch: got %v, want %v", extractedSpanCtx.SpanID(), parentSpanCtx.SpanID())
	}
}

func TestAMQPHeaderCarrier_EmptyHeaders(t *testing.T) {
	setupTracer(t)

	headers := amqp091.Table{}
	extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), huma.AMQPHeaderCarrier(headers))
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	if extractedSpanCtx.IsValid() {
		t.Fatal("no trace context in headers should yield invalid span context")
	}
}

func TestAMQPHeaderCarrier_ProducerConsumerChain(t *testing.T) {
	setupTracer(t)

	prodCtx, prodSpan := otel.Tracer("producer").Start(context.Background(), "publish")
	prodTraceID := prodSpan.SpanContext().TraceID()
	prodSpanID := prodSpan.SpanContext().SpanID()
	headers := amqp091.Table{}
	otel.GetTextMapPropagator().Inject(prodCtx, huma.AMQPHeaderCarrier(headers))
	prodSpan.End()

	extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), huma.AMQPHeaderCarrier(headers))
	_, consumerSpan := otel.Tracer("consumer").Start(extractedCtx, "consume")
	consumerSpan.End()

	if consumerSpan.SpanContext().TraceID() != prodTraceID {
		t.Fatalf("consumer span must share producer trace ID: got %v, want %v",
			consumerSpan.SpanContext().TraceID(), prodTraceID)
	}
	if consumerSpan.SpanContext().SpanID() == prodSpanID {
		t.Fatal("consumer span should have its own span ID, not reuse the producer's")
	}
}

func TestAMQPHeaderCarrier_Get(t *testing.T) {
	t.Parallel()

	headers := huma.AMQPHeaderCarrier{
		"str-key": "value",
		"int-key": 42,
	}

	if got := headers.Get("str-key"); got != "value" {
		t.Fatalf("Get(str-key) = %q, want %q", got, "value")
	}
	if got := headers.Get("int-key"); got != "" {
		t.Fatalf("Get(int-key) = %q, want empty string", got)
	}
	if got := headers.Get("nonexistent"); got != "" {
		t.Fatalf("Get(nonexistent) = %q, want empty string", got)
	}
}

func TestAMQPHeaderCarrier_Keys(t *testing.T) {
	t.Parallel()

	headers := huma.AMQPHeaderCarrier{"a": "1", "b": "2", "c": "3"}
	keys := headers.Keys()

	if len(keys) != 3 {
		t.Fatalf("Keys() returned %d items, want 3", len(keys))
	}
	seen := map[string]bool{}
	for _, k := range keys {
		seen[k] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Fatalf("Keys() missing %q", want)
		}
	}
}

func TestAMQPHeaderCarrier_CoexistsWithExistingHeaders(t *testing.T) {
	setupTracer(t)

	ctx, span := otel.Tracer("test").Start(context.Background(), "publish")
	prodTraceID := span.SpanContext().TraceID()
	span.End()

	headers := amqp091.Table{
		"x-app-id": "example",
	}

	otel.GetTextMapPropagator().Inject(ctx, huma.AMQPHeaderCarrier(headers))

	// App header must be preserved alongside the injected trace context.
	if headers["x-app-id"] != "example" {
		t.Fatalf("app header overwritten: got %v", headers["x-app-id"])
	}

	extractedCtx := otel.GetTextMapPropagator().Extract(context.Background(), huma.AMQPHeaderCarrier(headers))
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	if !extractedSpanCtx.IsValid() {
		t.Fatal("extracted span context should be valid")
	}
	if extractedSpanCtx.TraceID() != prodTraceID {
		t.Fatalf("trace ID mismatch: got %v, want %v", extractedSpanCtx.TraceID(), prodTraceID)
	}
}
