package huma

import (
	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
)

// AMQPHeaderCarrier adapts amqp091.Table to satisfy propagation.TextMapCarrier,
// enabling OTel propagators to inject/extract trace context via AMQP message headers.
type AMQPHeaderCarrier amqp091.Table

var _ propagation.TextMapCarrier = AMQPHeaderCarrier{}

// Get returns a string header value or an empty string for other value types.
func (c AMQPHeaderCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// Set stores a string header value.
func (c AMQPHeaderCarrier) Set(key string, value string) {
	c[key] = value
}

// Keys returns all header keys.
func (c AMQPHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
