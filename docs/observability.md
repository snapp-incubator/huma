# Observability

## Prometheus

Enable metrics by setting `EnableMetrics: true` in `SDKConfig`. The SDK registers the following metric families, all prefixed with your `MetricsNamespace` (default `"huma"`):

| Metric | Type | Description |
|---|---|---|
| `<ns>_messages_received_total` | Counter | Messages pulled off each queue |
| `<ns>_messages_acked_total` | Counter | Successfully processed messages |
| `<ns>_messages_nacked_total` | Counter | Failed or over-limit messages |
| `<ns>_publish_success_total` | Counter | Successful publishes |
| `<ns>_publish_failed_total` | Counter | Failed publishes |
| `<ns>_reconnects_total` | Counter | Successful RabbitMQ reconnections |
| `<ns>_message_processing_duration_seconds` | Histogram | End-to-end handler duration, including failed handlers |

Queue metrics carry a `queue_name` label. If you set `MetricLabelName`, those metrics receive a second label. `reconnects_total` has no labels.

### Scrape config

```yaml
scrape_configs:
  - job_name: "my-service"
    static_configs:
      - targets: ["localhost:2112"]
```

### Useful PromQL queries

Replace `huma` with your `MetricsNamespace`.

**Consume rate per queue (messages/s over 1 min)**
```promql
rate(huma_messages_received_total[1m])
```

**Ack/nack ratio**
```promql
rate(huma_messages_acked_total[5m])
/
(rate(huma_messages_acked_total[5m]) + rate(huma_messages_nacked_total[5m]))
```

**Publish failure rate**
```promql
rate(huma_publish_failed_total[5m])
/
(rate(huma_publish_success_total[5m]) + rate(huma_publish_failed_total[5m]))
```

**p95 processing latency per queue**
```promql
histogram_quantile(0.95,
  sum by (queue_name, le) (
    rate(huma_message_processing_duration_seconds_bucket[5m])
  )
)
```

**p99 processing latency per queue**
```promql
histogram_quantile(0.99,
  sum by (queue_name, le) (
    rate(huma_message_processing_duration_seconds_bucket[5m])
  )
)
```

**Reconnect spike detection**
```promql
increase(huma_reconnects_total[5m]) > 0
```

## OpenTelemetry

Enable trace propagation by setting `EnableTracing: true`. The SDK uses the global OTel tracer provider and propagator, so wire them up before calling `NewSDK`:

```go
otel.SetTracerProvider(tp)
otel.SetTextMapPropagator(propagation.TraceContext{})
```

On consume, the SDK extracts the trace context from AMQP headers and starts a child span named `"huma.consume"` with `SpanKindConsumer`. On publish, it injects any valid current span context into headers.

See `examples/tracing/main.go` for a complete setup with the stdout exporter.

## Grafana dashboard

Import `docs/grafana-dashboard.json` into Grafana (Dashboards -> Import). The dashboard expects a Prometheus data source and provides panels for:

- Throughput: receive/ack/nack rates per queue
- Error ratio: nack percentage over time
- Publish failures: rate of failed publishes
- Processing latency: p50/p95/p99 quantiles and a heatmap
- Reconnect events: reconnect spike timeline

The dashboard is templated by `namespace` (your `MetricsNamespace`) and `queue_name`.
