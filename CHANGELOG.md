# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-08-31

### Added

- `NewSDK` factory with configurable connection name, heartbeat, reconnect delay, and publisher pool.
- `Start` / `DeclareQueues` / `Publish` / `BatchPublish` / `PublishWithDelayDLXTTL` / `Shutdown` API.
- Publisher channel pool with configurable min/max size, wait timeout, and broker confirms.
- Automatic reconnect with topology replay (queues, QoS) and consumer restart.
- Dead-letter queue support (`EnableDLQ`): declares `<queue>.DLX` and `<queue>.DLQ` automatically.
- Delayed delivery through a dead-letter exchange and queue-level TTL.
- Quorum queue support (`QueueType: huma.Quorum`).
- `MaxRedelivery` with infinite, bounded, and no-SDK-retry modes.
- Prometheus metrics: receive, ack, nack, publish success/failure, reconnect counters, processing duration histogram.
- Optional extra Prometheus label via `MetricLabelName` / `MetricLabelValue` hooks.
- OpenTelemetry trace propagation via AMQP headers (`EnableTracing`).
- `InjectHeaders` and `ExtractContext` hooks for arbitrary per-message context propagation.
- `AMQPHeaderCarrier` for OTel propagator compatibility.
- `MockRabbitMQKit` generated mock.
- `Logger` interface with `NoOpLogger` default.
- Examples: basic, metrics, tracing, DLQ and delay, docker-compose.
- CI workflow (lint, race tests, build), CodeQL analysis, Dependabot.
- Grafana dashboard and PromQL reference in `docs/observability.md`.

[Unreleased]: https://github.com/snapp-incubator/huma/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/snapp-incubator/huma/releases/tag/v0.1.0
