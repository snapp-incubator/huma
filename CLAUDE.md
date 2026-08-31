# Huma Project Notes

## Overview

Huma is a standalone Go module (`github.com/snapp-incubator/huma`) providing RabbitMQ
consumers and publishers on top of `rabbitmq/amqp091-go`.

## Package Layout

All SDK code lives in the root `huma` package.

| File | Purpose |
|---|---|
| `rabbitmqsdk.go` | Construction, topology, consumers, publishing, retries, and shutdown. |
| `types.go` | SDK, configuration, queue, and message delivery types. |
| `publisher_pool.go` | Confirm-enabled publisher channel pool. |
| `reconnect.go` | Connection monitoring and recovery. |
| `metrics.go` | Prometheus and no-op metrics implementations. |
| `trace.go` | OpenTelemetry AMQP header carrier. |
| `message.go` | Message builder. |
| `rabbitmqkit.go` | Public interface and mock generation directive. |
| `mock.go` | Generated GoMock implementation; do not edit manually. |

Examples are under `examples/`; operational documentation is under `docs/`.

## Go Standards

- I/O and lifecycle operations accept `context.Context` first.
- Tests use the external `huma_test` package.
- Go comments are complete sentences ending with periods.
- Returned errors are handled.
- Static errors use `errors.New`; formatted errors use `fmt.Errorf`.
- Parallel-safe table tests call `t.Parallel()` in the test and subtests.
- Repeated literals use constants.

## Commands

```sh
make build
make test
make integration
make lint
make mock
make tidy
```

`make mock` runs the pinned `go.uber.org/mock` generator through `go generate`.

Start local infrastructure with:

```sh
docker compose -f examples/docker-compose.yml up -d
```

Run examples separately with `go run ./examples/basic`, `go run ./examples/metrics`,
`go run ./examples/tracing`, or `go run ./examples/dlq-and-delay`.

Before publishing, review the full Git history, dependency graph, generated files, workflow
permissions, and repository visibility. Never commit credentials or private planning material.
