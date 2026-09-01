# Contributing to huma

## Local setup

You need Go 1.25+ and Docker (for running the Testcontainers-backed RabbitMQ integration tests).

```
git clone https://github.com/snapp-incubator/huma.git
cd huma
go mod download
```

Run the test suite:

```
make test
```

Run the RabbitMQ integration tests. Testcontainers starts and removes an isolated broker:

```
make integration
```

Lint:

```
make lint
```

Regenerate the mock (requires `mockgen`):

```
make mock
```

Run an example against a local RabbitMQ:

```
docker compose -f examples/docker-compose.yml up -d
go run ./examples/basic
```

## Branch naming

Use `feat/<short-description>` for new features, `fix/<short-description>` for bug fixes, and `docs/<short-description>` for documentation changes.

## Commit style

This repo follows [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(metrics): add optional extra label hook
fix(reconnect): restore vhost on reconnect
docs(readme): add delay queue example
```

## Go standards

- Exported operations that perform I/O or manage lifecycle accept `context.Context` as the first parameter.
- Test files use the external package form: `package huma_test`.
- All comments in `.go` files are full sentences ending with a period.
- Never ignore a returned error.
- Use `errors.New` for static messages; `fmt.Errorf` only when formatting is needed.
- Table-driven tests use `t.Parallel()` for each sub-test.
- Prefer constants over repeated string/number literals.

## Pull request workflow

1. Open a PR against `main`.
2. CI runs lint, race tests, builds, and CodeQL analysis.
3. A maintainer will review and merge. Squash commits are preferred.

GitHub may hold workflows from first-time or external contributors for maintainer approval.

The project currently requires neither a contributor license agreement nor DCO sign-off.

## Reporting issues

Use the GitHub issue templates. For security vulnerabilities, follow [SECURITY.md](SECURITY.md) instead.
