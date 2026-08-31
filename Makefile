.PHONY: build test integration lint mock tidy example

build:
	go build ./...

test:
	go test -race -coverprofile=coverage.out ./...

integration:
	HUMA_INTEGRATION=1 go test -race -run TestBoundedRedeliveryRoutesToDLQ ./...

lint:
	golangci-lint run ./...

mock:
	go generate ./...

tidy:
	go mod tidy

example:
	docker compose -f examples/docker-compose.yml up -d
	@echo "RabbitMQ management UI: http://localhost:15672  (guest/guest)"
	@echo "Run an example: go run ./examples/basic"
