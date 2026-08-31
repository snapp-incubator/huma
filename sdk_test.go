package huma_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/snapp-incubator/huma"
)

func TestNewSDKRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ctx     context.Context
		config  huma.SDKConfig
		wantErr string
	}{
		{name: "nil context", config: huma.SDKConfig{Addr: "localhost:5672"}, wantErr: "context is nil"},
		{name: "missing address", ctx: context.Background(), wantErr: "address is required"},
		{name: "negative reconnect delay", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", ReconnectDelay: -time.Second}, wantErr: "reconnect delay"},
		{name: "negative dial timeout", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", DialTimeout: -time.Second}, wantErr: "dial timeout"},
		{name: "negative pool wait", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolWait: -time.Second}, wantErr: "pool wait"},
		{name: "negative pool size", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolSize: -1}, wantErr: "pool sizes"},
		{name: "maximum below initial", ctx: context.Background(), config: huma.SDKConfig{Addr: "localhost:5672", PublisherPoolSize: 2, PublisherMaxPoolSize: 1}, wantErr: "maximum pool size"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := huma.NewSDK(test.ctx, test.config)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewSDK() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestMessageWithHeaderInitializesHeaders(t *testing.T) {
	t.Parallel()

	var message huma.Message
	message.WithHeader("example", "value")
	if got := message.Headers["example"]; got != "value" {
		t.Fatalf("header = %v, want value", got)
	}
}
