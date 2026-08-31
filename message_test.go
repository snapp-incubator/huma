package huma_test

import (
	"reflect"
	"testing"

	"github.com/snapp-incubator/huma"
)

func TestMessage_WithJSONBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		v        any
		wantBody []byte
	}{
		{
			name:     "#1",
			v:        map[string]string{"t": "t"},
			wantBody: []byte(`{"t":"t"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := huma.NewMessage()
			got := m.WithJSONBody(tt.v)
			if !reflect.DeepEqual(got.Body, tt.wantBody) {
				t.Errorf("WithJSONBody() = %v, want %v", got.Body, tt.wantBody)
			}
		})
	}
}
