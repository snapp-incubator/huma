package huma

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// Message holds the data for a message to be published.
type Message struct {
	Body        []byte
	Headers     amqp091.Table
	ContentType string
	Expiration  time.Duration
	Priority    uint8
	MessageID   string
	Timestamp   time.Time
	err         error
}

// NewMessage initializes an empty message with a current timestamp.
func NewMessage() *Message {
	return &Message{
		Headers:   amqp091.Table{},
		Timestamp: time.Now(),
	}
}

// WithBody sets the message body.
func (m *Message) WithBody(body []byte) *Message {
	m.Body = body
	return m
}

// WithJSONBody JSON-encodes v and sets Content-Type to application/json.
func (m *Message) WithJSONBody(v any) *Message {
	b, err := json.Marshal(v)
	if err != nil {
		m.err = fmt.Errorf("marshal message body: %w", err)
		return m
	}
	m.Body = b
	m.ContentType = "application/json"
	return m
}

// WithHeader adds a custom AMQP header.
func (m *Message) WithHeader(key string, value any) *Message {
	if m.Headers == nil {
		m.Headers = amqp091.Table{}
	}
	m.Headers[key] = value
	return m
}

// WithExpiration sets the message TTL.
func (m *Message) WithExpiration(d time.Duration) *Message {
	m.Expiration = d
	return m
}

// WithContentType sets the Content-Type header.
func (m *Message) WithContentType(contentType string) *Message {
	m.ContentType = contentType
	return m
}

// WithPriority sets the message priority.
func (m *Message) WithPriority(priority uint8) *Message {
	m.Priority = priority
	return m
}

// WithMessageID sets a custom message ID.
func (m *Message) WithMessageID(id string) *Message {
	m.MessageID = id
	return m
}

func (m *Message) validate() error {
	if m == nil {
		return errors.New("message is nil")
	}
	if m.err != nil {
		return m.err
	}
	if m.Headers == nil {
		m.Headers = amqp091.Table{}
	}
	if m.ContentType == "" {
		m.ContentType = "text/plain"
	}
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	return nil
}

func formatDurationToExpiration(d time.Duration) string {
	if d > 0 {
		return fmt.Sprintf("%d", d.Milliseconds())
	}
	return ""
}
