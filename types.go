package huma

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rabbitmq/amqp091-go"
)

// SDK is the huma provider struct.
type SDK struct {
	conn              *amqp091.Connection
	channel           *amqp091.Channel
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	logger            Logger
	reconnectInterval time.Duration
	queues            []QueueConfig
	mu                sync.Mutex
	publisherMu       sync.RWMutex
	reconnecting      atomic.Bool
	shutdownOnce      sync.Once
	closeOnce         sync.Once
	closeErr          error
	declaredQueues    []QueueConfig
	qosConfigured     bool
	qosPrefetchCount  int
	qosPrefetchSize   int
	qosGlobal         bool

	metrics Metrics

	connectionConfig     amqp091.Config
	connectionURL        string
	connectionState      *connectionState
	publisherPoolSize    int
	publisherMaxPoolSize int
	publisherPoolWait    time.Duration
	publisherPool        *publisherChannelPool
	enableTracing        bool

	extractContext func(ctx context.Context, d amqp091.Delivery) context.Context
	injectHeaders  func(ctx context.Context, headers amqp091.Table) amqp091.Table
}

type connectionState struct {
	mu   sync.Mutex
	conn net.Conn
}

// SDKConfig is the config struct for the huma SDK.
type SDKConfig struct {
	Addr           string        `yaml:"ADDRESS" json:"ADDRESS" mapstructure:"ADDRESS"`
	VHost          string        `yaml:"VHOST" json:"VHOST" mapstructure:"VHOST"`
	Username       string        `yaml:"USERNAME" json:"USERNAME" mapstructure:"USERNAME"`
	Password       string        `yaml:"PASSWORD" json:"PASSWORD" mapstructure:"PASSWORD"`
	Heartbeat      time.Duration `yaml:"HEARTBEAT" json:"HEARTBEAT" mapstructure:"HEARTBEAT"`                   // Heartbeat interval
	DialTimeout    time.Duration `yaml:"DIAL_TIMEOUT" json:"DIAL_TIMEOUT" mapstructure:"DIAL_TIMEOUT"`          // TCP connection timeout
	ConnectionName string        `yaml:"CONNECTION_NAME" json:"CONNECTION_NAME" mapstructure:"CONNECTION_NAME"` // Connection identifier
	ReconnectDelay time.Duration `yaml:"RECONNECT_DELAY" json:"RECONNECT_DELAY" mapstructure:"RECONNECT_DELAY"` // How often to try reconnecting
	TLSConfig      *tls.Config   `yaml:"-" json:"-" mapstructure:"-"`                                           // TLS configuration for AMQPS

	// Prometheus metrics options.
	EnableMetrics     bool                  `yaml:"ENABLE_METRICS" json:"ENABLE_METRICS" mapstructure:"ENABLE_METRICS"`          // Enable Prometheus metrics
	MetricsNamespace  string                `yaml:"METRICS_NAMESPACE" json:"METRICS_NAMESPACE" mapstructure:"METRICS_NAMESPACE"` // Prometheus metrics namespace
	MetricsRegisterer prometheus.Registerer `yaml:"-" json:"-" mapstructure:"-"`                                                 // Prometheus collector registerer

	PublisherPoolSize    int           `yaml:"PUBLISHER_POOL_SIZE" json:"PUBLISHER_POOL_SIZE" mapstructure:"PUBLISHER_POOL_SIZE"`             // Initial channel pool size (default 5)
	PublisherMaxPoolSize int           `yaml:"PUBLISHER_MAX_POOL_SIZE" json:"PUBLISHER_MAX_POOL_SIZE" mapstructure:"PUBLISHER_MAX_POOL_SIZE"` // Maximum channel pool size
	PublisherPoolWait    time.Duration `yaml:"PUBLISHER_POOL_WAIT" json:"PUBLISHER_POOL_WAIT" mapstructure:"PUBLISHER_POOL_WAIT"`             // How long a goroutine waits for a free channel before a new one is created

	EnableTracing bool `yaml:"ENABLE_TRACING" json:"ENABLE_TRACING" mapstructure:"ENABLE_TRACING"` // Enable OpenTelemetry trace context propagation through AMQP headers

	// MetricLabelName, when non-empty, adds one extra Prometheus label to queue metrics.
	// MetricLabelValue derives its value from the message context.
	MetricLabelName  string                           `yaml:"-" json:"-" mapstructure:"-"`
	MetricLabelValue func(ctx context.Context) string `yaml:"-" json:"-" mapstructure:"-"`

	// InjectHeaders, if set, is called on publish to enrich AMQP headers from the context.
	// The returned table is used as the final headers.
	InjectHeaders func(ctx context.Context, headers amqp091.Table) amqp091.Table `yaml:"-" json:"-" mapstructure:"-"`

	// ExtractContext, if set, is called on consume to derive a child context from the delivery
	// (e.g. restore an app id into the context before the handler runs).
	ExtractContext func(ctx context.Context, d amqp091.Delivery) context.Context `yaml:"-" json:"-" mapstructure:"-"`
}

// QueueName identifies a RabbitMQ queue.
type QueueName string

// ExchangeName identifies a RabbitMQ exchange.
type ExchangeName string

// RoutingKeyName identifies a RabbitMQ routing key.
type RoutingKeyName string

// DelayStrategy defines which mechanism a delay queue uses.
type DelayStrategy int

const (
	// DelayDLXTTL uses a dead-letter exchange and queue-level TTL to implement delay.
	DelayDLXTTL DelayStrategy = iota
)

// QueueType represents the RabbitMQ queue type.
type QueueType string

const (
	// Classic uses a RabbitMQ classic queue.
	Classic QueueType = "classic"
	// Quorum uses a RabbitMQ quorum queue.
	Quorum QueueType = "quorum"
)

// MsgHandler is the prototype of a function that processes messages.
//
// The target context is a context.WithCancel that is canceled on Shutdown, enabling
// graceful shutdown of heavy processing loops.
// The queueName parameter lets a single handler serve multiple queue types.
type MsgHandler func(ctx context.Context, queueName QueueName, msg RabbitMQMsg) error

// QueueConfig is the queue configuration struct.
type QueueConfig struct {
	Name       QueueName  `yaml:"NAME" json:"NAME" mapstructure:"NAME"`
	QueueType  QueueType  `yaml:"QUEUE_TYPE" json:"QUEUE_TYPE" mapstructure:"QUEUE_TYPE"`
	Handler    MsgHandler `yaml:"-" json:"-" mapstructure:"-"`
	NumWorkers int        `yaml:"NUM_WORKERS" json:"NUM_WORKERS" mapstructure:"NUM_WORKERS"`

	IsDelayQueue  bool          `yaml:"IS_DELAY_QUEUE" json:"IS_DELAY_QUEUE" mapstructure:"IS_DELAY_QUEUE"`
	DelayStrategy DelayStrategy `yaml:"DELAY_STRATEGY" json:"DELAY_STRATEGY" mapstructure:"DELAY_STRATEGY"`
	DLXTTL        time.Duration `yaml:"TTL" json:"TTL" mapstructure:"TTL"` // Only used for DLX+TTL delay queue

	// MaxRedelivery controls message redelivery behavior when processing fails:
	//
	// MaxRedelivery = 0:  INFINITE REDELIVERY (standard RabbitMQ behavior)
	//                     Messages are requeued indefinitely until successfully processed.
	//                     Use this for critical messages that must eventually be processed.
	//
	// MaxRedelivery > 0:  LIMITED REDELIVERY
	//                     Messages are redelivered up to N times, then discarded or sent to DLQ.
	//                     Example: MaxRedelivery = 3 means the message is processed up to 4 times total
	//                     (original delivery + 3 retries).
	//
	// MaxRedelivery = -1: NO SDK-REQUESTED REDELIVERY
	//                     Handler failures are rejected without requeuing. RabbitMQ can still
	//                     redeliver after a connection loss. Configured DLQ routing still applies.
	MaxRedelivery int `yaml:"MAX_REDELIVERY" json:"MAX_REDELIVERY" mapstructure:"MAX_REDELIVERY"`

	Exchange        ExchangeName   `yaml:"EXCHANGE" json:"EXCHANGE" mapstructure:"EXCHANGE"`
	RoutingKey      RoutingKeyName `yaml:"ROUTING_KEY" json:"ROUTING_KEY" mapstructure:"ROUTING_KEY"`
	Durable         bool           `yaml:"DURABLE" json:"DURABLE" mapstructure:"DURABLE"`
	AutoDelete      bool           `yaml:"AUTO_DELETE" json:"AUTO_DELETE" mapstructure:"AUTO_DELETE"`
	ConsumerTag     string         `yaml:"CONSUMER_TAG" json:"CONSUMER_TAG" mapstructure:"CONSUMER_TAG"`
	Exclusive       bool           `yaml:"EXCLUSIVE" json:"EXCLUSIVE" mapstructure:"EXCLUSIVE"`
	NoWait          bool           `yaml:"NO_WAIT" json:"NO_WAIT" mapstructure:"NO_WAIT"`
	ConsumerTimeout time.Duration  `yaml:"CONSUMER_TIMEOUT" json:"CONSUMER_TIMEOUT" mapstructure:"CONSUMER_TIMEOUT"`

	// EnableDLQ declares a dead-letter exchange and dead-letter queue for this queue.
	// When true, two additional resources are created: an exchange named <Queue>.DLX
	// and a queue named <Queue>.DLQ.
	EnableDLQ bool `yaml:"ENABLE_DLQ" json:"ENABLE_DLQ" mapstructure:"ENABLE_DLQ"`

	ProcessTimeout time.Duration `yaml:"PROCESS_TIMEOUT" json:"PROCESS_TIMEOUT" mapstructure:"PROCESS_TIMEOUT"`

	DummyMessageEnabled   bool          `yaml:"DUMMY_MESSAGE_ENABLED" json:"DUMMY_MESSAGE_ENABLED" mapstructure:"DUMMY_MESSAGE_ENABLED"`
	DummyMessageFrequency time.Duration `yaml:"DUMMY_MESSAGE_FREQUENCY" json:"DUMMY_MESSAGE_FREQUENCY" mapstructure:"DUMMY_MESSAGE_FREQUENCY"`
}

// RabbitMQMsg wraps the amqp091 Delivery struct.
type RabbitMQMsg struct {
	amqp091.Delivery
}

var heartBeatMSG = amqp091.Publishing{
	Headers: amqp091.Table{
		"type": "heartbeat",
	},
	Body: []byte("heartbeat"),
}
