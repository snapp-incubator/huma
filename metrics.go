package huma

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics wraps the SDK metrics interface.
type Metrics interface {
	IncMessagesReceived(ctx context.Context, queue string)
	ObserveProcessingDuration(ctx context.Context, queue string, durationSeconds float64)
	IncMessagesAcked(ctx context.Context, queue string)
	IncMessagesNacked(ctx context.Context, queue string)
	IncPublishSuccess(ctx context.Context, queue string)
	IncPublishFailed(ctx context.Context, queue string)
	IncReconnects(ctx context.Context)
}

type prometheusMetrics struct {
	messagesReceived          *prometheus.CounterVec
	messageProcessingDuration *prometheus.HistogramVec
	messagesAcked             *prometheus.CounterVec
	messagesNacked            *prometheus.CounterVec
	publishSuccess            *prometheus.CounterVec
	publishFailed             *prometheus.CounterVec
	reconnects                prometheus.Counter
	labelName                 string
	labelValue                func(ctx context.Context) string
}

func newPrometheusMetrics(
	ctx context.Context,
	registerer prometheus.Registerer,
	namespace, labelName string,
	labelValue func(ctx context.Context) string,
) (*prometheusMetrics, error) {
	_ = ctx
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	labels := []string{"queue_name"}
	if labelName != "" {
		labels = append(labels, labelName)
	}

	m := &prometheusMetrics{
		labelName:  labelName,
		labelValue: labelValue,
		messagesReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
			Help:      "Total number of messages received per queue",
		}, labels),
		messageProcessingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "message_processing_duration_seconds",
				Help:      "Time taken to process each message per queue",
				Buckets:   append(prometheus.DefBuckets, 300, 600, 1800, 3600, 18000),
			},
			labels,
		),
		messagesAcked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_acked_total",
			Help:      "Total number of messages acknowledged per queue",
		}, labels),
		messagesNacked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_nacked_total",
			Help:      "Total number of messages negatively acknowledged per queue",
		}, labels),
		publishSuccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_success_total",
			Help:      "Total number of successful publishes per queue",
		}, labels),
		publishFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_failed_total",
			Help:      "Total number of failed publishes per queue",
		}, labels),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "reconnects_total",
			Help:      "Total number of successful reconnects",
		}),
	}

	collectors := []prometheus.Collector{
		m.messagesReceived, m.messageProcessingDuration, m.messagesAcked,
		m.messagesNacked, m.publishSuccess, m.publishFailed, m.reconnects,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, previous := range registered {
				registerer.Unregister(previous)
			}
			return nil, fmt.Errorf("register Prometheus metrics: %w", err)
		}
		registered = append(registered, collector)
	}

	return m, nil
}

func (m *prometheusMetrics) extraLabel(ctx context.Context) []string {
	if m.labelName == "" {
		return nil
	}
	val := ""
	if m.labelValue != nil {
		val = m.labelValue(ctx)
	}
	return []string{val}
}

func (m *prometheusMetrics) IncMessagesReceived(ctx context.Context, queue string) {
	m.messagesReceived.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Inc()
}

func (m *prometheusMetrics) ObserveProcessingDuration(ctx context.Context, queue string, durationSeconds float64) {
	m.messageProcessingDuration.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Observe(durationSeconds)
}

func (m *prometheusMetrics) IncMessagesAcked(ctx context.Context, queue string) {
	m.messagesAcked.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Inc()
}

func (m *prometheusMetrics) IncMessagesNacked(ctx context.Context, queue string) {
	m.messagesNacked.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Inc()
}

func (m *prometheusMetrics) IncPublishSuccess(ctx context.Context, queue string) {
	m.publishSuccess.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Inc()
}

func (m *prometheusMetrics) IncPublishFailed(ctx context.Context, queue string) {
	m.publishFailed.WithLabelValues(append([]string{queue}, m.extraLabel(ctx)...)...).Inc()
}

func (m *prometheusMetrics) IncReconnects(_ context.Context) {
	m.reconnects.Inc()
}

type noOpMetrics struct{}

func (n *noOpMetrics) IncMessagesReceived(_ context.Context, _ string)                  {}
func (n *noOpMetrics) ObserveProcessingDuration(_ context.Context, _ string, _ float64) {}
func (n *noOpMetrics) IncMessagesAcked(_ context.Context, _ string)                     {}
func (n *noOpMetrics) IncMessagesNacked(_ context.Context, _ string)                    {}
func (n *noOpMetrics) IncPublishSuccess(_ context.Context, _ string)                    {}
func (n *noOpMetrics) IncPublishFailed(_ context.Context, _ string)                     {}
func (n *noOpMetrics) IncReconnects(_ context.Context)                                  {}
