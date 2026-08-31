package huma

// Logger defines the logging interface for the SDK.
type Logger interface {
	Infof(template string, args ...any)
	Infow(msg string, keysAndValues ...any)
	Errorw(msg string, keysAndValues ...any)
}

// NoOpLogger is a logger that discards all output.
type NoOpLogger struct{}

// Infof discards a formatted informational log entry.
func (l *NoOpLogger) Infof(_ string, _ ...any) {}

// Infow discards a structured informational log entry.
func (l *NoOpLogger) Infow(_ string, _ ...any) {}

// Errorw discards a structured error log entry.
func (l *NoOpLogger) Errorw(_ string, _ ...any) {}
