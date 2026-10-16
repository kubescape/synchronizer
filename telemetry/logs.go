package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.opentelemetry.io/otel/log/noop"
)

const scope = "github.com/kubescape/synchronizer/telemetry"

// Only fixed operational records from this scope are exported. Existing logs
// retain their local behavior; arbitrary bodies/attributes never enter OTLP.
type filteredProvider struct {
	embedded.LoggerProvider
	delegate log.LoggerProvider
	mu       sync.Mutex
	last     map[string]time.Time
	now      func() time.Time
}

func newFilteredProvider(p log.LoggerProvider) *filteredProvider {
	return &filteredProvider{delegate: p, last: make(map[string]time.Time), now: time.Now}
}
func (p *filteredProvider) Logger(name string, opts ...log.LoggerOption) log.Logger {
	if name != scope {
		return noop.NewLoggerProvider().Logger(name)
	}
	return &filteredLogger{delegate: p.delegate.Logger(name, opts...), provider: p}
}

type filteredLogger struct {
	embedded.Logger
	delegate log.Logger
	provider *filteredProvider
}

func (l *filteredLogger) Enabled(_ context.Context, p log.EnabledParameters) bool {
	return p.Severity >= log.SeverityWarn
}
func (l *filteredLogger) Emit(ctx context.Context, r log.Record) {
	if r.Severity() < log.SeverityWarn {
		return
	}
	body := r.Body().AsString()
	switch body {
	case "synchronizer operation failed", "synchronizer operation slow", "synchronizer work dropped":
	default:
		return
	}
	// Rebuild the record; retain only one normalized operation attribute.
	var safe log.Record
	safe.SetTimestamp(r.Timestamp())
	safe.SetSeverity(r.Severity())
	safe.SetBody(attribute.StringValue(body))
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if kv.Key == "operation" {
			safe.AddAttributes(attribute.String("operation", operation(kv.Value.AsString())))
			return false
		}
		return true
	})
	p := l.provider
	p.mu.Lock()
	now := p.now()
	allowed := now.Sub(p.last[body]) >= time.Minute
	if allowed {
		p.last[body] = now
	}
	p.mu.Unlock()
	if allowed {
		l.delegate.Emit(ctx, safe)
	} else {
		suppressedLogs.Add(1)
	}
}
