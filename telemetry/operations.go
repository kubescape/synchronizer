package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// A fixed operation vocabulary bounds cardinality independently of customers,
// Kubernetes kinds, event properties and connection count.
func operation(s string) string {
	switch s {
	case "http_ingest", "connect", "receive", "send", "dispatch", "batch", "get", "verify", "delete", "patch", "put", "mq_produce", "mq_receive":
		return s
	default:
		return "other"
	}
}

type Instruments struct {
	connections metric.Int64UpDownCounter
	calls       metric.Int64Counter
	bytes       metric.Int64Counter
	duration    metric.Float64Histogram
	active      metric.Int64UpDownCounter
	drops       metric.Int64Counter
	pending     metric.Int64UpDownCounter
	stalled     metric.Int64UpDownCounter
	suppressed  metric.Int64Counter
	tracer      trace.Tracer
	logs        log.Logger
	mu          sync.Mutex
	lastSpan    time.Time
	now         func() time.Time
	stallAfter  time.Duration
}

func New() *Instruments {
	m := otel.Meter(scope)
	i := &Instruments{tracer: otel.Tracer(scope), logs: global.GetLoggerProvider().Logger(scope), now: time.Now, stallAfter: 30 * time.Second}
	_, _ = m.Int64ObservableCounter("synchronizer.telemetry.errors", metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error { o.Observe(exportErrors.Load()); return nil }))
	_, _ = m.Int64ObservableCounter("synchronizer.logs.suppressed", metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error { o.Observe(suppressedLogs.Load()); return nil }))
	i.connections, _ = m.Int64UpDownCounter("synchronizer.connections.active")
	i.calls, _ = m.Int64Counter("synchronizer.operations", metric.WithDescription("Completed operations by bounded operation and outcome"))
	i.bytes, _ = m.Int64Counter("synchronizer.transfer.bytes", metric.WithUnit("By"))
	i.duration, _ = m.Float64Histogram("synchronizer.operation.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(.001, .01, .1, 1, 5, 30))
	i.active, _ = m.Int64UpDownCounter("synchronizer.operations.active")
	i.pending, _ = m.Int64UpDownCounter("synchronizer.dispatch.pending", metric.WithDescription("Callers awaiting the unbuffered writer"))
	i.stalled, _ = m.Int64UpDownCounter("synchronizer.operations.stalled", metric.WithDescription("Operations in progress for at least 30 seconds"))
	i.drops, _ = m.Int64Counter("synchronizer.drops")
	i.suppressed, _ = m.Int64Counter("synchronizer.spans.suppressed")
	return i
}

// Begin aggregates ordinary work; only completed operations taking >=1s create
// a retrospective slow span, with an additional process-wide 10s creation gate.
// A timer exists only while work is active and is stopped on completion.
func (i *Instruments) Begin(ctx context.Context, op string) func(error, int) {
	op = operation(op)
	start := i.now()
	attrs := metric.WithAttributes(attribute.String("operation", op))
	i.active.Add(ctx, 1, attrs)
	var mu sync.Mutex
	finished, stalled := false, false
	timer := time.AfterFunc(i.stallAfter, func() {
		mu.Lock()
		defer mu.Unlock()
		if !finished {
			stalled = true
			i.stalled.Add(ctx, 1, attrs)
		}
	})
	if op == "dispatch" {
		i.pending.Add(ctx, 1)
	}
	return func(err error, size int) {
		timer.Stop()
		mu.Lock()
		finished = true
		if stalled {
			i.stalled.Add(ctx, -1, attrs)
		}
		mu.Unlock()
		i.active.Add(ctx, -1, attrs)
		if op == "dispatch" {
			i.pending.Add(ctx, -1)
		}
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		options := metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome))
		i.calls.Add(ctx, 1, options)
		if size > 0 {
			i.bytes.Add(ctx, int64(size), options)
		}
		elapsed := i.now().Sub(start)
		i.duration.Record(ctx, elapsed.Seconds(), attrs)
		if elapsed >= time.Second {
			if op != "connect" {
				i.Span(ctx, op, start, err)
			}
			i.diagnostic(ctx, "synchronizer operation slow", op)
		} else if err != nil {
			i.diagnostic(ctx, "synchronizer operation failed", op)
		}
	}
}

// Span is also used for connection establishment (never connection lifetime).
func (i *Instruments) Span(ctx context.Context, op string, start time.Time, err error) {
	if !i.allowSpan(ctx) {
		return
	}
	now := i.now()
	_, s := i.tracer.Start(ctx, "synchronizer."+operation(op), trace.WithTimestamp(start))
	if err != nil {
		s.SetStatus(codes.Error, "operation failed")
	}
	// Emit correlated diagnostics while the span is available, without raw errors.
	if err != nil {
		i.diagnostic(trace.ContextWithSpan(ctx, s), "synchronizer operation failed", op)
	}
	s.End(trace.WithTimestamp(now))
}
func (i *Instruments) Drop(ctx context.Context, reason string) {
	switch reason {
	case "invalid_message", "depth", "canceled", "send_failed":
	default:
		reason = "other"
	}
	i.drops.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	i.diagnostic(ctx, "synchronizer work dropped", "other")
}
func (i *Instruments) diagnostic(ctx context.Context, body, op string) {
	var r log.Record
	r.SetTimestamp(i.now())
	r.SetSeverity(log.SeverityWarn)
	r.SetBody(attribute.StringValue(body))
	r.AddAttributes(attribute.String("operation", operation(op)))
	i.logs.Emit(ctx, r)
}

var defaultOnce sync.Once
var defaultInstruments *Instruments

func Default() *Instruments {
	defaultOnce.Do(func() { defaultInstruments = New() })
	return defaultInstruments
}

// Control creates a bounded connection-establishment span and propagates its
// context during the handshake. Remote sampling never bypasses the local gate.
func (i *Instruments) Control(ctx context.Context, op string) (context.Context, func(error)) {
	finish := i.Begin(ctx, op)
	if !i.allowSpan(ctx) {
		return ctx, func(err error) { finish(err, 0) }
	}
	child, span := i.tracer.Start(ctx, "synchronizer."+operation(op))
	return child, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, "operation failed")
			i.diagnostic(child, "synchronizer operation failed", op)
		}
		span.End()
		finish(err, 0)
	}
}

func (i *Instruments) Connection(ctx context.Context, delta int64) { i.connections.Add(ctx, delta) }

func (i *Instruments) allowSpan(ctx context.Context) bool {
	i.mu.Lock()
	now := i.now()
	allowed := now.Sub(i.lastSpan) >= 10*time.Second
	if allowed {
		i.lastSpan = now
	}
	i.mu.Unlock()
	if !allowed {
		i.suppressed.Add(ctx, 1)
	}
	return allowed
}
