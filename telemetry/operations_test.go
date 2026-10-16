package telemetry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type capturedLogs struct {
	embedded.LoggerProvider
	logger capturedLogger
}

func (p *capturedLogs) Logger(string, ...log.LoggerOption) log.Logger { return &p.logger }

type capturedLogger struct {
	embedded.Logger
	records []log.Record
}

func (l *capturedLogger) Emit(_ context.Context, r log.Record)                { l.records = append(l.records, r) }
func (l *capturedLogger) Enabled(context.Context, log.EnabledParameters) bool { return true }

func TestLogExportFilter(t *testing.T) {
	ctx := context.Background()
	sink := &capturedLogs{}
	p := newFilteredProvider(sink)
	now := time.Now()
	p.now = func() time.Time { return now }
	var r log.Record
	r.SetSeverity(log.SeverityWarn)
	r.SetBody(attribute.StringValue("synchronizer operation failed"))
	r.AddAttributes(attribute.String("credential", "secret"), attribute.String("operation", "tenant-attacker"))
	p.Logger("slog").Emit(ctx, r)
	require.Empty(t, sink.logger.records)
	l := p.Logger(scope)
	l.Emit(ctx, r)
	l.Emit(ctx, r)
	require.Len(t, sink.logger.records, 1)
	safe := sink.logger.records[0]
	require.Equal(t, 1, safe.AttributesLen())
	safe.WalkAttributes(func(kv attribute.KeyValue) bool {
		require.Equal(t, "operation", string(kv.Key))
		require.Equal(t, "other", kv.Value.AsString())
		return true
	})
	now = now.Add(time.Minute)
	l.Emit(ctx, r)
	require.Len(t, sink.logger.records, 2)
	r.SetSeverity(log.SeverityInfo)
	l.Emit(ctx, r)
	require.Len(t, sink.logger.records, 2)
	r.SetSeverity(log.SeverityError)
	r.SetBody(attribute.StringValue("secret payload"))
	l.Emit(ctx, r)
	require.Len(t, sink.logger.records, 2)
	require.Len(t, p.last, 1)
}

func TestOperationsBoundedAndSlowGated(t *testing.T) {
	ctx := context.Background()
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	oldM, oldT, oldL := otel.GetMeterProvider(), otel.GetTracerProvider(), global.GetLoggerProvider()
	otel.SetMeterProvider(mp)
	otel.SetTracerProvider(tp)
	sink := &capturedLogs{}
	global.SetLoggerProvider(newFilteredProvider(sink))
	defer func() {
		otel.SetMeterProvider(oldM)
		otel.SetTracerProvider(oldT)
		global.SetLoggerProvider(oldL)
		_ = mp.Shutdown(ctx)
		_ = tp.Shutdown(ctx)
	}()
	i := New()
	now := time.Now()
	i.now = func() time.Time { return now }
	for n := 0; n < 1000; n++ {
		done := i.Begin(ctx, "unbounded-tenant-kind")
		done(nil, 100)
	}
	require.Empty(t, spans.GetSpans())
	for n := 0; n < 100; n++ {
		done := i.Begin(ctx, "put")
		now = now.Add(time.Second)
		done(errors.New("secret"), 10)
	}
	require.Len(t, spans.GetSpans(), 10)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))
	var count int64
	for _, sm := range data.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch points := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range points.DataPoints {
					for _, a := range dp.Attributes.ToSlice() {
						require.Contains(t, []string{"operation", "outcome", "reason"}, string(a.Key))
						require.NotContains(t, a.Value.AsString(), "tenant")
					}
					if m.Name == "synchronizer.operations" {
						count += dp.Value
					}
					if m.Name == "synchronizer.operations.active" || m.Name == "synchronizer.dispatch.pending" {
						require.Zero(t, dp.Value)
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range points.DataPoints {
					require.Equal(t, []float64{.001, .01, .1, 1, 5, 30}, dp.Bounds)
				}
			}
		}
	}
	require.EqualValues(t, 1100, count)
}

func TestControlPropagationAndGate(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer func() { otel.SetTracerProvider(old); _ = tp.Shutdown(context.Background()) }()
	i := New()
	now := time.Now()
	i.now = func() time.Time { return now }
	ctx, end := i.Control(context.Background(), "connect")
	require.True(t, ctx != nil)
	now = now.Add(20 * time.Second)
	end(nil)
	require.Len(t, spans.GetSpans(), 1)
	_, end = i.Control(ctx, "connect")
	end(nil)
	_, end = i.Control(ctx, "connect")
	end(nil)
	require.Len(t, spans.GetSpans(), 2)
}

func TestPendingAndStalledWork(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() { otel.SetMeterProvider(old); _ = mp.Shutdown(context.Background()) }()
	i := New()
	i.stallAfter = 5 * time.Millisecond
	done := i.Begin(context.Background(), "dispatch")
	value := func(name string) int64 {
		var data metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &data))
		for _, sm := range data.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == name {
					for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
						return p.Value
					}
				}
			}
		}
		return 0
	}
	require.EqualValues(t, 1, value("synchronizer.dispatch.pending"))
	require.Eventually(t, func() bool { return value("synchronizer.operations.stalled") == 1 }, time.Second, time.Millisecond)
	done(context.Canceled, 0)
	require.Zero(t, value("synchronizer.operations.stalled"))
	require.Zero(t, value("synchronizer.dispatch.pending"))
	require.Zero(t, value("synchronizer.operations.active"))
}
