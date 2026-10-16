package telemetry

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kubescape/go-logger/otelsetup"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	traces "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
)

type exportCounts struct {
	leaked              bool
	mu                  sync.Mutex
	wire, protobuf      map[string]int
	series, spans, logs int
	authenticated       bool
	correlated          bool
}

func (c *exportCounts) add(ctx context.Context, signal string, m proto.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.protobuf[signal] += proto.Size(m)
	encoded, _ := proto.Marshal(m)
	if bytes.Contains(encoded, []byte("secret-must-not-export")) {
		c.leaked = true
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("x-api-key")) != 1 || md.Get("x-api-key")[0] != "test-key" || len(md.Get("x-api-account")) != 1 || md.Get("x-api-account")[0] != "test-account" {
		c.authenticated = false
	}
}
func (c *exportCounts) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, methodKey{}, info.FullMethodName)
}
func (c *exportCounts) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if p, ok := s.(*stats.InPayload); ok {
		c.mu.Lock()
		c.wire[ctx.Value(methodKey{}).(string)] += p.WireLength
		c.mu.Unlock()
	}
}
func (c *exportCounts) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (c *exportCounts) HandleConn(context.Context, stats.ConnStats)                       {}

type methodKey struct{}
type metricSink struct {
	metrics.UnimplementedMetricsServiceServer
	c *exportCounts
}

func (s metricSink) Export(ctx context.Context, r *metrics.ExportMetricsServiceRequest) (*metrics.ExportMetricsServiceResponse, error) {
	s.c.add(ctx, "metrics", r)
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	for _, rm := range r.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				s.c.series += len(m.GetSum().GetDataPoints()) + len(m.GetHistogram().GetDataPoints()) + len(m.GetGauge().GetDataPoints())
			}
		}
	}
	return &metrics.ExportMetricsServiceResponse{}, nil
}

type traceSink struct {
	traces.UnimplementedTraceServiceServer
	c *exportCounts
}

func (s traceSink) Export(ctx context.Context, r *traces.ExportTraceServiceRequest) (*traces.ExportTraceServiceResponse, error) {
	s.c.add(ctx, "traces", r)
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	for _, rs := range r.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			s.c.spans += len(ss.Spans)
		}
	}
	return &traces.ExportTraceServiceResponse{}, nil
}

type logSink struct {
	logs.UnimplementedLogsServiceServer
	c *exportCounts
}

func (s logSink) Export(ctx context.Context, r *logs.ExportLogsServiceRequest) (*logs.ExportLogsServiceResponse, error) {
	s.c.add(ctx, "logs", r)
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	for _, rl := range r.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, l := range sl.LogRecords {
				s.c.logs++
				if len(l.TraceId) == 16 && len(l.SpanId) == 8 {
					s.c.correlated = true
				}
			}
		}
	}
	return &logs.ExportLogsServiceResponse{}, nil
}

// This is a reproducible synthetic measurement of real shared-provider gRPC
// exports, not a production traffic replay or a validation of an hourly budget.
func TestLocalOTLPVolume(t *testing.T) {
	for _, tc := range []struct {
		name       string
		operations int
	}{{"normal", 100}, {"burst", 10000}} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			c := &exportCounts{wire: map[string]int{}, protobuf: map[string]int{}, authenticated: true}
			srv := grpc.NewServer(grpc.StatsHandler(c))
			metrics.RegisterMetricsServiceServer(srv, metricSink{c: c})
			traces.RegisterTraceServiceServer(srv, traceSink{c: c})
			logs.RegisterLogsServiceServer(srv, logSink{c: c})
			go func() { _ = srv.Serve(listener) }()
			defer srv.Stop()
			t.Setenv(EndpointEnv, "http://"+listener.Addr().String())
			t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
			t.Setenv("KS_LOGGER_LEVEL", "info")
			t.Setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
			t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
			for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
				unset(t, "OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT")
				unset(t, "OTEL_EXPORTER_OTLP_"+signal+"_COMPRESSION")
			}
			defaults()
			oldM, oldT, oldL := otel.GetMeterProvider(), otel.GetTracerProvider(), global.GetLoggerProvider()
			defer func() { otel.SetMeterProvider(oldM); otel.SetTracerProvider(oldT); global.SetLoggerProvider(oldL) }()
			stop, err := initProviders(context.Background(), otelsetup.ProviderConfig{ServiceName: "synchronizer-client", ServiceVersion: "test", AccessKey: "test-key", AccountID: "test-account"})
			require.NoError(t, err)
			global.SetLoggerProvider(newFilteredProvider(global.GetLoggerProvider()))
			i := New()
			ctx, finish := i.Control(context.Background(), "connect")
			carrier := propagation.MapCarrier{}
			propagation.TraceContext{}.Inject(ctx, carrier)
			remote := propagation.TraceContext{}.Extract(context.Background(), carrier)
			require.Equal(t, trace.SpanContextFromContext(ctx).TraceID(), trace.SpanContextFromContext(remote).TraceID())
			finish(errors.New("secret-must-not-export"))
			for n := 0; n < tc.operations; n++ {
				done := i.Begin(ctx, "put")
				if n%10 == 0 {
					done(errors.New("secret-must-not-export"), 1024)
				} else {
					done(nil, 1024)
				}
			}
			require.NoError(t, stop(context.Background()))
			c.mu.Lock()
			defer c.mu.Unlock()
			require.False(t, c.leaked)
			require.True(t, c.authenticated)
			require.True(t, c.correlated)
			require.Equal(t, 1, c.spans)
			require.Equal(t, 1, c.logs)
			require.LessOrEqual(t, c.series, 12)
			require.NotEmpty(t, c.wire)
			require.NotEmpty(t, c.protobuf)
			t.Logf("operations=%d datapoints=%d spans=%d logs=%d protobuf_bytes=%v compressed_grpc_wire_bytes=%v; sampler=always_on for deterministic gate verification", tc.operations, c.series, c.spans, c.logs, c.protobuf, c.wire)
		})
	}
}

func TestBackendOutageDoesNotBlockProcessing(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := listener.Addr().String()
	require.NoError(t, listener.Close())
	t.Setenv(EndpointEnv, "http://"+endpoint)
	for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
		unset(t, "OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT")
	}
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	t.Setenv("KS_LOGGER_LEVEL", "debug")
	defaults()
	oldM, oldT, oldL := otel.GetMeterProvider(), otel.GetTracerProvider(), global.GetLoggerProvider()
	defer func() { otel.SetMeterProvider(oldM); otel.SetTracerProvider(oldT); global.SetLoggerProvider(oldL) }()
	stop, err := initProviders(context.Background(), otelsetup.ProviderConfig{ServiceName: "synchronizer-client"})
	require.NoError(t, err)
	require.Equal(t, "debug", os.Getenv("KS_LOGGER_LEVEL"))
	global.SetLoggerProvider(newFilteredProvider(global.GetLoggerProvider()))
	i := New()
	start := time.Now()
	for n := 0; n < 10000; n++ {
		done := i.Begin(context.Background(), "put")
		done(errors.New("failure"), 100)
	}
	require.Less(t, time.Since(start), 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	require.Error(t, stop(ctx))
	require.Less(t, time.Since(start), time.Second)
}
