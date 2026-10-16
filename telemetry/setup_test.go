package telemetry

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/otelsetup"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	logs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	traces "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// Provider APIs start with delegating globals. Exercise public startup in fresh
// processes so previously installed SDK providers cannot mask recursion or
// disabled-signal bugs. Every network destination here is localhost.
func TestInitInFreshProcess(t *testing.T) {
	if mode := os.Getenv("SYNCHRONIZER_TELEMETRY_TEST_MODE"); mode != "" {
		t.Setenv("KS_LOGGER_NAME", "slog")
		t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "20")
		t.Setenv("OTEL_METRIC_EXPORT_TIMEOUT", "20")
		switch mode {
		case "disable":
			t.Setenv(EndpointEnv, "disable")
			t.Setenv("OTEL_COLLECTOR_SVC", "127.0.0.1:1")
			for _, signal := range []string{"TRACES", "LOGS", "METRICS"} {
				t.Setenv("OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT", "http://127.0.0.1:1")
			}
		case "empty":
			t.Setenv(EndpointEnv, "")
			t.Setenv("OTEL_COLLECTOR_SVC", "127.0.0.1:1")
		case "metrics", "traces", "logs":
			t.Setenv(EndpointEnv, "")
			t.Setenv("OTEL_EXPORTER_OTLP_"+strings.ToUpper(mode)+"_ENDPOINT", "http://127.0.0.1:1")
		}
		var counts *exportCounts
		if mode == "collector" {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			counts = &exportCounts{wire: map[string]int{}, protobuf: map[string]int{}}
			srv := grpc.NewServer()
			metrics.RegisterMetricsServiceServer(srv, metricSink{c: counts})
			traces.RegisterTraceServiceServer(srv, traceSink{c: counts})
			logs.RegisterLogsServiceServer(srv, logSink{c: counts})
			go func() { _ = srv.Serve(listener) }()
			defer srv.Stop()
			t.Setenv(EndpointEnv, "http://"+listener.Addr().String())
			t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
			t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "1000")
			t.Setenv("OTEL_METRIC_EXPORT_TIMEOUT", "1000")
		}
		apiURL := "http://127.0.0.1:1"
		if mode == "collector" || mode == "oss" || mode == "metrics" || mode == "traces" || mode == "logs" {
			apiURL = ""
		}
		if mode == "oss" {
			http.DefaultTransport = rejectDiscoveryTransport{t: t}
		}
		if mode == "missing" {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"version":"v3","response":{}}`)) }))
			defer srv.Close()
			apiURL = srv.URL
		}
		stop := Init(context.Background(), otelsetup.ProviderConfig{ServiceName: "synchronizer-client"}, apiURL)
		filtered, ok := global.GetLoggerProvider().(*filteredProvider)
		require.True(t, ok)
		_, logNoop := filtered.delegate.(lognoop.LoggerProvider)
		_, metricNoop := otel.GetMeterProvider().(metricnoop.MeterProvider)
		_, traceNoop := otel.GetTracerProvider().(tracenoop.TracerProvider)
		if mode == "oss" || mode == "disable" || mode == "empty" || mode == "missing" || mode == "failure" {
			require.True(t, metricNoop)
			require.True(t, traceNoop)
			require.True(t, logNoop)
		}
		if mode == "metrics" {
			require.True(t, logNoop)
			require.False(t, metricNoop)
			require.True(t, traceNoop)
		}
		if mode == "traces" {
			require.True(t, logNoop)
			require.True(t, metricNoop)
			require.False(t, traceNoop)
		}
		if mode == "logs" {
			require.False(t, logNoop)
			require.True(t, metricNoop)
			require.True(t, traceNoop)
		}
		logger.L().Info("console logging preserved")
		original := otel.GetMeterProvider()
		_ = Init(context.Background(), otelsetup.ProviderConfig{ServiceName: "must-not-reinitialize"}, apiURL)
		require.Equal(t, original, otel.GetMeterProvider())
		done := Default().Begin(context.Background(), "put")
		done(nil, 10)
		if mode == "collector" {
			_, finish := Default().Control(context.Background(), "connect")
			finish(errors.New("validation failure"))
		}
		stop()
		if counts != nil {
			counts.mu.Lock()
			defer counts.mu.Unlock()
			require.False(t, counts.authPresent, "unauthenticated collectors must receive no credential headers")
			require.Positive(t, counts.series)
			require.Positive(t, counts.spans)
			require.Positive(t, counts.logs)
		}
		stop() // idempotent shutdown
		return
	}
	for _, mode := range []string{"collector", "oss", "disable", "empty", "missing", "failure", "metrics", "traces", "logs"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInitInFreshProcess$")
			for _, env := range os.Environ() {
				if !strings.HasPrefix(env, "OTEL_") && !strings.HasPrefix(env, "KS_LOGGER_") && !strings.HasPrefix(env, "SYNCHRONIZER_TELEMETRY_TEST_MODE=") {
					cmd.Env = append(cmd.Env, env)
				}
			}
			cmd.Env = append(cmd.Env, "SYNCHRONIZER_TELEMETRY_TEST_MODE="+mode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.Contains(t, string(output), "console logging preserved")
			require.NotContains(t, strings.ToLower(string(output)), "credential")
			if mode == "oss" {
				require.NotContains(t, string(output), "operational telemetry")
			}
		})
	}
}

// Reject unexpected HTTP discovery in the unconfigured startup subprocess.
type rejectDiscoveryTransport struct{ t *testing.T }

func (r rejectDiscoveryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	r.t.Error("unconfigured telemetry attempted HTTP discovery")
	return nil, errors.New("discovery prohibited in OSS test")
}
