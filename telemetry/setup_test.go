package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kubescape/go-logger/otelsetup"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
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
		apiURL := "http://127.0.0.1:1"
		if mode == "missing" {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"version":"v3","response":{}}`)) }))
			defer srv.Close()
			apiURL = srv.URL
		}
		stop := Init(context.Background(), otelsetup.ProviderConfig{ServiceName: "synchronizer-client"}, apiURL)
		original := otel.GetMeterProvider()
		_ = Init(context.Background(), otelsetup.ProviderConfig{ServiceName: "must-not-reinitialize"}, apiURL)
		require.Equal(t, original, otel.GetMeterProvider())
		done := Default().Begin(context.Background(), "put")
		done(nil, 10)
		stop()
		stop() // idempotent shutdown
		return
	}
	for _, mode := range []string{"disable", "empty", "missing", "failure", "metrics", "traces", "logs"} {
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
		})
	}
}
