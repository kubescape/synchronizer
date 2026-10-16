package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/kubescape/backend/pkg/servicediscovery/schema"
	v3 "github.com/kubescape/backend/pkg/servicediscovery/v3"
	"github.com/stretchr/testify/require"
)

func unset(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	_ = os.Unsetenv(key)
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}
func TestEndpointPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, value, discovered, want string
		present, call                 bool
	}{
		{name: "unset", discovered: "collector:443", want: "https://collector:443", call: true},
		{name: "explicit", value: "https://explicit", want: "https://explicit", present: true},
		{name: "empty", present: true},
		{name: "disabled", value: "DiSaBlE", present: true},
		{name: "http", discovered: "http://collector:4317", want: "http://collector:4317", call: true},
		{name: "https", discovered: "https://collector", want: "https://collector", call: true},
		{name: "missing", call: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unset(t, EndpointEnv)
			unset(t, "OTEL_COLLECTOR_SVC")
			if tc.present {
				t.Setenv(EndpointEnv, tc.value)
			}
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces")
			t.Setenv("OTEL_EVENTS_EXPORTER_ENDPOINT", "https://sandbox")
			called := false
			err := ConfigureEndpoint(context.Background(), "test-api", func(_ context.Context, base string) (schema.IBackendServices, error) {
				called = true
				require.Equal(t, "test-api", base)
				return &v3.ServicesV3{MetricsUrl: tc.discovered}, nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.call, called)
			require.Equal(t, tc.want, os.Getenv(EndpointEnv))
			require.Equal(t, "https://sandbox", os.Getenv("OTEL_EVENTS_EXPORTER_ENDPOINT"))
			if tc.name != "disabled" {
				require.Equal(t, "https://traces", os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
			}
		})
	}
}
func TestDiscoveryFailureAndLegacy(t *testing.T) {
	unset(t, EndpointEnv)
	unset(t, "OTEL_COLLECTOR_SVC")
	require.Error(t, ConfigureEndpoint(context.Background(), "test", func(context.Context, string) (schema.IBackendServices, error) { return nil, errors.New("secret error") }))
	_, present := os.LookupEnv(EndpointEnv)
	require.False(t, present)
	t.Setenv("OTEL_COLLECTOR_SVC", "collector:4317")
	require.NoError(t, ConfigureEndpoint(context.Background(), "test", func(context.Context, string) (schema.IBackendServices, error) {
		t.Fatal("legacy should skip discovery")
		return nil, nil
	}))
	require.Equal(t, "collector:4317", os.Getenv(EndpointEnv))
	t.Setenv(EndpointEnv, "disable")
	require.NoError(t, ConfigureEndpoint(context.Background(), "test", nil))
	require.Empty(t, os.Getenv("OTEL_COLLECTOR_SVC"))
}
func TestLoadServices(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		ok         bool
	}{
		{"valid", `{"version":"v3","response":{"metrics":"collector:443","synchronizer":"wss://sync"}}`, 200, true},
		{"missing", `{"version":"v3","response":{}}`, 200, true},
		{"bad-json", `secret`, 200, false},
		{"bad-version", `{"version":"unknown","response":{}}`, 200, false},
		{"status", `secret`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v3/servicediscovery", r.URL.Path)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			services, err := LoadServices(context.Background(), srv.URL)
			if tc.ok {
				require.NoError(t, err)
				require.NotNil(t, services)
			} else {
				require.Error(t, err)
			}
		})
	}
}
func TestDiscoveryDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := LoadServices(ctx, srv.URL)
	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second)
	_, err = LoadServices(context.Background(), "://bad")
	require.Error(t, err)
}
func TestDefaultsRespectOverrides(t *testing.T) {
	for key := range map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "", "OTEL_TRACES_SAMPLER": "", "OTEL_TRACES_SAMPLER_ARG": "", "OTEL_EXPORTER_OTLP_COMPRESSION": "", "OTEL_BSP_MAX_QUEUE_SIZE": "", "OTEL_BLRP_MAX_QUEUE_SIZE": "", "OTEL_METRIC_EXPORT_TIMEOUT": "", "OTEL_EXPORTER_OTLP_TIMEOUT": "", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "", "OTEL_BSP_EXPORT_TIMEOUT": "", "OTEL_BLRP_MAX_EXPORT_BATCH_SIZE": "", "OTEL_BLRP_EXPORT_TIMEOUT": "", "OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT": "", "OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT": ""} {
		unset(t, key)
	}
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "300000")
	defaults()
	require.Equal(t, "300000", os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"))
	require.Equal(t, "parentbased_traceidratio", os.Getenv("OTEL_TRACES_SAMPLER"))
	require.Equal(t, "gzip", os.Getenv("OTEL_EXPORTER_OTLP_COMPRESSION"))
}
