package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/kubescape/backend/pkg/servicediscovery"
	"github.com/kubescape/backend/pkg/servicediscovery/schema"
	v3 "github.com/kubescape/backend/pkg/servicediscovery/v3"
)

const EndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"

// LoadServices reuses the v3 client URL/parser with a bounded transport. The
// shared client's Get uses http.Get without a deadline or response-body close.
func LoadServices(ctx context.Context, base string) (schema.IBackendServices, error) {
	client, err := v3.NewServiceDiscoveryClientV3(base)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.GetServiceDiscoveryUrl(), nil)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("discovery returned non-200 status")
	}
	return servicediscovery.GetServices(&boundedGetter{ServiceDiscoveryClientV3: client, reader: io.LimitReader(resp.Body, 1<<20)})
}

type boundedGetter struct {
	*v3.ServiceDiscoveryClientV3
	reader io.Reader
}

func (g *boundedGetter) Get() (io.Reader, error) { return g.reader, nil }

// ConfigureEndpoint intentionally distinguishes unset from explicitly empty.
// It never discovers or modifies the independent sandbox events endpoint.
func ConfigureEndpoint(ctx context.Context, base string, get func(context.Context, string) (schema.IBackendServices, error)) error {
	if value, present := os.LookupEnv(EndpointEnv); present {
		if strings.EqualFold(strings.TrimSpace(value), "disable") {
			// Keep an empty sentinel to defeat the shared setup's legacy alias.
			_ = os.Setenv(EndpointEnv, "")
			_ = os.Unsetenv("OTEL_COLLECTOR_SVC")
			for _, signal := range []string{"TRACES", "METRICS", "LOGS"} {
				_ = os.Unsetenv("OTEL_EXPORTER_OTLP_" + signal + "_ENDPOINT")
			}
		}
		return nil
	}
	if legacy, present := os.LookupEnv("OTEL_COLLECTOR_SVC"); present && legacy != "" {
		return os.Setenv(EndpointEnv, legacy)
	}
	services, err := get(ctx, base)
	if err != nil {
		return errors.New("operational telemetry discovery unavailable")
	}
	if services == nil || services.GetMetricsUrl() == "" {
		return nil
	}
	endpoint := services.GetMetricsUrl()
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	return os.Setenv(EndpointEnv, endpoint)
}
