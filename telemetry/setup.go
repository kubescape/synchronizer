// Package telemetry owns synchronizer operational instrumentation. Providers and
// transport are supplied by go-logger/otelsetup; sandbox events are independent.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/otelsetup"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

var once sync.Once
var shutdownOnce sync.Once
var stop func(context.Context) error
var exportErrors atomic.Int64
var suppressedLogs atomic.Int64

// Init must precede logger and instrument construction. All telemetry failures
// are nonfatal; errors are deliberately fixed text to avoid leaking endpoints.
func Init(ctx context.Context, cfg otelsetup.ProviderConfig, apiURL string) func() {
	once.Do(func() {
		if apiURL == "" {
			apiURL = "api.armosec.io"
		}
		if err := ConfigureEndpoint(ctx, apiURL, LoadServices); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		// The shared compatibility alias uses Getenv, not LookupEnv. An explicitly
		// empty standard endpoint must still win over the legacy variable.
		if value, present := os.LookupEnv(EndpointEnv); present && value == "" {
			_ = os.Unsetenv("OTEL_COLLECTOR_SVC")
		}
		defaults()
		var errorMu sync.Mutex
		var lastError time.Time
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
			exportErrors.Add(1)
			errorMu.Lock()
			defer errorMu.Unlock()
			if time.Since(lastError) >= time.Minute {
				lastError = time.Now()
				fmt.Fprintln(os.Stderr, "operational telemetry export failed; records may be dropped")
			}
		}))
		var err error
		// Shared setup leaves unconfigured signals untouched. Install concrete
		// no-ops first: wrapping the API's delegating default logger provider would
		// otherwise cause it to delegate recursively back into our facade.
		global.SetLoggerProvider(lognoop.NewLoggerProvider())
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		stop, err = initProviders(ctx, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "operational telemetry initialization failed; continuing")
			otel.SetTracerProvider(tracenoop.NewTracerProvider())
			otel.SetMeterProvider(metricnoop.NewMeterProvider())
			global.SetLoggerProvider(lognoop.NewLoggerProvider())
		}
		// Gate before records reach either batch processor or shared ring buffer.
		global.SetLoggerProvider(newFilteredProvider(global.GetLoggerProvider()))
		logger.InitLogger("") // preserve KS_LOGGER_NAME/LEVEL and existing stdout format
	})
	return func() {
		shutdownOnce.Do(func() {
			if stop != nil {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := stop(shutdownCtx); err != nil {
					fmt.Fprintln(os.Stderr, "operational telemetry shutdown incomplete")
				}
			}
		})
	}
}

func defaults() {
	for key, value := range map[string]string{
		"OTEL_METRIC_EXPORT_INTERVAL": "60000", "OTEL_METRIC_EXPORT_TIMEOUT": "3000",
		"OTEL_EXPORTER_OTLP_TIMEOUT": "3000", "OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
		"OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.1",
		"OTEL_BSP_MAX_QUEUE_SIZE": "128", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "32", "OTEL_BSP_EXPORT_TIMEOUT": "3000",
		"OTEL_BLRP_MAX_QUEUE_SIZE": "128", "OTEL_BLRP_MAX_EXPORT_BATCH_SIZE": "32", "OTEL_BLRP_EXPORT_TIMEOUT": "3000",
		"OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT": "16", "OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT": "128",
	} {
		if _, present := os.LookupEnv(key); !present {
			_ = os.Setenv(key, value)
		}
	}
}

// v0.0.35's debug listener flushes its ring directly to its SDK logger, bypassing
// our export gate. Suppress that listener while preserving the local log level.
// Remove this startup-only env adaptation when shared setup accepts processors.
func initProviders(ctx context.Context, cfg otelsetup.ProviderConfig) (func(context.Context) error, error) {
	if os.Getenv(logger.EnvLoggerLevel) == "debug" {
		_ = os.Setenv(logger.EnvLoggerLevel, "info")
		defer func() { _ = os.Setenv(logger.EnvLoggerLevel, "debug") }()
	}
	return otelsetup.InitProviders(ctx, cfg)
}
