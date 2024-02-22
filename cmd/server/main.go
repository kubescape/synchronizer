package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/gobwas/ws"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/cmd/server/authentication"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
)

func main() {
	ctx := context.Background()

	// load config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		logger.L().Fatal("unable to load configuration", helpers.Error(err))
	}

	// backend adapter
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// enable prometheus metrics
	if cfg.Backend.Prometheus != nil && cfg.Backend.Prometheus.Enabled {
		go func() {
			logger.L().Info("prometheus metrics enabled", helpers.Int("port", cfg.Backend.Prometheus.Port))
			http.Handle("/metrics", promhttp.Handler())
			_ = http.ListenAndServe(fmt.Sprintf(":%d", cfg.Backend.Prometheus.Port), nil)
		}()
	}

	var adapter adapters.Adapter
	if cfg.Backend.PulsarConfig != nil {
		logger.L().Info("initializing pulsar client")
		pulsarClient, err := pulsarconnector.NewClient(
			pulsarconnector.WithConfig(cfg.Backend.PulsarConfig),
		)
		if err != nil {
			logger.L().Fatal("failed to create pulsar client", helpers.Error(err), helpers.String("config", fmt.Sprintf("%+v", cfg.Backend.PulsarConfig)))
		}
		defer pulsarClient.Close()

		pulsarProducer, err := backend.NewPulsarMessageProducer(cfg, pulsarClient)
		if err != nil {
			logger.L().Fatal("failed to create pulsar producer", helpers.Error(err), helpers.String("config", fmt.Sprintf("%+v", cfg.Backend.PulsarConfig)))
		}

		pulsarReader, err := backend.NewPulsarMessageReader(cfg, pulsarClient)
		if err != nil {
			logger.L().Fatal("failed to create pulsar reader", helpers.Error(err), helpers.String("config", fmt.Sprintf("%+v", cfg.Backend.PulsarConfig)))
		}

		adapter = backend.NewBackendAdapter(ctx, pulsarProducer, cfg.Backend.ReconciliationTask)
		pulsarReader.Start(ctx, adapter)
	} else {
		// mock adapter
		logger.L().Info("initializing mock adapter")
		adapter = adapters.NewMockAdapter(false)
	}

	// start pprof server
	utils.ServePprof()

	// start liveness probe
	utils.StartLivenessProbe()

	var addr string
	if cfg.Backend.Port > 0 {
		addr = fmt.Sprintf(":%d", cfg.Backend.Port)
	} else {
		addr = ":8080"
	}

	hostname, _ := os.Hostname()
	logger.L().Info("starting synchronizer server", helpers.String("port", addr), helpers.String("hostname", hostname))

	// websocket server
	_ = http.ListenAndServe(addr,
		authentication.AuthenticationServerMiddleware(cfg.Backend.AuthenticationServer,
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, _, err := ws.UpgradeHTTP(r, w)
				if err != nil {
					logger.L().Error("unable to upgrade connection", helpers.Error(err))
					return
				}

				go func() {
					defer conn.Close()
					id := utils.ClientIdentifierFromContext(r.Context())
					synchronizer, err := core.NewSynchronizerServer(r.Context(), []adapters.Adapter{adapter}, conn)
					if err != nil {
						logger.L().Error("error during creating synchronizer server instance",
							helpers.String("account", id.Account),
							helpers.String("cluster", id.Cluster),
							helpers.String("connectionId", id.ConnectionId),
							helpers.Error(err))
						return
					}
					err = synchronizer.Start(r.Context())
					if err != nil {
						logger.L().Error("error during sync, closing listener",
							helpers.String("account", id.Account),
							helpers.String("cluster", id.Cluster),
							helpers.String("connectionId", id.ConnectionId),
							helpers.Error(err))
						err := synchronizer.Stop(r.Context())
						if err != nil {
							logger.L().Error("error during sync stop", helpers.Error(err))
						}
						return
					}
				}()
			})))
}
