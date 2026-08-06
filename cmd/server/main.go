package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/kubescape/synchronizer/utils"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/backend/v1"
	"github.com/kubescape/synchronizer/cmd/server/authentication"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/messaging"
)

// shutdownTimeout bounds how long the server waits for in-flight requests to
// finish before the process exits and deferred cleanup runs.
const shutdownTimeout = 30 * time.Second

// readHeaderTimeout bounds how long a client may take to send request headers.
// Kept small and header-only so it does not affect established websocket connections.
const readHeaderTimeout = 10 * time.Second

// notifyShutdownSignals registers the receiver for the signals that trigger graceful
// shutdown. It is registered up front and only stopped when main returns, so no window is
// left where a signal has no receiver and the default disposition kills the process before
// the cleanup runs. signal.Notify delivers to every registered channel, so this coexists
// with the initialization-scoped context below and outlives that context unregistering itself.
func notifyShutdownSignals() (<-chan os.Signal, func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	return signals, func() { signal.Stop(signals) }
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		logger.L().Fatal("unable to load configuration", helpers.Error(err))
	}

	// enable prometheus metrics
	if cfg.Backend.Prometheus != nil && cfg.Backend.Prometheus.Enabled {
		go func() {
			logger.L().Info("prometheus metrics enabled", helpers.Int("port", cfg.Backend.Prometheus.Port))
			http.Handle("/metrics", promhttp.Handler())
			_ = http.ListenAndServe(fmt.Sprintf(":%d", cfg.Backend.Prometheus.Port), nil)
		}()
	}

	// start pprof server
	utils.ServePprof()

	// start the liveness probe before connecting to the message queue, so a slow broker shows
	// up as a startup failure rather than as kubelet killing a pod with nothing on the port
	utils.StartLivenessProbe()

	signals, stopSignals := notifyShutdownSignals()
	defer stopSignals()

	// connecting retries for a while, so let a shutdown signal abort it. Scoped to
	// initialization: the running server has its own shutdown path below.
	initCtx, stopInit := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

	var adapter adapters.Adapter
	mq, err := messaging.NewFromConfig(initCtx, cfg)
	// read before stopInit, which cancels initCtx itself and would make this always true
	interrupted := initCtx.Err() != nil
	stopInit()
	// Leave whether or not initialization went on to succeed, closing the queue on the way,
	// rather than starting to serve only to shut back down on the buffered signal.
	if interrupted {
		if err != nil {
			logger.L().Info("shutting down before the message queue was initialized", helpers.Error(err))
		} else {
			logger.L().Info("shutting down before the server started serving")
		}
		mq.Shutdown()
		return
	}
	if err != nil {
		logger.L().Fatal("failed to initialize message queue", helpers.Error(err))
	}
	if mq != nil {
		defer mq.Shutdown()
		adapter = backend.NewBackendAdapter(ctx, mq.Producer, cfg.Backend)
		mq.Reader.Start(ctx, adapter)
	} else {
		logger.L().Info("initializing mock adapter")
		adapter = adapters.NewMockAdapter(false)
	}

	var addr string
	if cfg.Backend.Port > 0 {
		addr = fmt.Sprintf(":%d", cfg.Backend.Port)
	} else {
		addr = ":8080"
	}

	hostname, _ := os.Hostname()
	logger.L().Info("starting synchronizer server", helpers.String("port", addr), helpers.String("hostname", hostname))

	// websocket server
	srv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: readHeaderTimeout,
		Handler: authentication.AuthenticationServerMiddleware(cfg.Backend.AuthenticationServer,
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
			})),
	}

	// Shut down on SIGTERM/SIGINT so the deferred cleanup runs, in particular closing
	// the message queue, which flushes buffered producer records instead of dropping them.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		// signals is buffered, so one that arrived while initialization was handing over
		// is still waiting here rather than lost
		sig := <-signals
		logger.L().Info("shutting down synchronizer server", helpers.String("signal", sig.String()))
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.L().Error("error during server shutdown", helpers.Error(err))
		}
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.L().Error("websocket server stopped unexpectedly", helpers.Error(err))
		return
	}

	<-shutdownDone
}
