package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gobwas/ws"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/backend/v1"

	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
)

func main() {
	ctx := context.Background()

	if err := logger.L().SetLevel(helpers.DebugLevel.String()); err != nil {
		logger.L().Fatal("unable to set log level", helpers.Error(err))
	}

	// load config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		logger.L().Fatal("unable to load configuration", helpers.Error(err))
	}

	// backend adapter
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
		adapter = backend.NewBackendAdapter(cfg, pulsarClient)
	} else {
		// mock adapter
		logger.L().Info("initializing mock adapter")
		adapter = adapters.NewMockAdapter()
	}

	// websocket server
	_ = http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorizedCtx, isAuthorized := authorizedContext(ctx, r)
		if !isAuthorized {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			logger.L().Error("unable to upgrade connection", helpers.Error(err))
			return
		}
		go func() {
			defer conn.Close()
			synchronizer := core.NewSynchronizerServer(authorizedCtx, adapter, conn)
			err = synchronizer.Start(authorizedCtx)
			if err != nil {
				logger.L().Error("error during sync", helpers.Error(err))
				return
			}
		}()
	}))
}

// authorize checks if the request is authorized, and if so, returns an authorized context.
func authorizedContext(ctx context.Context, r *http.Request) (context.Context, bool) {
	accessKey := r.Header.Get(core.AccessKeyHeader)
	account := r.Header.Get(core.AccountHeader)
	cluster := r.Header.Get(core.ClusterNameHeader)

	if accessKey == "" || account == "" || cluster == "" {
		return ctx, false
	}

	// TODO: validate access key and account, maybe also cluster name

	// updates the context with the access key and account
	ctx = context.WithValue(ctx, domain.ContextKeyAccessKey, accessKey) //nolint
	ctx = context.WithValue(ctx, domain.ContextKeyAccount, account)     //nolint
	ctx = context.WithValue(ctx, domain.ContextKeyClusterName, cluster) //nolint
	return ctx, true
}
