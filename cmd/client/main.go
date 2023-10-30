package main

import (
	"context"
	"net/url"
	"os"

	"github.com/gobwas/ws"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/utils"
)

func main() {
	ctx := context.Background()

	err := logger.L().SetLevel(helpers.DebugLevel.String())
	if err != nil {
		logger.L().Fatal("unable to set log level", helpers.Error(err))
	}

	// load config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		logger.L().Fatal("unable to load configuration", helpers.Error(err))
	}

	// to enable otel, set OTEL_COLLECTOR_SVC=otel-collector:4317
	if otelHost, present := os.LookupEnv("OTEL_COLLECTOR_SVC"); present {
		ctx = logger.InitOtel("synchronizer",
			os.Getenv("RELEASE"),
			cfg.InCluster.Account,
			cfg.InCluster.ClusterName,
			url.URL{Host: otelHost})
		defer logger.ShutdownOtel(ctx)
	}

	// k8s client
	k8sclient, err := utils.NewClient()
	if err != nil {
		logger.L().Fatal("unable to create k8s client", helpers.Error(err))
	}
	// in-cluster adapter
	adapter := incluster.NewInClusterAdapter(cfg, k8sclient)
	// websocket client
	conn, _, _, err := ws.DefaultDialer.Dial(ctx, cfg.InCluster.BackendUrl)
	if err != nil {
		logger.L().Fatal("unable to create websocket connection", helpers.Error(err))
	}
	defer conn.Close()
	// synchronizer
	synchronizer := core.NewSynchronizerClient(adapter, conn)
	err = synchronizer.Start(ctx)
	if err != nil {
		logger.L().Fatal("error during sync", helpers.Error(err))
	}
}
