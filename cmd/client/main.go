package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/gobwas/ws"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
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

	// authentication headers
	dialer := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(map[string][]string{
			core.AccessKeyHeader:   {cfg.InCluster.AccessKey},
			core.AccountHeader:     {cfg.InCluster.Account},
			core.ClusterNameHeader: {cfg.InCluster.ClusterName},
		}),
	}

	for {
		if err := start(ctx, cfg, adapter, dialer); err != nil {
			d := 5 * time.Second // TODO: use exponential backoff for retries
			logger.L().Error("connection error", helpers.Error(err), helpers.String("retry in", d.String()))
			time.Sleep(d)
		} else {
			break
		}
	}
	logger.L().Info("exiting")
}

func start(ctx context.Context, cfg config.Config, adapter adapters.Adapter, dialer ws.Dialer) error {
	// websocket client
	conn, _, _, err := dialer.Dial(ctx, cfg.InCluster.BackendUrl)
	if err != nil {
		return fmt.Errorf("unable to create websocket connection: %w", err)
	}
	defer conn.Close()

	ctx = context.WithValue(ctx, domain.ContextKeyAccessKey, cfg.InCluster.AccessKey)     //nolint
	ctx = context.WithValue(ctx, domain.ContextKeyAccount, cfg.InCluster.Account)         //nolint
	ctx = context.WithValue(ctx, domain.ContextKeyClusterName, cfg.InCluster.ClusterName) //nolint

	// synchronizer
	synchronizer := core.NewSynchronizerClient(ctx, adapter, conn)
	err = synchronizer.Start(ctx)
	if err != nil {
		return fmt.Errorf("error during sync: %w", err)
	}
	return nil
}
