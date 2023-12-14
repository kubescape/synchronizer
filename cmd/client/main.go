package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/gobwas/ws"
	backendUtils "github.com/kubescape/backend/pkg/utils"
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

	// load service config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		logger.L().Fatal("load config error", helpers.Error(err))
	}

	// load common ClusterConfig
	if clusterConfig, err := config.LoadClusterConfig(); err != nil {
		logger.L().Warning("failed to load cluster config", helpers.Error(err))
	} else {
		logger.L().Debug("cluster config loaded", helpers.String("clusterName", clusterConfig.ClusterName))
		cfg.InCluster.ClusterName = clusterConfig.ClusterName
	}

	// load credentials (access key & account)
	if credentials, err := backendUtils.LoadCredentialsFromFile("/etc/credentials"); err != nil {
		logger.L().Warning("failed to load credentials", helpers.Error(err))
	} else {
		logger.L().Debug("credentials loaded",
			helpers.Int("accessKeyLength", len(credentials.AccessKey)),
			helpers.Int("accountLength", len(credentials.Account)))
		cfg.InCluster.AccessKey = credentials.AccessKey
		cfg.InCluster.Account = credentials.Account
	}

	// synchronizer server URL using service discovery
	if services, err := config.LoadServiceURLs("/etc/config/services.json"); err != nil {
		logger.L().Warning("failed discovering urls", helpers.Error(err))
	} else if serverUrl := services.GetSynchronizerUrl(); serverUrl != "" {
		logger.L().Debug("synchronizer server URL", helpers.String("synchronizer", serverUrl))
		cfg.InCluster.ServerUrl = serverUrl
	}

	cfg.InCluster.ValidateConfig()

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
	adapter := incluster.NewInClusterAdapter(cfg.InCluster, k8sclient)

	// authentication headers
	dialer := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(map[string][]string{
			core.AccessKeyHeader:   {cfg.InCluster.AccessKey},
			core.AccountHeader:     {cfg.InCluster.Account},
			core.ClusterNameHeader: {cfg.InCluster.ClusterName},
		}),
	}

	// start liveness probe
	utils.StartLivenessProbe()

	for {
		if err := start(ctx, cfg.InCluster, adapter, dialer); err != nil {
			d := 5 * time.Second // TODO: use exponential backoff for retries
			logger.L().Ctx(ctx).Error("connection error", helpers.Error(err), helpers.String("retry in", d.String()))
			time.Sleep(d)
		} else {
			break
		}
	}
	logger.L().Info("exiting")
}

func start(ctx context.Context, cfg config.InCluster, adapter adapters.Adapter, dialer ws.Dialer) error {
	// websocket client
	conn, _, _, err := dialer.Dial(ctx, cfg.ServerUrl)
	if err != nil {
		return fmt.Errorf("unable to create websocket connection: %w", err)
	}
	defer conn.Close()

	ctx = context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: cfg.Account,
		Cluster: cfg.ClusterName,
	})

	// synchronizer
	synchronizer := core.NewSynchronizerClient(ctx, adapter, conn)
	err = synchronizer.Start(ctx)
	if err != nil {
		return fmt.Errorf("error during sync: %w", err)
	}
	return nil
}
