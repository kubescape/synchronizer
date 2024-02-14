package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gobwas/ws"
	backendUtils "github.com/kubescape/backend/pkg/utils"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
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

	ctx = context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: cfg.InCluster.Account,
		Cluster: cfg.InCluster.ClusterName,
	})

	// k8s client
	k8sclient, err := utils.NewClient()
	if err != nil {
		logger.L().Fatal("unable to create k8s client", helpers.Error(err))
	}
	// in-cluster adapter
	adapter := incluster.NewInClusterAdapter(cfg.InCluster, k8sclient)

	// authentication headers
	version := os.Getenv("RELEASE")
	dialer := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(map[string][]string{
			core.AccessKeyHeader:   {cfg.InCluster.AccessKey},
			core.AccountHeader:     {cfg.InCluster.Account},
			core.ClusterNameHeader: {cfg.InCluster.ClusterName},
			core.HelmVersionHeader: {os.Getenv("HELM_RELEASE")},
			core.VersionHeader:     {version},
		}),
		NetDial: utils.GetDialer(),
	}

	// start pprof server
	utils.ServePprof()

	// start liveness probe
	utils.StartLivenessProbe()

	// websocket client
	newConn := func() (net.Conn, error) {
		var conn net.Conn
		if err := backoff.RetryNotify(func() error {
			conn, _, _, err = dialer.Dial(ctx, cfg.InCluster.ServerUrl)
			var status ws.StatusError
			if errors.As(err, &status) && status == http.StatusFailedDependency {
				return backoff.Permanent(fmt.Errorf("server rejected our client version <%s>, please update", version))
			}
			return err
		}, utils.NewBackOff(), func(err error, d time.Duration) {
			logger.L().Ctx(ctx).Warning("connection error", helpers.Error(err),
				helpers.String("retry in", d.String()))
		}); err != nil {
			return nil, fmt.Errorf("unable to create websocket connection: %w", err)
		}
		return conn, nil
	}

	conn, err := newConn()
	if err != nil {
		logger.L().Ctx(ctx).Fatal("failed to connect", helpers.Error(err))
	}

	// synchronizer
	synchronizer, err := core.NewSynchronizerClient(ctx, adapter, conn, newConn)
	if err != nil {
		logger.L().Ctx(ctx).Fatal("failed to create synchronizer", helpers.Error(err))
	}
	err = synchronizer.Start(ctx)
	if err != nil {
		logger.L().Ctx(ctx).Fatal("error during sync, exiting", helpers.Error(err))
	}
}
