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
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/httpendpoint/v1"
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
		logger.L().Debug("cluster config loaded",
			helpers.String("clusterName", clusterConfig.ClusterName),
			helpers.String("namespace", clusterConfig.Namespace))
		cfg.InCluster.ClusterName = clusterConfig.ClusterName
		cfg.InCluster.Namespace = clusterConfig.Namespace
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

	updateClusterName(&cfg)
	// init adapters
	var adpts []adapters.Adapter
	if err := cfg.InCluster.ValidateConfig(); err == nil {
		dynamicClient, storageClient, err := utils.NewClient()
		if err != nil {
			logger.L().Fatal("unable to create k8s client", helpers.Error(err))
		}
		inClusterAdapter := incluster.NewInClusterAdapter(cfg.InCluster, dynamicClient, storageClient)
		adpts = append(adpts, inClusterAdapter)
	}
	if err := cfg.HTTPEndpoint.ValidateConfig(); err == nil {
		httpEndpointAdapter := httpendpoint.NewHTTPEndpointAdapter(cfg)
		adpts = append(adpts, httpEndpointAdapter)
	}
	if len(adpts) == 0 {
		logger.L().Fatal("no valid adapter found")
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

	ctx = context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: cfg.InCluster.Account,
		Cluster: cfg.InCluster.ClusterName,
	})

	gitVersion, cloudProvider := incluster.GetApiServerGitVersionAndCloudProvider(ctx)

	// authentication headers
	version := os.Getenv("RELEASE")
	dialer := ws.Dialer{
		Header: ws.HandshakeHeaderHTTP(map[string][]string{
			core.AccessKeyHeader:     {cfg.InCluster.AccessKey},
			core.AccountHeader:       {cfg.InCluster.Account},
			core.ClusterNameHeader:   {cfg.InCluster.ClusterName},
			core.HelmVersionHeader:   {os.Getenv("HELM_RELEASE")},
			core.VersionHeader:       {version},
			core.GitVersionHeader:    {gitVersion},
			core.CloudProviderHeader: {cloudProvider},
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
			if errors.As(err, &status) && status == http.StatusUnauthorized {
				return backoff.Permanent(fmt.Errorf("server rejected our credentials"))
			}
			return err
		}, utils.NewBackOff(false), func(err error, d time.Duration) {
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
	synchronizer, err := core.NewSynchronizerClient(ctx, adpts, conn, newConn)
	if err != nil {
		logger.L().Ctx(ctx).Fatal("failed to create synchronizer", helpers.Error(err))
	}
	err = synchronizer.Start(ctx)
	if err != nil {
		logger.L().Ctx(ctx).Fatal("error during sync, exiting", helpers.Error(err))
	}
}

func updateClusterName(cfg *config.Config) {
	// get cluster name from env
	clusterName, present := os.LookupEnv("CLUSTER_NAME")
	if present && clusterName != "" {
		logger.L().Debug("cluster name from env", helpers.String("clusterName", clusterName))
		cfg.InCluster.ClusterName = clusterName
	}
}
