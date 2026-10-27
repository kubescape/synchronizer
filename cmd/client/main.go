package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gobwas/ws"
	backendUtils "github.com/kubescape/backend/pkg/utils"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/go-logger/otelsetup"
	"github.com/kubescape/k8s-interface/k8sinterface"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/adapters/httpendpoint/v1"
	"github.com/kubescape/synchronizer/adapters/incluster/v1"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/core"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/telemetry"
	"github.com/kubescape/synchronizer/utils"
	"go.opentelemetry.io/otel/propagation"
)

func main() {
	if err := run(); err != nil {
		logger.L().Fatal("synchronizer client stopped", helpers.Error(err))
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// load service config
	cfg, err := config.LoadConfig("/etc/config")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// load common ClusterConfig
	if clusterConfig, err := config.LoadClusterConfig(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to load cluster config")
	} else {
		cfg.InCluster.ClusterName = clusterConfig.ClusterName
		cfg.InCluster.Namespace = clusterConfig.Namespace
	}

	// load credentials (access key & account)
	if credentials, err := backendUtils.LoadCredentialsFromFile("/etc/credentials"); err == nil {
		cfg.InCluster.AccessKey = credentials.AccessKey
		cfg.InCluster.Account = credentials.Account
	}

	// Resolve operational telemetry before constructing loggers or instruments.
	if clusterName := os.Getenv("CLUSTER_NAME"); clusterName != "" {
		cfg.InCluster.ClusterName = clusterName
	}
	defer telemetry.Init(ctx, otelsetup.ProviderConfig{
		ServiceName: "synchronizer-client", ServiceVersion: os.Getenv("RELEASE"),
		AccountID: cfg.InCluster.Account, AccessKey: cfg.InCluster.AccessKey,
		ClusterName: cfg.InCluster.ClusterName, Namespace: cfg.InCluster.Namespace,
		NodeName: os.Getenv("NODE_NAME"), PodName: os.Getenv("POD_NAME"),
	}, os.Getenv("API_URL"))()

	apiURL := os.Getenv("API_URL")
	if apiURL != "" {
		if services, err := telemetry.LoadServices(ctx, apiURL); err != nil {
			logger.L().Warning("failed discovering synchronizer URL")
		} else if serverURL := services.GetSynchronizerUrl(); serverURL != "" {
			cfg.InCluster.ServerUrl = serverURL
		}
	}

	// init adapters
	var adpts []adapters.Adapter
	var k8sApi *k8sinterface.KubernetesApi
	if err := cfg.InCluster.ValidateConfig(); err == nil {
		dynamicClient, storageClient, err := utils.NewClient()
		if err != nil {
			return fmt.Errorf("create k8s client: %w", err)
		}
		k8sApi = k8sinterface.NewKubernetesApi()
		inClusterAdapter := incluster.NewInClusterAdapter(cfg.InCluster, dynamicClient, storageClient)
		adpts = append(adpts, inClusterAdapter)
	}
	if err := cfg.HTTPEndpoint.ValidateConfig(); err == nil {
		httpEndpointAdapter := httpendpoint.NewHTTPEndpointAdapter(cfg)
		adpts = append(adpts, httpEndpointAdapter)
	}
	if len(adpts) == 0 {
		return errors.New("no valid adapter found")
	}

	ctx = context.WithValue(ctx, domain.ContextKeyClientIdentifier, domain.ClientIdentifier{
		Account: cfg.InCluster.Account,
		Cluster: cfg.InCluster.ClusterName,
	})

	gitVersion, cloudProvider := incluster.GetApiServerGitVersionAndCloudProvider(ctx, k8sApi)
	clusterUID := incluster.GetClusterUID(ctx, k8sApi)
	resourceGroup := incluster.GetResourceGroup(ctx, k8sApi)

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
			core.ClusterUIDHeader:    {clusterUID},
			core.ResourceGroupHeader: {resourceGroup},
		}),
		NetDial: utils.GetDialer(),
	}

	// start pprof server
	utils.ServePprof()

	// start liveness probe
	utils.StartLivenessProbe()

	// websocket client
	newConn := func() (net.Conn, error) {
		return connectWebsocket(ctx, dialer, cfg.InCluster.ServerUrl, version)
	}

	conn, err := newConn()
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("connect: %w", err)
	}

	// synchronizer
	synchronizer, err := core.NewSynchronizerClient(ctx, adpts, conn, newConn, core.WithMaxMessageBytes(int64(cfg.InCluster.MaxMessageBytes)))
	if err != nil {
		return fmt.Errorf("create synchronizer: %w", err)
	}
	go func() { <-ctx.Done(); _ = synchronizer.Stop(ctx) }()
	err = synchronizer.Start(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("synchronize: %w", err)
	}
	return nil
}

// connectWebsocket owns each attempt error independently of the caller lifecycle.
func connectWebsocket(ctx context.Context, dialer ws.Dialer, serverURL, version string) (net.Conn, error) {
	var conn net.Conn
	if err := backoff.RetryNotify(func() error {
		attemptCtx, finish := telemetry.Default().Control(ctx, "connect")
		attemptDialer := dialer
		headers := http.Header{}
		propagation.TraceContext{}.Inject(attemptCtx, propagation.HeaderCarrier(headers))
		attemptDialer.Header = ws.HandshakeHeaderFunc(func(w io.Writer) (int64, error) {
			n, err := dialer.Header.WriteTo(w)
			if err != nil {
				return n, err
			}
			var b bytes.Buffer
			_ = headers.Write(&b)
			m, err := w.Write(b.Bytes())
			return n + int64(m), err
		})
		var err error
		conn, _, _, err = attemptDialer.Dial(attemptCtx, serverURL)
		finish(err)
		var status ws.StatusError
		if errors.As(err, &status) && status == http.StatusFailedDependency {
			return backoff.Permanent(fmt.Errorf("server rejected our client version <%s>, please update", version))
		}
		if errors.As(err, &status) && status == http.StatusUnauthorized {
			logger.L().Info("authentication failed", helpers.Error(err), helpers.String("url", serverURL))
			return backoff.Permanent(fmt.Errorf("server rejected our credentials"))
		}
		return err
	}, backoff.WithContext(utils.NewBackOff(false), ctx), func(err error, d time.Duration) {
		logger.L().Ctx(ctx).Warning("connection error", helpers.Error(err),
			helpers.String("retry in", d.String()))
	}); err != nil {
		return nil, fmt.Errorf("unable to create websocket connection: %w", err)
	}
	return conn, nil
}
