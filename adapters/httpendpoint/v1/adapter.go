package httpendpoint

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
)

type Adapter struct {
	callbacks  domain.Callbacks
	cfg        config.HTTPEndpoint
	clients    map[string]adapters.Client
	httpServer *http.Server
}

func NewHTTPEndpointAdapter(cfg config.HTTPEndpoint) *Adapter {
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.ServerPort),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
		Handler:      httpMux,
	}
	return &Adapter{
		cfg:        cfg,
		clients:    map[string]adapters.Client{},
		httpServer: server,
	}
}

// ensure that the Adapter struct satisfies the adapters.Adapter interface at compile-time
var _ adapters.Adapter = (*Adapter)(nil)

// func (a *Adapter) GetClient(id domain.KindName) (adapters.Client, error) {
// 	if id.Kind == nil {
// 		return nil, fmt.Errorf("invalid resource kind. resource name: %s", id.Name)
// 	}

// 	return a.GetClientByKind(*id.Kind), nil
// }

// func (a *Adapter) GetClientByKind(kind domain.Kind) adapters.Client {
// 	client, ok := a.clients[kind.String()]
// 	if !ok {
// 		client = NewClient(a.k8sclient, a.cfg.Account, a.cfg.ClusterName, config.Resource{
// 			Group:    kind.Group,
// 			Version:  kind.Version,
// 			Resource: kind.Resource,
// 			Strategy: "copy",
// 		})
// 		a.clients[kind.String()] = client
// 	}
// 	return client
// }

// No-OP functions for functions needed only for backend re-sync
func (a *Adapter) DeleteObject(ctx context.Context, id domain.KindName) error {
	return nil
}

func (a *Adapter) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	return nil
}

func (a *Adapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	return nil
}

func (a *Adapter) PutObject(ctx context.Context, id domain.KindName, object []byte) error {
	return nil
}

func (a *Adapter) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	return nil
}

func (a *Adapter) Batch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	return nil
}

func (a *Adapter) RegisterCallbacks(_ context.Context, callbacks domain.Callbacks) {
	a.callbacks = callbacks
}

func (a *Adapter) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return a.callbacks, nil
}

func (a *Adapter) ServeHttp(w http.ResponseWriter, r *http.Request) {
	logger.L().Info("httpendpoint request", helpers.String("path", r.URL.Path))
}

func (a *Adapter) Start(ctx context.Context) error {
	// for _, r := range a.cfg.Resources {
	// 	client.RegisterCallbacks(ctx, a.callbacks)

	// 	go func() {
	// 		if err := backoff.RetryNotify(func() error {
	// 			return client.Start(ctx)
	// 		}, utils.NewBackOff(), func(err error, d time.Duration) {
	// 			logger.L().Ctx(ctx).Warning("start client", helpers.Error(err),
	// 				helpers.String("resource", client.res.Resource),
	// 				helpers.String("retry in", d.String()))
	// 		}); err != nil {
	// 			logger.L().Ctx(ctx).Fatal("giving up start client", helpers.Error(err),
	// 				helpers.String("resource", client.res.Resource))
	// 		}
	// 	}()
	// }
	go func() {
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L().Ctx(ctx).Fatal("httpendpoint server error", helpers.Error(err))
		}
		logger.L().Ctx(ctx).Info("httpendpoint server stopped")
	}()
	logger.L().Ctx(ctx).Info("httpendpoint server started", helpers.String("port", a.cfg.ServerPort))
	return nil
}

func (a *Adapter) Stop(ctx context.Context) error {
	if err := a.httpServer.Shutdown(ctx); err != nil {
		return err
	}
	return nil
}

func (a *Adapter) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	return a.cfg.Account == id.Account && a.cfg.ClusterName == id.Cluster
}
