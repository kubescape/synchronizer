package incluster

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"k8s.io/client-go/dynamic"
	"time"
)

type Adapter struct {
	callbacks domain.Callbacks
	cfg       config.InCluster
	clients   map[string]adapters.Client
	k8sclient dynamic.Interface
}

func NewInClusterAdapter(cfg config.InCluster, k8sclient dynamic.Interface) *Adapter {
	return &Adapter{
		cfg:       cfg,
		clients:   map[string]adapters.Client{},
		k8sclient: k8sclient,
	}
}

var _ adapters.Adapter = (*Adapter)(nil)

func (a *Adapter) GetClient(id domain.KindName) (adapters.Client, error) {
	if id.Kind == nil {
		return nil, fmt.Errorf("invalid resource kind. resource name: %s", id.Name)
	}
	client, ok := a.clients[id.Kind.String()]
	if !ok {
		client = NewClient(a.k8sclient, a.cfg.Account, a.cfg.ClusterName, config.Resource{
			Group:    id.Kind.Group,
			Version:  id.Kind.Version,
			Resource: id.Kind.Resource,
			Strategy: "copy",
		})
		a.clients[id.Kind.String()] = client
	}
	return client, nil
}

func (a *Adapter) DeleteObject(ctx context.Context, id domain.KindName) error {
	client, err := a.GetClient(id)
	if err != nil {
		return fmt.Errorf("failed to get client for resource %s: %w", id.Kind, err)
	}
	return client.DeleteObject(ctx, id)
}

func (a *Adapter) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	client, err := a.GetClient(id)
	if err != nil {
		return fmt.Errorf("failed to get client for resource %s: %w", id.Kind, err)
	}
	return client.GetObject(ctx, id, baseObject)
}

func (a *Adapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	client, err := a.GetClient(id)
	if err != nil {
		return fmt.Errorf("failed to get client for resource %s: %w", id.Kind, err)
	}
	return client.PatchObject(ctx, id, checksum, patch)
}

func (a *Adapter) PutObject(ctx context.Context, id domain.KindName, object []byte) error {
	client, err := a.GetClient(id)
	if err != nil {
		return fmt.Errorf("failed to get client for resource %s: %w", id.Kind, err)
	}
	return client.PutObject(ctx, id, object)
}

func (a *Adapter) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	client, err := a.GetClient(id)
	if err != nil {
		return fmt.Errorf("failed to get client for resource %s: %w", id.Kind, err)
	}
	return client.VerifyObject(ctx, id, checksum)
}

func (a *Adapter) RegisterCallbacks(_ context.Context, callbacks domain.Callbacks) {
	a.callbacks = callbacks
}

func (a *Adapter) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return a.callbacks, nil
}

func (a *Adapter) Start(ctx context.Context) error {
	for _, r := range a.cfg.Resources {
		client := NewClient(a.k8sclient, a.cfg.Account, a.cfg.ClusterName, r)
		client.RegisterCallbacks(ctx, a.callbacks)
		a.clients[r.String()] = client

		go func() {
			if err := backoff.RetryNotify(func() error {
				return client.Start(ctx)
			}, utils.NewBackOff(), func(err error, d time.Duration) {
				logger.L().Ctx(ctx).Warning("start client", helpers.Error(err),
					helpers.String("resource", client.res.Resource),
					helpers.String("retry in", d.String()))
			}); err != nil {
				logger.L().Ctx(ctx).Fatal("giving up start client", helpers.Error(err),
					helpers.String("resource", client.res.Resource))
			}
		}()
	}
	return nil
}
