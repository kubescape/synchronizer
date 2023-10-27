package incluster

import (
	"context"
	"fmt"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"k8s.io/client-go/dynamic"
)

type Adapter struct {
	callbacks domain.Callbacks
	cfg       config.Config
	clients   map[string]adapters.Client
	k8sclient dynamic.Interface
}

func NewInClusterAdapter(cfg config.Config, k8sclient dynamic.Interface) *Adapter {
	return &Adapter{
		cfg:       cfg,
		clients:   map[string]adapters.Client{},
		k8sclient: k8sclient,
	}
}

var _ adapters.Adapter = (*Adapter)(nil)

func (a *Adapter) DeleteObject(ctx context.Context, id domain.ClusterKindName) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.DeleteObject(ctx, id)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *Adapter) GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.GetObject(ctx, id, baseObject)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *Adapter) PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.PatchObject(ctx, id, checksum, patch)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *Adapter) PutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.PutObject(ctx, id, object)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *Adapter) RegisterCallbacks(callbacks domain.Callbacks) {
	a.callbacks = callbacks
}

func (a *Adapter) Start(mainCtx context.Context) error {
	for _, r := range a.cfg.Resources {
		client := NewClient(a.k8sclient, a.cfg.InCluster.ClusterName, r)
		client.RegisterCallbacks(a.callbacks)
		a.clients[r.String()] = client
		go func() {
			_ = client.Start(mainCtx)
		}()
	}
	return nil
}

func (a *Adapter) VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.VerifyObject(ctx, id, checksum)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}
