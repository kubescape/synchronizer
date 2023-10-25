package incluster

import (
	"context"
	"fmt"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"k8s.io/client-go/dynamic"
)

type InClusterAdapter struct {
	callbacks domain.Callbacks
	cfg       config.Config
	clients   map[string]*Client
	k8sclient dynamic.Interface
}

func NewInClusterAdapter(cfg config.Config, k8sclient dynamic.Interface) *InClusterAdapter {
	return &InClusterAdapter{
		cfg:       cfg,
		clients:   map[string]*Client{},
		k8sclient: k8sclient,
	}
}

var _ adapters.Adapter = (*InClusterAdapter)(nil)

func (a *InClusterAdapter) DeleteObject(_ context.Context, id domain.ClusterKindName) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.DeleteObject(id)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *InClusterAdapter) GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.GetObject(ctx, id, baseObject)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *InClusterAdapter) PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		baseObject, err := client.PatchObject(id, checksum, patch)
		if err != nil {
			logger.L().Warning("patch object, sending get object", helpers.Error(err))
			return a.callbacks.GetObject(ctx, id, baseObject)
		}
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *InClusterAdapter) PutObject(_ context.Context, id domain.ClusterKindName, object []byte) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		return client.PutObject(object)
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}

func (a *InClusterAdapter) RegisterCallbacks(callbacks domain.Callbacks) {
	a.callbacks = callbacks
}

func (a *InClusterAdapter) Start() error {
	for _, r := range a.cfg.Resources {
		client := NewClient(a.k8sclient, a.cfg.InCluster.ClusterName, r, a.callbacks)
		a.clients[r.String()] = client
		go client.Run()
	}
	return nil
}

func (a *InClusterAdapter) VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	if client, ok := a.clients[id.Kind.String()]; ok {
		baseObject, err := client.VerifyObject(id, checksum)
		if err != nil {
			logger.L().Warning("verify object, sending get object", helpers.Error(err))
			return a.callbacks.GetObject(ctx, id, baseObject)
		}
	}
	return fmt.Errorf("unknown resource %s", id.Kind.String())
}
