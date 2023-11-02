package backend

import (
	"context"

	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
)

type Adapter struct {
	callbacks    domain.Callbacks
	cfg          config.Config
	client       adapters.Client
	pulsarClient pulsarconnector.Client
}

func NewBackendAdapter(cfg config.Config, pulsarClient pulsarconnector.Client) *Adapter {
	return &Adapter{
		cfg:          cfg,
		pulsarClient: pulsarClient,
	}
}

var _ adapters.Adapter = (*Adapter)(nil)

func (b *Adapter) DeleteObject(ctx context.Context, id domain.ClusterKindName) error {
	return b.client.DeleteObject(ctx, id)
}

func (b *Adapter) GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	return b.client.GetObject(ctx, id, baseObject)
}

func (b *Adapter) PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	return b.client.PatchObject(ctx, id, checksum, patch)
}

func (b *Adapter) PutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	return b.client.PutObject(ctx, id, object)
}

func (b *Adapter) RegisterCallbacks(callbacks domain.Callbacks) {
	b.callbacks = callbacks
}

func (b *Adapter) Init(ctx context.Context) error {
	b.client = NewClient(b.cfg, b.pulsarClient)
	b.client.RegisterCallbacks(b.callbacks)
	return b.client.Init(ctx)
}

func (b *Adapter) Start(mainCtx context.Context) error {
	go func() {
		_ = b.client.Start(mainCtx)
	}()
	return nil
}

func (b *Adapter) VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	return b.client.VerifyObject(ctx, id, checksum)
}
