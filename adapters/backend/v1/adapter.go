package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

type Adapter struct {
	callbacksMap sync.Map // <string>:<domain.Callbacks>
	clientsMap   sync.Map // <string>:<adapters.Client>
	producer     messaging.MessageProducer
	consumer     messaging.MessageConsumer
	once         sync.Once
	mainContext  context.Context
}

func NewBackendAdapter(mainContext context.Context, messageProducer messaging.MessageProducer, messageConsumer messaging.MessageConsumer) *Adapter {
	adapter := &Adapter{
		producer:    messageProducer,
		consumer:    messageConsumer,
		mainContext: mainContext,
	}
	return adapter
}

var _ adapters.Adapter = (*Adapter)(nil)

func (b *Adapter) getClient(ctx context.Context) (adapters.Client, error) {
	id := utils.ClientIdentifierFromContext(ctx)
	if client, ok := b.clientsMap.Load(id.String()); ok {
		return client.(adapters.Client), nil
	}
	return nil, fmt.Errorf("unknown resource %s", id.String())
}

func (b *Adapter) Callbacks(ctx context.Context) (domain.Callbacks, error) {
	id := utils.ClientIdentifierFromContext(ctx)
	if callbacks, ok := b.callbacksMap.Load(id.String()); ok {
		return callbacks.(domain.Callbacks), nil
	}
	return domain.Callbacks{}, fmt.Errorf("unknown resource %s", id.String())
}

func (b *Adapter) DeleteObject(ctx context.Context, id domain.KindName) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}
	return client.DeleteObject(ctx, id)
}

func (b *Adapter) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}
	return client.GetObject(ctx, id, baseObject)
}

func (b *Adapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}
	return client.PatchObject(ctx, id, checksum, patch)
}

func (b *Adapter) PutObject(ctx context.Context, id domain.KindName, object []byte) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}
	return client.PutObject(ctx, id, object)
}

func (b *Adapter) RegisterCallbacks(ctx context.Context, callbacks domain.Callbacks) {
	id := utils.ClientIdentifierFromContext(ctx)
	b.callbacksMap.Store(id.String(), callbacks)
}

func (b *Adapter) Start(ctx context.Context) error {
	b.once.Do(func() {
		b.consumer.Start(b.mainContext, b)
	})

	client := NewClient(b.producer)
	id := utils.ClientIdentifierFromContext(ctx)
	b.clientsMap.Store(id.String(), client)
	callbacks, err := b.Callbacks(ctx)
	if err != nil {
		return err
	}

	client.RegisterCallbacks(ctx, callbacks)
	go func() {
		_ = client.Start(ctx)
	}()

	return nil
}

func (b *Adapter) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}

	return client.VerifyObject(ctx, id, checksum)
}
