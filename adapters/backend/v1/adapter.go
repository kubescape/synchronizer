package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/goradd/maps"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

type Adapter struct {
	callbacksMap  maps.SafeMap[string, domain.Callbacks]
	clientsMap    maps.SafeMap[string, adapters.Client]
	connectionMap maps.SafeMap[string, string] // <cluster, account> -> <connection string>
	producer      messaging.MessageProducer
	consumer      messaging.MessageConsumer
	once          sync.Once
	mainContext   context.Context
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
		return client, nil
	}
	return nil, fmt.Errorf("unknown resource %s", id.String())
}

func (b *Adapter) Callbacks(ctx context.Context) (domain.Callbacks, error) {
	id := utils.ClientIdentifierFromContext(ctx)
	if callbacks, ok := b.callbacksMap.Load(id.String()); ok {
		return callbacks, nil
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
	b.callbacksMap.Set(id.String(), callbacks)
}

func (b *Adapter) Start(ctx context.Context) error {
	b.once.Do(func() {
		b.consumer.Start(b.mainContext, b)
	})
	// keeps track of the last connection string for each cluster/account
	id := utils.ClientIdentifierFromContext(ctx)
	b.connectionMap.Set(id.String(), id.ConnectionString())

	client := NewClient(b.producer)
	b.clientsMap.Set(id.String(), client)
	connectedClientsGauge.Inc()
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

func (b *Adapter) Stop(ctx context.Context) error {
	id := utils.ClientIdentifierFromContext(ctx)

	// make sure we are not removing a connected client if a different connection was already found
	if connectionString, ok := b.connectionMap.Load(id.String()); ok && connectionString != id.ConnectionString() {
		logger.L().Info("not removing connected client since a different connection was already found",
			helpers.String("old", id.ConnectionString()),
			helpers.String("existing", connectionString))
		return nil
	}

	logger.L().Debug("removing connected client", helpers.Interface("id", id))
	b.callbacksMap.Delete(id.String())
	if client, ok := b.clientsMap.Load(id.String()); ok {
		_ = client.Stop(ctx)
		b.clientsMap.Delete(id.String())
	}
	connectedClientsGauge.Dec()

	return nil
}

func (b *Adapter) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	return b.callbacksMap.Has(id.String()) && b.clientsMap.Has(id.String())
}

func (b *Adapter) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}

	return client.VerifyObject(ctx, id, checksum)
}
