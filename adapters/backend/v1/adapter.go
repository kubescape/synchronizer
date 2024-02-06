package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goradd/maps"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

type Adapter struct {
	callbacksMap maps.SafeMap[string, domain.Callbacks]
	clientsMap   maps.SafeMap[string, *Client]

	connMapMutex  sync.RWMutex
	connectionMap map[string]domain.ClientIdentifier // <cluster, account> -> <connection string>
	producer      messaging.MessageProducer
	mainContext   context.Context
}

func NewBackendAdapter(mainContext context.Context, messageProducer messaging.MessageProducer, rtc *config.ReconciliationTaskConfig) *Adapter {
	adapter := &Adapter{
		producer:      messageProducer,
		mainContext:   mainContext,
		connectionMap: make(map[string]domain.ClientIdentifier),
	}

	adapter.startReconciliationPeriodicTask(mainContext, rtc)
	return adapter
}

var _ adapters.Adapter = (*Adapter)(nil)

func (b *Adapter) getClient(ctx context.Context) (adapters.Client, error) {
	id := utils.ClientIdentifierFromContext(ctx)
	if client, ok := b.clientsMap.Load(id.String()); ok {
		return client, nil
	}
	return nil, fmt.Errorf("client was missing from map %s (probably disconnected client)", id.String())
}

func (b *Adapter) Callbacks(ctx context.Context) (domain.Callbacks, error) {
	id := utils.ClientIdentifierFromContext(ctx)
	if callbacks, ok := b.callbacksMap.Load(id.String()); ok {
		return callbacks, nil
	}
	return domain.Callbacks{}, fmt.Errorf("callbacks for client %s were missing (probably disconnected client)", id.String())
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
	b.connMapMutex.Lock()
	defer b.connMapMutex.Unlock()
	incomingId := utils.ClientIdentifierFromContext(ctx)

	logger.L().Info("starting synchronizer backend adapter",
		helpers.Interface("id", incomingId),
	)

	// an existing different connection connection was found
	if existingId, ok := b.connectionMap[incomingId.String()]; ok && existingId.ConnectionString() != incomingId.ConnectionString() {
		// if the existing connection is newer than the incoming one, we don't start the synchronization
		if existingId.ConnectionTime.After(incomingId.ConnectionTime) {
			err := fmt.Errorf("failed to start synchronization for client because a newer connection exist")
			logger.L().Error(err.Error(),
				helpers.Interface("existingId", existingId),
				helpers.Interface("incomingId", incomingId))
			return err
		}
	}
	b.connectionMap[incomingId.String()] = incomingId

	client := NewClient(b.producer)
	b.clientsMap.Set(incomingId.String(), client)
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
	toDelete := utils.ClientIdentifierFromContext(ctx)

	b.connMapMutex.Lock()
	defer b.connMapMutex.Unlock()

	logger.L().Info("stopping synchronizer backend adapter",
		helpers.String("connectionString", toDelete.ConnectionString()),
	)
	// an existing different connection connection was found
	if existingId, ok := b.connectionMap[toDelete.String()]; ok && existingId.ConnectionString() != toDelete.ConnectionString() {
		if existingId.ConnectionTime.After(toDelete.ConnectionTime) {
			logger.L().Info("not removing connected client since a different connection with newer connection time found",
				helpers.Interface("toDelete", toDelete),
				helpers.Interface("existingId", existingId))
			return nil
		}
	}

	logger.L().Info("removing connected client from backend adapter",
		helpers.Interface("id", toDelete),
	)
	delete(b.connectionMap, toDelete.String())

	b.callbacksMap.Delete(toDelete.String())
	if client, ok := b.clientsMap.Load(toDelete.String()); ok {
		_ = client.Stop(ctx)
		b.clientsMap.Delete(toDelete.String())
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

func (b *Adapter) Batch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	client, err := b.getClient(ctx)
	if err != nil {
		return err
	}

	return client.Batch(ctx, kind, batchType, items)
}

// startReconciliationPeriodicTask starts a periodic task that sends reconciliation request messages to connected clients
// every configurable minutes (interval). If interval is 0 (not set), the task is disabled.
// intervalFromConnection is the minimum interval time in minutes from the connection time that the reconciliation task will be sent.
func (a *Adapter) startReconciliationPeriodicTask(mainCtx context.Context, cfg *config.ReconciliationTaskConfig) {
	if cfg == nil || cfg.TaskIntervalSeconds == 0 || cfg.IntervalFromConnectionSeconds == 0 {
		logger.L().Warning("reconciliation task is disabled (intervals are not set)")
		return
	}

	go func() {
		logger.L().Info("starting reconciliation periodic task",
			helpers.Int("TaskIntervalSeconds", cfg.TaskIntervalSeconds),
			helpers.Int("IntervalFromConnectionSeconds", cfg.IntervalFromConnectionSeconds))
		ticker := time.NewTicker(time.Duration(cfg.TaskIntervalSeconds) * time.Second)
		for {
			select {
			case <-mainCtx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				a.connMapMutex.Lock()
				logger.L().Info("running reconciliation task for connected clients", helpers.Int("clients", a.clientsMap.Len()))
				for connId, clientId := range a.connectionMap {
					if time.Since(clientId.ConnectionTime) < time.Duration(cfg.IntervalFromConnectionSeconds)*time.Second {
						logger.L().Info("skipping reconciliation request for client because it was connected recently", helpers.Interface("clientId", clientId.String()))
						continue
					}

					client, ok := a.clientsMap.Load(connId)
					if !ok {
						logger.L().Error("expected to find client for reconciliation in clients map", helpers.String("clientId", clientId.String()))
						continue
					}
					clientCtx := utils.ContextFromIdentifiers(mainCtx, clientId)
					err := client.SendReconciliationRequestMessage(clientCtx)
					if err != nil {
						logger.L().Error("failed to send reconciliation request message", helpers.String("error", err.Error()))
					} else {
						logger.L().Info("sent reconciliation request message", helpers.Interface("clientId", clientId))
					}
				}
				a.connMapMutex.Unlock()
			}
		}
	}()
}
