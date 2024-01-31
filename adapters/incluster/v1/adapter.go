package incluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type BatchProcessingFunc func(context.Context, *Adapter, []domain.BatchItem) error

type Adapter struct {
	callbacks           domain.Callbacks
	cfg                 config.InCluster
	clients             map[string]adapters.Client
	batchProcessingFunc map[domain.BatchType]BatchProcessingFunc
	k8sclient           dynamic.Interface
}

func NewInClusterAdapter(cfg config.InCluster, k8sclient dynamic.Interface) *Adapter {
	return &Adapter{
		cfg:     cfg,
		clients: map[string]adapters.Client{},
		batchProcessingFunc: map[domain.BatchType]BatchProcessingFunc{
			"":                         commonBatchProcessingFunc, // regular processing, when batch type is not set
			domain.ReconciliationBatch: reconcileBatchProcessingFunc,
		},
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

func (a *Adapter) Batch(ctx context.Context, batchType domain.BatchType, messages []domain.BatchItem) error {
	if f, ok := a.batchProcessingFunc[batchType]; ok {
		logger.L().Debug("batch processing", helpers.String("batch type", string(batchType)))
		return f(ctx, a, messages)
	}

	return fmt.Errorf("batch type %s not supported", batchType)
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

func (a *Adapter) Stop(ctx context.Context) error {
	return nil
}

func (a *Adapter) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	return a.cfg.Account == id.Account && a.cfg.ClusterName == id.Cluster
}

// Batch processing functions
func reconcileBatchProcessingFunc(ctx context.Context, a *Adapter, messages []domain.BatchItem) error {
	kindToMessages := map[string][]domain.NewChecksum{}

	// build map of kind to list of messages
	for _, batchItem := range messages {
		payload := []byte(batchItem.Payload)

		// all messages should be of type EventNewChecksum
		if *batchItem.Event != domain.EventNewChecksum {
			return fmt.Errorf("invalid event type in reconciliation batch message (expected NewChecksum)")
		}

		var msg domain.NewChecksum
		err := json.Unmarshal(payload, &msg)
		if err != nil {
			return fmt.Errorf("failed unmarshal NewChecksum message in batch message: %w", err)
		}

		id := domain.KindName{
			Kind:      msg.Kind,
			Name:      msg.Name,
			Namespace: msg.Namespace,
		}

		kindToMessages[id.Kind.String()] = append(kindToMessages[id.Kind.String()], msg)
	}

	newBatch := []domain.BatchItem{}
	// for each kind, list all resources
	for kindStr, messages := range kindToMessages {
		kind := domain.KindFromString(ctx, kindStr)
		if kind == nil {
			return fmt.Errorf("invalid kind string: %s", kindStr)
		}

		res := schema.GroupVersionResource{Group: kind.Group, Version: kind.Version, Resource: kind.Resource}
		listRes, err := a.k8sclient.Resource(res).Namespace("").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		for _, messageInBatch := range messages {
			resourceFound := false
			// find resource in list
			for i, resource := range listRes.Items {
				// special handling for namespaces?
				if resource.GetName() == messageInBatch.Name && resource.GetNamespace() == messageInBatch.Namespace {
					resourceFound = true
					currentVersion := domain.ToResourceVersion(resource.GetResourceVersion())
					if currentVersion != messageInBatch.ResourceVersion { // should we use ">" ?
						// resource has changed, sending a put message
						logger.L().Debug("resource has changed",
							helpers.String("resource", messageInBatch.Kind.String()),
							helpers.String("name", messageInBatch.Name),
							helpers.String("namespace", messageInBatch.Namespace),
							helpers.Int("batch resource version", messageInBatch.ResourceVersion),
							helpers.Int("current resource version", currentVersion))
						event := domain.EventPutObject
						depth := ctx.Value(domain.ContextKeyDepth).(int)
						msgId := ctx.Value(domain.ContextKeyMsgId).(string)
						object, err := utils.FilterAndMarshal(&listRes.Items[i])
						if err != nil {
							return fmt.Errorf("filter and marshal resource failed in batch: %w", err)
						}
						msg := domain.PutObject{
							Depth:     depth + 1,
							Event:     &event,
							Kind:      kind,
							MsgId:     msgId,
							Name:      messageInBatch.Name,
							Namespace: messageInBatch.Namespace,
							Object:    string(object),
						}
						data, err := json.Marshal(msg)
						if err != nil {
							return fmt.Errorf("marshal put object message: %w", err)
						}
						newBatch = append(newBatch, domain.BatchItem{
							Event:   msg.Event,
							Payload: string(data),
						})

					} else {
						logger.L().Debug("resource has same version", helpers.String("resource", messageInBatch.Kind.String()), helpers.String("name", messageInBatch.Name), helpers.String("namespace", messageInBatch.Namespace))
					}
					break
				}
			}

			if !resourceFound {
				// resource was not found, sending delete message
				logger.L().Debug("resource missing", helpers.String("kind", messageInBatch.Kind.String()), helpers.String("name", messageInBatch.Name), helpers.String("namespace", messageInBatch.Namespace))

				event := domain.EventObjectDeleted
				depth := ctx.Value(domain.ContextKeyDepth).(int)
				msgId := ctx.Value(domain.ContextKeyMsgId).(string)
				msg := domain.ObjectDeleted{
					Depth:     depth + 1,
					Event:     &event,
					Kind:      kind,
					MsgId:     msgId,
					Name:      messageInBatch.Name,
					Namespace: messageInBatch.Namespace,
				}
				data, err := json.Marshal(msg)
				if err != nil {
					return fmt.Errorf("marshal delete message: %w", err)
				}
				newBatch = append(newBatch, domain.BatchItem{
					Event:   msg.Event,
					Payload: string(data),
				})
			}
		}
	}

	return a.callbacks.Batch(ctx, "", newBatch)
}

func commonBatchProcessingFunc(ctx context.Context, a *Adapter, messages []domain.BatchItem) error {
	// TODO: implement
	// Should iterate over all messages and call the appropriate client
	return fmt.Errorf("common batch processing not implemented")
}
