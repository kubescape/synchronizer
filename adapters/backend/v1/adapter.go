package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armosec/armoapi-go/identifiers"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/messaging/pulsar/common/synchronizer"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
)

const (
	SynchronizerServerProducerKey = "SynchronizerServerProducer"
)

type Adapter struct {
	callbacksMap sync.Map // <string>:<domain.Callbacks>
	clientsMap   sync.Map // <string>:<adapters.Client>
	cfg          config.Config
	pulsarClient pulsarconnector.Client

	producer pulsar.Producer
	once     sync.Once
}

func NewBackendAdapter(cfg config.Config, pulsarClient pulsarconnector.Client) *Adapter {
	return &Adapter{
		cfg:          cfg,
		pulsarClient: pulsarClient,
	}
}

var _ adapters.Adapter = (*Adapter)(nil)

func (b *Adapter) getClient(ctx context.Context) (adapters.Client, error) {
	id := domain.ClientIdentifierFromContext(ctx)
	if client, ok := b.clientsMap.Load(id.String()); ok {
		return client.(adapters.Client), nil
	}
	return nil, fmt.Errorf("unknown resource %s", id.String())
}

func (b *Adapter) getCallbacks(id domain.ClientIdentifier) (domain.Callbacks, error) {
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
	id := domain.ClientIdentifierFromContext(ctx)
	b.callbacksMap.Store(id.String(), callbacks)
}

func (b *Adapter) Start(ctx context.Context) error {
	// this code is executed only once on the server
	var initErr error
	b.once.Do(func() {
		b.producer, initErr = initPulsarProducer(b.cfg, b.pulsarClient)
		if initErr != nil {
			return
		}

		consumer, consumerMsgChannel, err := initPulsarConsumer(b.cfg, b.pulsarClient)
		if err != nil {
			initErr = err
			return
		}

		go func() {
			logger.L().Info("starting to consume messages from pulsar")
			_ = b.startConsumingMessages(ctx, consumer, consumerMsgChannel)
		}()
	})

	if initErr != nil {
		logger.L().Fatal("failed to start backend adapter", helpers.Error(initErr))
		return initErr
	}

	client := NewClient(b.cfg, b.producer)
	id := domain.ClientIdentifierFromContext(ctx)
	b.clientsMap.Store(id.String(), client)
	callbacks, err := b.getCallbacks(id)
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

func (b *Adapter) startConsumingMessages(ctx context.Context, consumer pulsar.Consumer, consumerMsgChannel chan pulsar.ConsumerMessage) error {
	defer consumer.Close()
	for {
		select {
		case <-ctx.Done():
			close(consumerMsgChannel)
			return nil
		case msg := <-consumerMsgChannel:
			// skip messages sent by the synchronizer server
			if msg.Key() == SynchronizerServerProducerKey {
				if err := consumer.Ack(msg); err != nil {
					logger.L().Error("failed to ack message", helpers.Error(err))
				}
				continue
			}
			// handle message
			if err := b.handleSingleSynchronizerMessage(ctx, msg); err != nil {
				logger.L().Error("failed to handle message", helpers.Error(err))
				consumer.Nack(msg)
			} else if err := consumer.Ack(msg); err != nil {
				logger.L().Error("failed to ack message", helpers.Error(err))
			}
		}
	}
}

func (b *Adapter) handleSingleSynchronizerMessage(ctx context.Context, msg pulsar.ConsumerMessage) error {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	logger.L().Debug("Received message from pulsar",
		helpers.String("account", msg.Properties()[identifiers.AttributeCustomerGUID]),
		helpers.String("cluster", msg.Properties()["clusterName"]),
		helpers.String("msgId", msgID))

	switch msg.Properties()[synchronizer.MsgPropEvent] {
	case synchronizer.MsgPropEventValueGetObjectMessage:
		var data synchronizer.GetObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		cId := domain.ClientIdentifier{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
		}
		callbacks, err := b.getCallbacks(cId)
		if err != nil {
			return err
		}
		if err := callbacks.GetObject(ctx, domain.KindName{
			Kind: domain.KindFromString(data.Kind),
			Name: data.Name,
		}, data.BaseObject); err != nil {
			return fmt.Errorf("failed to send GetObject message: %w", err)
		}
	case synchronizer.MsgPropEventValuePatchObjectMessage:
		var data synchronizer.PatchObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		cId := domain.ClientIdentifier{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
		}
		callbacks, err := b.getCallbacks(cId)
		if err != nil {
			return err
		}
		if err := callbacks.PatchObject(ctx, domain.KindName{
			Kind: domain.KindFromString(data.Kind),
			Name: data.Name,
		}, data.Checksum, data.Patch); err != nil {
			return fmt.Errorf("failed to send PatchObject message: %w", err)
		}
	case synchronizer.MsgPropEventValueVerifyObjectMessage:
		var data synchronizer.VerifyObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		cId := domain.ClientIdentifier{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
		}
		callbacks, err := b.getCallbacks(cId)
		if err != nil {
			return err
		}
		if err := callbacks.VerifyObject(ctx, domain.KindName{
			Kind: domain.KindFromString(data.Kind),
			Name: data.Name,
		}, data.Checksum); err != nil {
			return fmt.Errorf("failed to send VerifyObject message: %w", err)
		}
	case synchronizer.MsgPropEventValuePutObjectMessage:
		var data synchronizer.PutObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		cId := domain.ClientIdentifier{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
		}
		callbacks, err := b.getCallbacks(cId)
		if err != nil {
			return err
		}
		if err := callbacks.PutObject(ctx, domain.KindName{
			Kind: domain.KindFromString(data.Kind),
			Name: data.Name,
		}, data.Object); err != nil {
			return fmt.Errorf("failed to send PutObject message: %w", err)
		}
	case synchronizer.MsgPropEventValueDeleteObjectMessage:
		var data synchronizer.DeleteObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		cId := domain.ClientIdentifier{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
		}
		callbacks, err := b.getCallbacks(cId)
		if err != nil {
			return err
		}
		if err := callbacks.DeleteObject(ctx, domain.KindName{
			Kind: domain.KindFromString(data.Kind),
			Name: data.Name,
		}); err != nil {
			return fmt.Errorf("failed to send DeleteObject message: %w", err)
		}
	default:
		return fmt.Errorf("unknown message type")
	}

	return nil
}

func initPulsarProducer(cfg config.Config, pulsarClient pulsarconnector.Client) (pulsar.Producer, error) {
	topic := cfg.Backend.Topic
	fullTopic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, pulsarconnector.TopicName(topic))
	ksr, err := pulsarClient.CreateProducer(getCommonProducerOptions(fullTopic))
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic '%s': %w", topic, err)
	}

	return ksr, nil
}

func initPulsarConsumer(cfg config.Config, pulsarClient pulsarconnector.Client) (pulsar.Consumer, chan pulsar.ConsumerMessage, error) {
	consumerMsgChannel := make(chan pulsar.ConsumerMessage)
	consumer, err := pulsarClient.NewConsumer(pulsarconnector.WithTopic(cfg.Backend.Topic),
		pulsarconnector.WithMessageChannel(consumerMsgChannel),
		pulsarconnector.WithSubscriptionName(cfg.Backend.Subscription))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create new consumer: %w", err)
	}
	return consumer, consumerMsgChannel, nil
}

func getCommonProducerOptions(topic string) pulsar.ProducerOptions {
	// get pod name
	podName := os.Getenv("HOSTNAME")
	return pulsar.ProducerOptions{
		CompressionType:  pulsar.ZSTD,
		CompressionLevel: 1,
		Properties: map[string]string{
			"podName": podName,
		},
		Topic: topic,
	}
}
