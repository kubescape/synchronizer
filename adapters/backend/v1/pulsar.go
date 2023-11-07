package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/messaging/pulsar/common/synchronizer"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
)

const (
	// this will be used to filter out messages by the synchronizer server consumer (so it doesn't process its own messages)
	SynchronizerServerProducerKey = "SynchronizerServerProducer"
)

// ******************************
// * Pulsar Message Consumer  *//
// ******************************

type PulsarMessageConsumer struct {
	consumer       pulsar.Consumer
	messageChannel chan pulsar.ConsumerMessage
}

func NewPulsarMessageConsumer(cfg config.Config, pulsarClient pulsarconnector.Client) (*PulsarMessageConsumer, error) {
	consumerMsgChannel := make(chan pulsar.ConsumerMessage)
	consumer, err := pulsarClient.NewConsumer(pulsarconnector.WithTopic(cfg.Backend.Topic),
		pulsarconnector.WithMessageChannel(consumerMsgChannel),
		pulsarconnector.WithSubscriptionName(cfg.Backend.Subscription))
	if err != nil {
		return nil, fmt.Errorf("failed to create new consumer: %w", err)
	}

	return &PulsarMessageConsumer{consumer: consumer, messageChannel: consumerMsgChannel}, nil
}

func (c *PulsarMessageConsumer) Start(ctx context.Context, adapter *Adapter) {
	go func() {
		logger.L().Info("starting to consume messages from pulsar")
		_ = c.startConsumingMessages(ctx, adapter)
	}()
}

func (c *PulsarMessageConsumer) startConsumingMessages(ctx context.Context, adapter *Adapter) error {
	defer c.consumer.Close()
	for {
		select {
		case <-ctx.Done():
			close(c.messageChannel)
			return nil
		case msg := <-c.messageChannel:
			// skip messages sent by the synchronizer server
			if msg.Key() == SynchronizerServerProducerKey {
				if err := c.consumer.Ack(msg); err != nil {
					logger.L().Error("failed to ack message", helpers.Error(err))
				}
				continue
			}
			// handle message
			if err := c.handleSingleSynchronizerMessage(ctx, adapter, msg); err != nil {
				logger.L().Error("failed to handle message", helpers.Error(err))
				c.consumer.Nack(msg)
			} else if err := c.consumer.Ack(msg); err != nil {
				logger.L().Error("failed to ack message", helpers.Error(err))
			}
		}
	}
}

func (c *PulsarMessageConsumer) handleSingleSynchronizerMessage(ctx context.Context, adapter *Adapter, msg pulsar.ConsumerMessage) error {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgProperties := msg.Properties()

	logger.L().Debug("Received message from pulsar",
		helpers.String("account", msgProperties[synchronizer.MsgPropAccount]),
		helpers.String("cluster", msgProperties[synchronizer.MsgPropCluster]),
		helpers.String("msgId", msgID))

	switch msgProperties[synchronizer.MsgPropEvent] {
	case synchronizer.MsgPropEventValueGetObjectMessage:
		var data synchronizer.GetObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		ctx = utils.ContextFromIdentifiers(ctx, domain.ClientIdentifier{
			Account: data.Account,
			Cluster: data.Cluster,
		})
		callbacks, err := adapter.Callbacks(ctx)
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
		ctx = utils.ContextFromIdentifiers(ctx, domain.ClientIdentifier{
			Account: data.Account,
			Cluster: data.Cluster,
		})
		callbacks, err := adapter.Callbacks(ctx)
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
		ctx = utils.ContextFromIdentifiers(ctx, domain.ClientIdentifier{
			Account: data.Account,
			Cluster: data.Cluster,
		})
		callbacks, err := adapter.Callbacks(ctx)
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
		ctx = utils.ContextFromIdentifiers(ctx, domain.ClientIdentifier{
			Account: data.Account,
			Cluster: data.Cluster,
		})
		callbacks, err := adapter.Callbacks(ctx)
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
		ctx = utils.ContextFromIdentifiers(ctx, domain.ClientIdentifier{
			Account: data.Account,
			Cluster: data.Cluster,
		})
		callbacks, err := adapter.Callbacks(ctx)
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

// ******************************
// * Pulsar Message Producer  *//
// ******************************

type PulsarMessageProducer struct {
	producer pulsar.Producer
}

func NewPulsarMessageProducer(cfg config.Config, pulsarClient pulsarconnector.Client) (*PulsarMessageProducer, error) {
	topic := cfg.Backend.Topic
	fullTopic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, pulsarconnector.TopicName(topic))

	options := pulsar.ProducerOptions{
		CompressionType:  pulsar.ZSTD,
		CompressionLevel: 1,
		Properties: map[string]string{
			"podName": os.Getenv("HOSTNAME"),
		},
		Topic: fullTopic,
	}

	producer, err := pulsarClient.CreateProducer(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for topic '%s': %w", topic, err)
	}

	return &PulsarMessageProducer{producer: producer}, nil
}

func (p *PulsarMessageProducer) ProduceMessage(ctx context.Context, id domain.ClientIdentifier, eventType string, payload []byte) error {
	producerMessage := synchronizer.CreateProducerMessage(SynchronizerServerProducerKey, id.Account, id.Cluster, eventType, payload)
	p.producer.SendAsync(ctx, producerMessage, logPulsarSyncAsyncErrors)
	return nil
}

func logPulsarSyncAsyncErrors(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	if err != nil {
		logger.L().Error("failed to send message to pulsar", helpers.Error(err))
	} else {
		logger.L().Debug("successfully sent message to pulsar", helpers.String("messageID", msgID.String()), helpers.Interface("messageProperties", message.Properties))
	}
}
