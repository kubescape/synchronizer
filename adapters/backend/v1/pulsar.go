package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	pulsarconnector "github.com/kubescape/messaging/pulsar/connector"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

const (
	// this will be used to filter out messages by the synchronizer server consumer (so it doesn't process its own messages)
	SynchronizerServerProducerKey = "SynchronizerServerProducer"
)

// ******************************
// * Pulsar Message Reader  *//
// ******************************

type PulsarMessageReader struct {
	name           string
	reader         pulsar.Reader
	messageChannel chan pulsar.Message
}

var _ messaging.MessageReader = (*PulsarMessageReader)(nil)

func NewPulsarMessageReader(cfg config.Config, pulsarClient pulsarconnector.Client) (*PulsarMessageReader, error) {
	msgChannel := make(chan pulsar.Message)

	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	readerName := fmt.Sprintf("%s-%s", cfg.Backend.Subscription, hostname)
	topic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, cfg.Backend.Topic)
	logger.L().Debug("creating new pulsar reader",
		helpers.String("readerName", readerName),
		helpers.String("topic", topic))

	reader, err := pulsarClient.CreateReader(
		pulsar.ReaderOptions{
			Name:                    readerName,
			Topic:                   topic,
			StartMessageID:          pulsar.LatestMessageID(),
			StartMessageIDInclusive: true,
		})

	if err != nil {
		panic(err)
	}

	return &PulsarMessageReader{
		name:           readerName,
		reader:         reader,
		messageChannel: msgChannel,
	}, nil
}

func (c *PulsarMessageReader) Start(mainCtx context.Context, adapter adapters.Adapter) {
	go func() {
		logger.L().Info("starting to read messages from pulsar")
		c.readerLoop(mainCtx)
	}()
	go func() {
		logger.L().Info("starting to listening on pulsar message channel")
		_ = c.listenOnMessageChannel(mainCtx, adapter)
	}()
}

func (c *PulsarMessageReader) readerLoop(ctx context.Context) {
	for {
		msg, err := c.reader.Next(context.Background())
		if err != nil {
			panic(err)
		}

		// skip messages sent by the synchronizer server
		if msg.Key() == SynchronizerServerProducerKey {
			continue
		}

		c.messageChannel <- msg
	}
}

func (c *PulsarMessageReader) listenOnMessageChannel(ctx context.Context, adapter adapters.Adapter) error {
	defer c.reader.Close()
	for {
		select {
		case <-ctx.Done():
			close(c.messageChannel)
			return nil
		case msg := <-c.messageChannel:
			msgID := utils.PulsarMessageIDtoString(msg.ID())
			if err := c.handleSingleSynchronizerMessage(ctx, adapter, msg); err != nil {
				logger.L().Ctx(ctx).Error("failed to handle message", helpers.Error(err), helpers.String("msgId", msgID))
			} else {
				logger.L().Ctx(ctx).Debug("message processed successfully", helpers.String("msgId", msgID))
			}
		}
	}
}

func (c *PulsarMessageReader) handleSingleSynchronizerMessage(ctx context.Context, adapter adapters.Adapter, msg pulsar.Message) error {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgProperties := msg.Properties()
	clientIdentifier := domain.ClientIdentifier{
		Account: msgProperties[messaging.MsgPropAccount],
		Cluster: msgProperties[messaging.MsgPropCluster],
	}

	if !adapter.IsRelated(ctx, clientIdentifier) {
		logger.L().Debug("skipping message from pulsar. client is not related to this instance",
			helpers.String("msgId", msgID),
			helpers.Interface("clientIdentifier", clientIdentifier),
			helpers.String("readerName", c.name),
		)
		return nil
	}

	logger.L().Debug("received message from pulsar",
		helpers.String("account", msgProperties[messaging.MsgPropAccount]),
		helpers.String("cluster", msgProperties[messaging.MsgPropCluster]),
		helpers.String("msgId", msgID))

	switch msgProperties[messaging.MsgPropEvent] {
	case messaging.MsgPropEventValueGetObjectMessage:
		var data messaging.GetObjectMessage
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
			Kind:      domain.KindFromString(ctx, data.Kind),
			Name:      data.Name,
			Namespace: data.Namespace,
		}, data.BaseObject); err != nil {
			return fmt.Errorf("failed to send GetObject message: %w", err)
		}
	case messaging.MsgPropEventValuePatchObjectMessage:
		var data messaging.PatchObjectMessage
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
			Kind:      domain.KindFromString(ctx, data.Kind),
			Name:      data.Name,
			Namespace: data.Namespace,
		}, data.Checksum, data.Patch); err != nil {
			return fmt.Errorf("failed to send PatchObject message: %w", err)
		}
	case messaging.MsgPropEventValueVerifyObjectMessage:
		var data messaging.VerifyObjectMessage
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
			Kind:      domain.KindFromString(ctx, data.Kind),
			Name:      data.Name,
			Namespace: data.Namespace,
		}, data.Checksum); err != nil {
			return fmt.Errorf("failed to send VerifyObject message: %w", err)
		}
	case messaging.MsgPropEventValuePutObjectMessage:
		var data messaging.PutObjectMessage
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
			Kind:      domain.KindFromString(ctx, data.Kind),
			Name:      data.Name,
			Namespace: data.Namespace,
		}, data.Object); err != nil {
			return fmt.Errorf("failed to send PutObject message: %w", err)
		}
	case messaging.MsgPropEventValueDeleteObjectMessage:
		var data messaging.DeleteObjectMessage
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
			Kind:      domain.KindFromString(ctx, data.Kind),
			Name:      data.Name,
			Namespace: data.Namespace,
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
	fullTopic := pulsarconnector.BuildPersistentTopic(pulsarClient.GetConfig().Tenant, pulsarClient.GetConfig().Namespace, topic)

	options := pulsar.ProducerOptions{
		DisableBatching:  true,
		EnableChunking:   true,
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
	producerMessage := NewProducerMessage(SynchronizerServerProducerKey, id.Account, id.Cluster, eventType, payload)
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

func NewProducerMessage(producerMessageKey, account, cluster, eventType string, payload []byte) *pulsar.ProducerMessage {
	producerMessageProperties := map[string]string{
		messaging.MsgPropTimestamp: time.Now().Format(time.RFC3339Nano),
		messaging.MsgPropAccount:   account,
		messaging.MsgPropCluster:   cluster,
		messaging.MsgPropEvent:     eventType,
	}
	return &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: producerMessageProperties,
		Key:        producerMessageKey,
	}
}
