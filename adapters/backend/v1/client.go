package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

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

type Client struct {
	callbacks    domain.Callbacks
	cfg          config.Config
	pulsarClient pulsarconnector.Client

	producer           pulsarconnector.Producer
	consumer           pulsarconnector.Consumer
	consumerMsgChannel chan pulsar.ConsumerMessage
}

func NewClient(cfg config.Config, pulsarClient pulsarconnector.Client) *Client {
	return &Client{
		cfg:          cfg,
		pulsarClient: pulsarClient,
	}
}

var _ adapters.Client = (*Client)(nil)

func (c *Client) Init(ctx context.Context) error {
	var err error
	if err := c.initPulsarProducer(); err != nil {
		return err
	}

	c.consumerMsgChannel = make(chan pulsar.ConsumerMessage)
	c.consumer, err = c.pulsarClient.NewConsumer(pulsarconnector.WithTopic(c.cfg.Backend.Topic),
		pulsarconnector.WithMessageChannel(c.consumerMsgChannel),
		pulsarconnector.WithSubscriptionName(c.cfg.Backend.Subscription))
	if err != nil {
		return fmt.Errorf("failed to create synchronizerConsumer: %w", err)
	}
	return nil
}

func (c *Client) Start(mainCtx context.Context) error {
	defer c.consumer.Close()

	for {
		select {
		case <-mainCtx.Done():
			close(c.consumerMsgChannel)
			return nil
		case msg := <-c.consumerMsgChannel:
			// skip messages sent by the synchronizer server
			if msg.Key() == SynchronizerServerProducerKey {
				if err := c.consumer.Ack(msg); err != nil {
					logger.L().Error("failed to ack message", helpers.Error(err))
				}
				continue
			}
			// handle message
			if err := c.handleSingleSynchronizerMessage(msg); err != nil {
				logger.L().Error("failed to handle message", helpers.Error(err))
				c.consumer.Nack(msg)
			} else if err := c.consumer.Ack(msg); err != nil {
				logger.L().Error("failed to ack message", helpers.Error(err))
			}
		}
	}
}

func (c *Client) handleSingleSynchronizerMessage(msg pulsar.ConsumerMessage) error {
	msgID := utils.PulsarMessageIDtoString(msg.ID())
	msgTopic := msg.Topic()
	logger.L().Debug("Received message from pulsar",
		helpers.String("msgId", msgID),
		helpers.String("subscriptionName", c.cfg.Backend.Subscription),
		helpers.String("topicName", string(c.cfg.Backend.Topic)),
		helpers.String("msgTopic", msgTopic))

	switch msg.Properties()[synchronizer.MsgPropEvent] {
	case synchronizer.MsgPropEventValueGetObjectMessage:
		var data synchronizer.GetObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := c.callbacks.GetObject(ctx, domain.ClusterKindName{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
			Kind:    domain.KindFromString(data.Kind),
			Name:    data.Name,
		}, data.BaseObject); err != nil {
			return fmt.Errorf("failed to send GetObject message: %w", err)
		}
	case synchronizer.MsgPropEventValuePatchObjectMessage:
		var data synchronizer.PatchObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := c.callbacks.PatchObject(ctx, domain.ClusterKindName{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
			Kind:    domain.KindFromString(data.Kind),
			Name:    data.Name,
		}, data.Checksum, data.Patch); err != nil {
			return fmt.Errorf("failed to send PatchObject message: %w", err)
		}
	case synchronizer.MsgPropEventValueVerifyObjectMessage:
		var data synchronizer.VerifyObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := c.callbacks.VerifyObject(ctx, domain.ClusterKindName{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
			Kind:    domain.KindFromString(data.Kind),
			Name:    data.Name,
		}, data.Checksum); err != nil {
			return fmt.Errorf("failed to send VerifyObject message: %w", err)
		}
	case synchronizer.MsgPropEventValuePutObjectMessage:
		var data synchronizer.PutObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := c.callbacks.PutObject(ctx, domain.ClusterKindName{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
			Kind:    domain.KindFromString(data.Kind),
			Name:    data.Name,
		}, data.Object); err != nil {
			return fmt.Errorf("failed to send PutObject message: %w", err)
		}
	case synchronizer.MsgPropEventValueDeleteObjectMessage:
		var data synchronizer.DeleteObjectMessage
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		if err := c.callbacks.DeleteObject(ctx, domain.ClusterKindName{
			Account: data.CustomerGUID,
			Cluster: data.ClusterName,
			Kind:    domain.KindFromString(data.Kind),
			Name:    data.Name,
		}); err != nil {
			return fmt.Errorf("failed to send DeleteObject message: %w", err)
		}
	default:
		return fmt.Errorf("unknown message type")
	}

	return nil
}

// FIXME no need to implement callPutOrPatch because we don't send patches from backend
// so you just call c.callbacks.PutObject instead

func (c *Client) callVerifyObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	// calculate checksum
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return fmt.Errorf("calculate checksum: %w", err)
	}
	err = c.callbacks.VerifyObject(ctx, id, checksum)
	if err != nil {
		return fmt.Errorf("send checksum: %w", err)
	}
	return nil
}

func (c *Client) DeleteObject(ctx context.Context, id domain.ClusterKindName) error {
	// deleting the object is delegated to the ingester
	return c.sendDeleteObjectMessage(ctx, id)
}

func (c *Client) GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	// getting the object is delegated to the ingester, we will send it to cluster once we get a NewObject message from Pulsar
	return c.sendGetObjectMessage(ctx, id, baseObject)
}

func (c *Client) PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	// patching the object is delegated to the ingester, it might emit a GetObject message if the patching fails
	return c.sendPatchObjectMessage(ctx, id, checksum, patch)
}

func (c *Client) PutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	// putting the object into the data store is delegated to the ingester
	return c.sendPutObjectMessage(ctx, id, object)
}

func (c *Client) RegisterCallbacks(callbacks domain.Callbacks) {
	c.callbacks = callbacks
}

func (c *Client) VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	// verifying the object is delegated to the ingester, it might emit a GetObject message if the verification fails
	return c.sendVerifyObjectMessage(ctx, id, checksum)
}

func (c *Client) sendDeleteObjectMessage(ctx context.Context, id domain.ClusterKindName) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.DeleteObjectMessage{
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending delete object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete object message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValueDeleteObjectMessage, data)
}

func (c *Client) sendGetObjectMessage(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.GetObjectMessage{
		BaseObject:   baseObject,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending get object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("base object size", len(msg.BaseObject)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValueGetObjectMessage, data)
}

func (c *Client) sendPatchObjectMessage(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.PatchObjectMessage{
		Checksum:     checksum,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
		Patch:        patch,
	}
	logger.L().Debug("Sending patch object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum),
		helpers.Int("patch size", len(msg.Patch)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch object message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValuePatchObjectMessage, data)
}

func (c *Client) sendPutObjectMessage(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.PutObjectMessage{
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
		Object:       object,
	}
	logger.L().Debug("Sending put object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("object size", len(msg.Object)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValuePutObjectMessage, data)
}

func (c *Client) sendVerifyObjectMessage(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := synchronizer.VerifyObjectMessage{
		Checksum:     checksum,
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		Kind:         id.Kind.String(),
		MsgId:        msgId,
		Name:         id.Name,
	}
	logger.L().Debug("Sending verify object message to pulsar",
		helpers.String("cluster", msg.ClusterName),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal verify object message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValueVerifyObjectMessage, data)
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

func (c *Client) initPulsarProducer() error {
	topic := c.cfg.Backend.Topic
	fullTopic := pulsarconnector.BuildPersistentTopic(c.pulsarClient.GetConfig().Tenant, c.pulsarClient.GetConfig().Namespace, pulsarconnector.TopicName(topic))
	ksr, err := c.pulsarClient.CreateProducer(getCommonProducerOptions(fullTopic))
	if err != nil {
		return fmt.Errorf("failed to create producer for topic '%s': %w", topic, err)
	}

	c.producer = ksr
	return nil
}

func logSendErrors(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
	if err != nil {
		logger.L().Error("failed to send message to pulsar", helpers.Error(err))
	} else {
		logger.L().Debug("successfully sent message to pulsar", helpers.String("messageID", msgID.String()), helpers.Interface("messageProperties", message.Properties))
	}
}

func (c *Client) producePulsarMessage(ctx context.Context, id domain.ClusterKindName, eventType string, payload []byte) error {
	producerMessageProperties := map[string]string{
		"timestamp":                       time.Now().Format(time.RFC3339Nano),
		identifiers.AttributeCustomerGUID: id.Account,
		"clusterName":                     id.Cluster,
		synchronizer.MsgPropEvent:         eventType,
	}
	producerMessage := &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: producerMessageProperties,
		// this will be used to filter out messages by the synchronizer server consumer (so it doesn't process its own messages)
		Key: SynchronizerServerProducerKey,
	}
	c.producer.SendAsync(ctx, producerMessage, logSendErrors)
	return nil
}
