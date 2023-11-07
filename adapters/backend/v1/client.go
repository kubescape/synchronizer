package backend

import (
	"context"
	"encoding/json"
	"fmt"
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

type Client struct {
	callbacks domain.Callbacks
	cfg       config.Config

	producer pulsarconnector.Producer
}

func NewClient(cfg config.Config, producer pulsarconnector.Producer) *Client {
	return &Client{
		cfg:      cfg,
		producer: producer,
	}
}

var _ adapters.Client = (*Client)(nil)

func (c *Client) Start(ctx context.Context) error {

	if err := c.sendServerConnectedMessage(ctx); err != nil {
		return fmt.Errorf("failed to produce server connected message: %w", err)
	}
	return nil
}

func (c *Client) sendServerConnectedMessage(ctx context.Context) error {
	ctx = utils.ContextFromGeneric(ctx, domain.Generic{})

	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	id := utils.ClusterKindNameFromContext(ctx)

	msg := synchronizer.ServerConnectedMessage{
		ClusterName:  id.Cluster,
		CustomerGUID: id.Account,
		Depth:        depth + 1,
		MsgId:        msgId,
	}
	logger.L().Debug("sending server connected message to pulsar",
		helpers.String("account", msg.CustomerGUID),
		helpers.String("cluster", msg.ClusterName),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal server connected message: %w", err)
	}

	return c.producePulsarMessage(ctx, id, synchronizer.MsgPropEventValueServerConnectedMessage, data)
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

func (c *Client) RegisterCallbacks(mainCtx context.Context, callbacks domain.Callbacks) {
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
