package backend

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/config"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
)

type Client struct {
	callbacks       domain.Callbacks
	cfg             config.Config
	messageProducer messaging.MessageProducer
}

func NewClient(cfg config.Config, producer messaging.MessageProducer) *Client {
	return &Client{
		cfg:             cfg,
		messageProducer: producer,
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
	id := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.ServerConnectedMessage{
		Cluster: id.Cluster,
		Account: id.Account,
		Depth:   depth + 1,
		MsgId:   msgId,
	}
	logger.L().Debug("sending server connected message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal server connected message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, id, messaging.MsgPropEventValueServerConnectedMessage, data)
}

// FIXME no need to implement callPutOrPatch because we don't send patches from backend
// so you just call c.callbacks.PutObject instead

func (c *Client) callVerifyObject(ctx context.Context, id domain.KindName, object []byte) error {
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

func (c *Client) DeleteObject(ctx context.Context, id domain.KindName) error {
	// deleting the object is delegated to the ingester
	return c.sendDeleteObjectMessage(ctx, id)
}

func (c *Client) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	// getting the object is delegated to the ingester, we will send it to cluster once we get a NewObject message from Pulsar
	return c.sendGetObjectMessage(ctx, id, baseObject)
}

func (c *Client) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	// patching the object is delegated to the ingester, it might emit a GetObject message if the patching fails
	return c.sendPatchObjectMessage(ctx, id, checksum, patch)
}

func (c *Client) PutObject(ctx context.Context, id domain.KindName, object []byte) error {
	// putting the object into the data store is delegated to the ingester
	return c.sendPutObjectMessage(ctx, id, object)
}

func (c *Client) RegisterCallbacks(mainCtx context.Context, callbacks domain.Callbacks) {
	c.callbacks = callbacks
}

func (c *Client) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return c.callbacks, nil
}

func (c *Client) VerifyObject(ctx context.Context, id domain.KindName, checksum string) error {
	// verifying the object is delegated to the ingester, it might emit a GetObject message if the verification fails
	return c.sendVerifyObjectMessage(ctx, id, checksum)
}

func (c *Client) sendDeleteObjectMessage(ctx context.Context, id domain.KindName) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	cId := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.DeleteObjectMessage{
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
	}
	logger.L().Debug("Sending delete object message to producer",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueDeleteObjectMessage, data)
}

func (c *Client) sendGetObjectMessage(ctx context.Context, id domain.KindName, baseObject []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	cId := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.GetObjectMessage{
		BaseObject: baseObject,
		Cluster:    cId.Cluster,
		Account:    cId.Account,
		Depth:      depth + 1,
		Kind:       id.Kind.String(),
		MsgId:      msgId,
		Name:       id.Name,
		Namespace:  id.Namespace,
	}
	logger.L().Debug("Sending get object message to producer",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("base object size", len(msg.BaseObject)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueGetObjectMessage, data)
}

func (c *Client) sendPatchObjectMessage(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	cId := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.PatchObjectMessage{
		Checksum:  checksum,
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
		Patch:     patch,
	}
	logger.L().Debug("Sending patch object message to producer",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum),
		helpers.Int("patch size", len(msg.Patch)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValuePatchObjectMessage, data)
}

func (c *Client) sendPutObjectMessage(ctx context.Context, id domain.KindName, object []byte) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	cId := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.PutObjectMessage{
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
		Object:    object,
	}
	logger.L().Debug("Sending put object message to producer",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.Int("object size", len(msg.Object)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValuePutObjectMessage, data)
}

func (c *Client) sendVerifyObjectMessage(ctx context.Context, id domain.KindName, checksum string) error {
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	cId := domain.ClientIdentifierFromContext(ctx)

	msg := messaging.VerifyObjectMessage{
		Checksum:  checksum,
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
	}
	logger.L().Debug("Sending verify object message to producer",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal verify object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueVerifyObjectMessage, data)
}
