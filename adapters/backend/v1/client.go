package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"
	"unsafe"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	storageutils "github.com/kubescape/storage/pkg/utils"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/messaging"
	"github.com/kubescape/synchronizer/utils"
	"go.uber.org/multierr"
)

const MaxObjectSizeEnvVar = "MAX_OBJECT_SIZE"

type Client struct {
	callbacks          domain.Callbacks
	messageProducer    messaging.MessageProducer
	skipAlertsFrom     []string
	maxObjectSizeBytes *int
}

func NewClient(producer messaging.MessageProducer, skipAlertsFrom []string) *Client {
	return &Client{
		messageProducer:    producer,
		skipAlertsFrom:     skipAlertsFrom,
		maxObjectSizeBytes: getMaxObjectSizeBytes(),
	}
}

// an optional safe guard to prevent sending too large objects to the server
func getMaxObjectSizeBytes() *int {
	if val, ok := os.LookupEnv(MaxObjectSizeEnvVar); ok && val != "" {
		if intVal, err := strconv.Atoi(val); err != nil {
			logger.L().Error("failed to parse MAX_OBJECT_SIZE_BYTES", helpers.String("value", val), helpers.Error(err))
		} else {
			return &intVal
		}
	}
	return nil
}

var _ adapters.Client = (*Client)(nil)

func (c *Client) Start(ctx context.Context) error {
	if err := c.sendServerConnectedMessage(ctx); err != nil {
		return fmt.Errorf("failed to produce server connected message: %w", err)
	}

	if err := c.sendSingleConnectedClientsMessage(ctx); err != nil {
		return fmt.Errorf("failed to produce connected clients message: %w", err)
	}

	return nil
}

func (c *Client) Stop(_ context.Context) error {
	return nil
}

func (c *Client) IsRelated(ctx context.Context, id domain.ClientIdentifier) bool {
	identifierFromContext := utils.ClientIdentifierFromContext(ctx)
	return identifierFromContext.Cluster == id.Cluster && identifierFromContext.Account == id.Account
}

func (c *Client) sendServerConnectedMessage(ctx context.Context) error {
	id := utils.ClientIdentifierFromContext(ctx)

	msg := messaging.ServerConnectedMessage{
		Cluster: id.Cluster,
		Account: id.Account,
		MsgId:   utils.NewMsgId(),
	}
	logger.L().Debug("sending server connected message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("msgid", msg.MsgId))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal server connected message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, id, messaging.MsgPropEventValueServerConnectedMessage, data)
}

func (c *Client) sendSingleConnectedClientsMessage(ctx context.Context) error {
	id := utils.ClientIdentifierFromContext(ctx)

	hostname, _ := os.Hostname()
	msg := messaging.ConnectedClientsMessage{
		ServerName: hostname,
		Clients: []messaging.ConnectedClient{
			{
				Account:             id.Account,
				Cluster:             id.Cluster,
				SynchronizerVersion: id.SyncVersion,
				HelmVersion:         id.HelmVersion,
				ConnectionId:        id.ConnectionId,
				ConnectionTime:      id.ConnectionTime,
				GitVersion:          id.GitVersion,
				CloudProvider:       id.CloudProvider,
			}},
		Timestamp: time.Now(),
		MsgId:     utils.NewMsgId(),
	}

	logger.L().Debug("sending connected client message to producer upon startup",
		helpers.String("account", id.Account),
		helpers.String("cluster", id.Cluster),
		helpers.String("msgid", msg.MsgId))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal connected clients message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, id, messaging.MsgPropEventValueConnectedClientsMessage, data)
}

// no need to implement callPutOrPatch because we don't send patches from backend
// so you just call c.callbacks.PutObject instead

func (c *Client) callVerifyObject(ctx context.Context, id domain.KindName, object []byte) error {
	// calculate checksum
	checksum, err := storageutils.CanonicalHash(object)
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

func (c *Client) PutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	// putting the object into the data store is delegated to the ingester
	return c.sendPutObjectMessage(ctx, id, checksum, object)
}

func (c *Client) Batch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	var err error
	for _, item := range items.GetObject {
		id := domain.KindName{
			Kind:            &kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.GetObject(ctx, id, unsafe.Slice(unsafe.StringData(item.BaseObject), len(item.BaseObject))))
	}

	for _, item := range items.NewChecksum {
		id := domain.KindName{
			Kind:            &kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.VerifyObject(ctx, id, item.Checksum))
	}

	for _, item := range items.ObjectDeleted {
		id := domain.KindName{
			Kind:            &kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.DeleteObject(ctx, id))
	}

	for _, item := range items.PatchObject {
		id := domain.KindName{
			Kind:            &kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}

		err = multierr.Append(err, c.PatchObject(ctx, id, item.Checksum, unsafe.Slice(unsafe.StringData(item.Patch), len(item.Patch))))
	}

	for _, item := range items.PutObject {
		id := domain.KindName{
			Kind:            &kind,
			Name:            item.Name,
			Namespace:       item.Namespace,
			ResourceVersion: item.ResourceVersion,
		}
		err = multierr.Append(err, c.PutObject(ctx, id, item.Checksum, unsafe.Slice(unsafe.StringData(item.Object), len(item.Object))))
	}
	return err
}

func (c *Client) RegisterCallbacks(_ context.Context, callbacks domain.Callbacks) {
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
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	cId := utils.ClientIdentifierFromContext(ctx)

	msg := messaging.DeleteObjectMessage{
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
	}
	logger.L().Debug("sending delete object message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueDeleteObjectMessage, data, messaging.KindNameToCustomProperties(id))
}

func (c *Client) sendGetObjectMessage(ctx context.Context, id domain.KindName, baseObject []byte) error {
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	cId := utils.ClientIdentifierFromContext(ctx)

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
	logger.L().Debug("sending get object message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("name", id.Name),
		helpers.Int("base object size", len(msg.BaseObject)))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueGetObjectMessage, data, messaging.KindNameToCustomProperties(id))
}

func (c *Client) sendPatchObjectMessage(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	cId := utils.ClientIdentifierFromContext(ctx)

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
	logger.L().Debug("sending patch object message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValuePatchObjectMessage, data, messaging.KindNameToCustomProperties(id))
}

func (c *Client) sendPutObjectMessage(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	cId := utils.ClientIdentifierFromContext(ctx)

	if id.Kind.Resource == "runtimealerts" && slices.Contains(c.skipAlertsFrom, cId.Account) {
		logger.L().Debug("skipping put object message for alerts",
			helpers.String("account", cId.Account),
			helpers.String("cluster", cId.Cluster),
			helpers.String("kind", id.Kind.String()),
			helpers.String("name", id.Name))
		return nil
	}

	// a safe guard to prevent sending too large objects to the server
	if c.maxObjectSizeBytes != nil && len(object) > *c.maxObjectSizeBytes {
		logger.L().Error("skipping put object message for object size too large",
			helpers.String("account", cId.Account),
			helpers.String("cluster", cId.Cluster),
			helpers.String("kind", id.Kind.String()),
			helpers.String("name", id.Name),
			helpers.Int("object size", len(object)),
			helpers.Int("max object size", *c.maxObjectSizeBytes))
		return nil
	}

	msg := messaging.PutObjectMessage{
		Checksum:  checksum,
		Cluster:   cId.Cluster,
		Account:   cId.Account,
		Depth:     depth + 1,
		Kind:      id.Kind.String(),
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
		Object:    object,
	}
	logger.L().Debug("sending put object message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("name", id.Name))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValuePutObjectMessage, data, messaging.KindNameToCustomProperties(id))
}

func (c *Client) sendVerifyObjectMessage(ctx context.Context, id domain.KindName, checksum string) error {
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	cId := utils.ClientIdentifierFromContext(ctx)

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
	logger.L().Debug("sending verify object message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", id.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("name", id.Name),
		helpers.String("checksum", msg.Checksum))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal verify object message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, cId, messaging.MsgPropEventValueVerifyObjectMessage, data, messaging.KindNameToCustomProperties(id))
}

func (c *Client) SendReconciliationRequestMessage(ctx context.Context) error {
	id := utils.ClientIdentifierFromContext(ctx)

	msg := messaging.ReconciliationRequestMessage{
		Cluster:         id.Cluster,
		Account:         id.Account,
		MsgId:           utils.NewMsgId(),
		ServerInitiated: true,
	}
	logger.L().Debug("sending reconciliation request message to producer",
		helpers.String("account", msg.Account),
		helpers.String("cluster", msg.Cluster),
		helpers.String("msgid", msg.MsgId))

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal reconciliation request message: %w", err)
	}

	return c.messageProducer.ProduceMessage(ctx, id, messaging.MsgPropEventValueReconciliationRequestMessage, data)
}
