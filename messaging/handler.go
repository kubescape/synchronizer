package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/telemetry"
	"github.com/kubescape/synchronizer/utils"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/multierr"
)

type MessageHandler struct {
	Name string
	// reconciliationMessageMutex prevents multiple goroutines from processing reconciliation messages concurrently, as they can be large.
	reconciliationMessageMutex sync.Mutex
}

func kindFromPayload(ctx context.Context, kindStr, messageType string) (*domain.Kind, error) {
	kind := domain.KindFromString(ctx, kindStr)
	if kind == nil {
		return nil, fmt.Errorf("unknown kind %q in %s message", kindStr, messageType)
	}
	return kind, nil
}

func (h *MessageHandler) Handle(ctx context.Context, adapter adapters.Adapter, msg IncomingMessage) (result error) {
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(msg.Properties))
	finish := telemetry.Default().Begin(ctx, "mq_receive")
	defer func() { finish(result, len(msg.Payload)) }()
	msgProperties := msg.Properties
	clientIdentifier := domain.ClientIdentifier{
		Account: msgProperties[MsgPropAccount],
		Cluster: msgProperties[MsgPropCluster],
	}

	if !adapter.IsRelated(ctx, clientIdentifier) {
		logger.L().Debug("skipping message. client is not related to this instance",
			helpers.String("msgId", msg.ID),
			helpers.Interface("clientIdentifier", clientIdentifier),
			helpers.String("readerName", h.Name),
		)
		return nil
	}

	logger.L().Debug("received message from message queue",
		helpers.String("account", msgProperties[MsgPropAccount]),
		helpers.String("cluster", msgProperties[MsgPropCluster]),
		helpers.Interface("event", msgProperties[MsgPropEvent]),
		helpers.String("msgId", msg.ID))

	ctx = utils.ContextFromIdentifiers(ctx, clientIdentifier)
	callbacks, err := adapter.Callbacks(ctx)
	if err != nil {
		return fmt.Errorf("failed to get callbacks: %w", err)
	}

	switch msgProperties[MsgPropEvent] {
	case MsgPropEventValueReconciliationRequestMessage:
		var data ReconciliationRequestMessage
		h.reconciliationMessageMutex.Lock()
		defer h.reconciliationMessageMutex.Unlock()
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}

		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})

		event := domain.EventNewChecksum

		for kindStr, objects := range data.KindToObjects {
			kind := domain.KindFromString(ctx, kindStr)
			if kind == nil {
				err = multierr.Append(err, fmt.Errorf("unknown kind %s in batch", kindStr))
				continue
			}

			items := domain.BatchItems{
				NewChecksum: make([]domain.NewChecksum, 0, len(objects))}

			for _, object := range objects {
				items.NewChecksum = append(items.NewChecksum, domain.NewChecksum{
					Name:            object.Name,
					Namespace:       object.Namespace,
					ResourceVersion: object.ResourceVersion,
					Checksum:        object.Checksum,
					Kind:            kind,
					Event:           &event,
				})
			}

			err = multierr.Append(err, callbacks.Batch(ctx, *kind, domain.ReconciliationBatch, items))
		}
		if err != nil {
			return fmt.Errorf("failed to handle ReconciliationRequest message: %w", err)
		}
	case MsgPropEventValueGetObjectMessage:
		var data GetObjectMessage
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		kind, err := kindFromPayload(ctx, data.Kind, "GetObject")
		if err != nil {
			return err
		}
		if err := callbacks.GetObject(ctx, domain.KindName{
			Kind:            kind,
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.BaseObject); err != nil {
			return fmt.Errorf("failed to send GetObject message: %w", err)
		}
	case MsgPropEventValuePatchObjectMessage:
		var data PatchObjectMessage
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		kind, err := kindFromPayload(ctx, data.Kind, "PatchObject")
		if err != nil {
			return err
		}
		if err := callbacks.PatchObject(ctx, domain.KindName{
			Kind:            kind,
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Checksum, data.Patch); err != nil {
			return fmt.Errorf("failed to send PatchObject message: %w", err)
		}
	case MsgPropEventValueVerifyObjectMessage:
		var data VerifyObjectMessage
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		kind, err := kindFromPayload(ctx, data.Kind, "VerifyObject")
		if err != nil {
			return err
		}
		if err := callbacks.VerifyObject(ctx, domain.KindName{
			Kind:            kind,
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Checksum); err != nil {
			return fmt.Errorf("failed to send VerifyObject message: %w", err)
		}
	case MsgPropEventValuePutObjectMessage:
		var data PutObjectMessage
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		kind, err := kindFromPayload(ctx, data.Kind, "PutObject")
		if err != nil {
			return err
		}
		if err := callbacks.PutObject(ctx, domain.KindName{
			Kind:            kind,
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}, data.Checksum, data.Object); err != nil {
			return fmt.Errorf("failed to send PutObject message: %w", err)
		}
	case MsgPropEventValueDeleteObjectMessage:
		var data DeleteObjectMessage
		if err := json.Unmarshal(msg.Payload, &data); err != nil {
			return fmt.Errorf("failed to unmarshal message: %w", err)
		}
		ctx := utils.ContextFromGeneric(ctx, domain.Generic{
			Depth: data.Depth,
			MsgId: data.MsgId,
		})
		kind, err := kindFromPayload(ctx, data.Kind, "DeleteObject")
		if err != nil {
			return err
		}
		if err := callbacks.DeleteObject(ctx, domain.KindName{
			Kind:            kind,
			Name:            data.Name,
			Namespace:       data.Namespace,
			ResourceVersion: data.ResourceVersion,
		}); err != nil {
			return fmt.Errorf("failed to send DeleteObject message: %w", err)
		}
	default:
		return fmt.Errorf("unknown message type")
	}

	return nil
}
