package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"github.com/gobwas/ws/wsutil"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/adapters"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
	"github.com/panjf2000/ants/v2"
)

const maxMessageDepth = 8

type Synchronizer struct {
	adapter      adapters.Adapter
	isClient     bool // which side of the connection is this?
	conn         net.Conn
	outPool      *ants.PoolWithFunc
	readDataFunc func(rw io.ReadWriter) ([]byte, error)
}

func NewSynchronizerClient(adapter adapters.Adapter, conn net.Conn) *Synchronizer {
	return newSynchronizer(adapter, conn, true, wsutil.ReadServerBinary, wsutil.WriteClientBinary)
}

func NewSynchronizerServer(adapter adapters.Adapter, conn net.Conn) *Synchronizer {
	return newSynchronizer(adapter, conn, false, wsutil.ReadClientBinary, wsutil.WriteServerBinary)
}

func newSynchronizer(adapter adapters.Adapter, conn net.Conn, isClient bool, readDataFunc func(rw io.ReadWriter) ([]byte, error), writeDataFunc func(w io.Writer, p []byte) error) *Synchronizer {
	// outgoing message pool
	outPool, err := ants.NewPoolWithFunc(10, func(i interface{}) {
		data := i.([]byte)
		err := writeDataFunc(conn, data)
		if err != nil {
			logger.L().Error("cannot send message", helpers.Error(err))
			return
		}
	})
	if err != nil {
		logger.L().Fatal("unable to create outgoing message pool", helpers.Error(err))
	}
	s := &Synchronizer{
		adapter:      adapter,
		isClient:     isClient,
		conn:         conn,
		outPool:      outPool,
		readDataFunc: readDataFunc,
	}
	callbacks := domain.Callbacks{
		DeleteObject: s.DeleteObjectCallback,
		GetObject:    s.GetObjectCallback,
		PatchObject:  s.PatchObjectCallback,
		PutObject:    s.PutObjectCallback,
		VerifyObject: s.VerifyObjectCallback,
	}
	adapter.RegisterCallbacks(callbacks)
	return s
}

func (s *Synchronizer) DeleteObjectCallback(ctx context.Context, id domain.ClusterKindName) error {
	err := s.sendObjectDeleted(ctx, id)
	if err != nil {
		return fmt.Errorf("send delete: %w", err)
	}
	return nil
}

func (s *Synchronizer) GetObjectCallback(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	if s.isClient {
		baseObject = nil
	}
	err := s.sendGetObject(ctx, id, baseObject)
	if err != nil {
		return fmt.Errorf("send get object: %w", err)
	}
	return nil
}

func (s *Synchronizer) PatchObjectCallback(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	err := s.sendPatchObject(ctx, id, checksum, patch)
	if err != nil {
		return fmt.Errorf("send patch: %w", err)
	}
	return nil
}

func (s *Synchronizer) PutObjectCallback(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	err := s.sendPutObject(ctx, id, object)
	if err != nil {
		return fmt.Errorf("send put object: %w", err)
	}
	return nil
}

func (s *Synchronizer) VerifyObjectCallback(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	err := s.sendNewChecksum(ctx, id, checksum)
	if err != nil {
		return fmt.Errorf("send checksum: %w", err)
	}
	return nil
}

func (s *Synchronizer) Start() error {
	logger.L().Info("starting sync")
	// adapter events
	err := s.adapter.Start()
	if err != nil {
		return fmt.Errorf("start adapter: %w", err)
	}
	// synchronizer events
	err = s.listenForSyncEvents()
	if err != nil {
		return fmt.Errorf("listen for sync events: %w", err)
	}
	return nil
}

func (s *Synchronizer) listenForSyncEvents() error {
	// process incoming messages
	for {
		data, err := s.readDataFunc(s.conn)
		if err != nil {
			return fmt.Errorf("cannot read server data: %w", err)
		}
		// unmarshal message
		var generic domain.Generic
		err = json.Unmarshal(data, &generic)
		if err != nil {
			logger.L().Error("cannot unmarshal message", helpers.Error(err))
			continue
		}
		logger.L().Debug("received message", helpers.Interface("event", generic.Event.Value()),
			helpers.String("msgid", generic.MsgId), helpers.Int("depth", generic.Depth))
		// check message depth and ID
		if generic.Depth > maxMessageDepth {
			logger.L().Error("message depth too high", helpers.Int("depth", generic.Depth))
			continue
		}
		// store in context
		ctx := utils.ContextFromGeneric(generic)
		// handle message
		switch *generic.Event {
		case domain.EventGetObject:
			var msg domain.GetObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Error("cannot unmarshal message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
			id := domain.ClusterKindName{
				Cluster: msg.Cluster,
				Kind:    msg.Kind,
				Name:    msg.Name,
			}
			err := s.handleSyncGetObject(ctx, id, []byte(msg.BaseObject))
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		case domain.EventNewChecksum:
			var msg domain.NewChecksum
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Error("cannot unmarshal message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
			id := domain.ClusterKindName{
				Cluster: msg.Cluster,
				Kind:    msg.Kind,
				Name:    msg.Name,
			}
			err := s.handleSyncNewChecksum(ctx, id, msg.Checksum)
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		case domain.EventObjectDeleted:
			var msg domain.ObjectDeleted
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Error("cannot unmarshal message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
			id := domain.ClusterKindName{
				Cluster: msg.Cluster,
				Kind:    msg.Kind,
				Name:    msg.Name,
			}
			err := s.handleSyncObjectDeleted(ctx, id)
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		case domain.EventPatchObject:
			var msg domain.PatchObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Error("cannot unmarshal message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
			id := domain.ClusterKindName{
				Cluster: msg.Cluster,
				Kind:    msg.Kind,
				Name:    msg.Name,
			}
			err := s.handleSyncPatchObject(ctx, id, msg.Checksum, []byte(msg.Patch))
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		case domain.EventPutObject:
			var msg domain.PutObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Error("cannot unmarshal message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
			id := domain.ClusterKindName{
				Cluster: msg.Cluster,
				Kind:    msg.Kind,
				Name:    msg.Name,
			}
			err := s.handleSyncPutObject(ctx, id, []byte(msg.Object))
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		}
	}
}

func (s *Synchronizer) handleSyncGetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	err := s.adapter.GetObject(ctx, id, baseObject)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncNewChecksum(ctx context.Context, id domain.ClusterKindName, newChecksum string) error {
	err := s.adapter.VerifyObject(ctx, id, newChecksum)
	if err != nil {
		return fmt.Errorf("verify object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncObjectDeleted(ctx context.Context, id domain.ClusterKindName) error {
	err := s.adapter.DeleteObject(ctx, id)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncPatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	err := s.adapter.PatchObject(ctx, id, checksum, patch)
	if err != nil {
		return fmt.Errorf("patch object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncPutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	err := s.adapter.PutObject(ctx, id, object)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *Synchronizer) sendGetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error {
	event := domain.EventGetObject
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := domain.GetObject{
		BaseObject: string(baseObject),
		Cluster:    id.Cluster,
		Depth:      depth + 1,
		Event:      &event,
		Kind:       id.Kind,
		MsgId:      msgId,
		Name:       id.Name,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on get object message: %w", err)
	}
	logger.L().Debug("sent get object message",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("name", msg.Name),
		helpers.Int("base object size", len(msg.BaseObject)))
	return nil
}

func (s *Synchronizer) sendNewChecksum(ctx context.Context, id domain.ClusterKindName, checksum string) error {
	event := domain.EventNewChecksum
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := domain.NewChecksum{
		Checksum: checksum,
		Cluster:  id.Cluster,
		Depth:    depth + 1,
		Event:    &event,
		Kind:     id.Kind,
		MsgId:    msgId,
		Name:     id.Name,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal checksum message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on checksum message: %w", err)
	}
	logger.L().Debug("sent new checksum message",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("name", msg.Name),
		helpers.String("checksum", msg.Checksum))
	return nil
}

func (s *Synchronizer) sendObjectDeleted(ctx context.Context, id domain.ClusterKindName) error {
	event := domain.EventObjectDeleted
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := domain.ObjectDeleted{
		Cluster: id.Cluster,
		Depth:   depth + 1,
		Event:   &event,
		Kind:    id.Kind,
		MsgId:   msgId,
		Name:    id.Name,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on delete message: %w", err)
	}
	logger.L().Debug("sent object deleted message",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("name", msg.Name))
	return nil
}

func (s *Synchronizer) sendPatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error {
	event := domain.EventPatchObject
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := domain.PatchObject{
		Checksum: checksum,
		Cluster:  id.Cluster,
		Depth:    depth + 1,
		Event:    &event,
		Kind:     id.Kind,
		MsgId:    msgId,
		Name:     id.Name,
		Patch:    string(patch),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on patch message: %w", err)
	}
	logger.L().Debug("sent patch object message",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("name", msg.Name),
		helpers.String("checksum", msg.Checksum),
		helpers.Int("patch size", len(msg.Patch)))
	return nil
}

func (s *Synchronizer) sendPutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error {
	event := domain.EventPutObject
	depth := ctx.Value(domain.ContextKeyDepth).(int)
	msgId := ctx.Value(domain.ContextKeyMsgId).(string)
	msg := domain.PutObject{
		Cluster: id.Cluster,
		Depth:   depth + 1,
		Event:   &event,
		Kind:    id.Kind,
		MsgId:   msgId,
		Name:    id.Name,
		Object:  string(object),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on put object message: %w", err)
	}
	logger.L().Debug("sent put object message",
		helpers.String("cluster", msg.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("name", msg.Name),
		helpers.Int("object size", len(msg.Object)))
	return nil
}
