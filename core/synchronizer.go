package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"

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
	adapters      []adapters.Adapter
	isClient      bool // which side of the connection is this?
	Conn          *net.Conn
	newConn       func() (net.Conn, error)
	outPool       *ants.PoolWithFunc
	inPool        *ants.PoolWithFunc
	readDataFunc  func(rw io.ReadWriter) ([]byte, error)
	writeDataFunc func(w io.Writer, p []byte) error
}

func NewSynchronizerClient(mainCtx context.Context, adapter []adapters.Adapter, conn net.Conn, newConn func() (net.Conn, error)) (*Synchronizer, error) {
	s, err := newSynchronizer(mainCtx, adapter, conn, true, wsutil.ReadServerBinary, wsutil.WriteClientBinary)
	if err != nil {
		return nil, err
	}
	s.newConn = newConn
	return s, nil
}

func NewSynchronizerServer(mainCtx context.Context, adapter []adapters.Adapter, conn net.Conn) (*Synchronizer, error) {
	return newSynchronizer(mainCtx, adapter, conn, false, wsutil.ReadClientBinary, wsutil.WriteServerBinary)
}

func newSynchronizer(mainCtx context.Context, adapter []adapters.Adapter, conn net.Conn, isClient bool, readDataFunc func(rw io.ReadWriter) ([]byte, error), writeDataFunc func(w io.Writer, p []byte) error) (*Synchronizer, error) {
	s := &Synchronizer{
		adapters:      adapter,
		isClient:      isClient,
		Conn:          &conn,
		readDataFunc:  readDataFunc,
		writeDataFunc: writeDataFunc,
	}
	// outgoing message pool
	var err error
	s.outPool, err = ants.NewPoolWithFunc(1, func(i interface{}) {
		data := i.([]byte)
		s.sendData(mainCtx, data)
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create outgoing message pool: %w", err)
	}
	callbacks := domain.Callbacks{
		DeleteObject: s.DeleteObjectCallback,
		GetObject:    s.GetObjectCallback,
		PatchObject:  s.PatchObjectCallback,
		PutObject:    s.PutObjectCallback,
		VerifyObject: s.VerifyObjectCallback,
		Batch:        s.BatchCallback,
	}
	for _, adapter := range s.adapters {
		adapter.RegisterCallbacks(mainCtx, callbacks)
	}
	return s, nil
}

func (s *Synchronizer) sendData(ctx context.Context, data []byte) {
	if err := backoff.RetryNotify(func() error {
		err := s.writeDataFunc(*s.Conn, data)
		if err != nil {
			// close connection
			_ = (*s.Conn).Close()
			if s.isClient {
				// try to reconnect
				conn, err := s.newConn()
				if err != nil {
					return fmt.Errorf("refreshing outgoing connection: %w", err)
				}
				logger.L().Ctx(ctx).Info("outgoing connection refreshed, synchronization will resume")
				s.Conn = &conn
				return s.writeDataFunc(*s.Conn, data)
			} else {
				return backoff.Permanent(fmt.Errorf("cannot send message: %w", err))
			}
		}
		return nil
	}, utils.NewBackOff(true), func(err error, d time.Duration) {
		logger.L().Ctx(ctx).Warning("send data", helpers.Error(err),
			helpers.String("retry in", d.String()))
	}); err != nil {
		logger.L().Ctx(ctx).Error("giving up send data", helpers.Error(err))
		if err := s.Stop(ctx); err != nil {
			logger.L().Ctx(ctx).Error("error stopping synchronizer", helpers.Error(err))
		}
	}
}

func (s *Synchronizer) DeleteObjectCallback(ctx context.Context, id domain.KindName) error {
	err := s.sendObjectDeleted(ctx, id)
	if err != nil {
		return fmt.Errorf("send delete: %w", err)
	}
	return nil
}

func (s *Synchronizer) BatchCallback(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	err := s.sendBatch(ctx, kind, batchType, items)
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

func (s *Synchronizer) GetObjectCallback(ctx context.Context, id domain.KindName, baseObject []byte) error {
	if s.isClient {
		baseObject = nil
	}
	err := s.sendGetObject(ctx, id, baseObject)
	if err != nil {
		return fmt.Errorf("send get object: %w", err)
	}
	return nil
}

func (s *Synchronizer) PatchObjectCallback(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	err := s.sendPatchObject(ctx, id, checksum, patch)
	if err != nil {
		return fmt.Errorf("send patch: %w", err)
	}
	return nil
}

func (s *Synchronizer) PutObjectCallback(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	err := s.sendPutObject(ctx, id, checksum, object)
	if err != nil {
		return fmt.Errorf("send put object: %w", err)
	}
	return nil
}

func (s *Synchronizer) VerifyObjectCallback(ctx context.Context, id domain.KindName, checksum string) error {
	err := s.sendNewChecksum(ctx, id, checksum)
	if err != nil {
		return fmt.Errorf("send checksum: %w", err)
	}
	return nil
}

func (s *Synchronizer) Start(ctx context.Context) error {
	hostname, _ := os.Hostname()

	identifiers := utils.ClientIdentifierFromContext(ctx)
	logger.L().Info("starting synchronization",
		helpers.String("account", identifiers.Account),
		helpers.String("cluster", identifiers.Cluster),
		helpers.String("connId", identifiers.ConnectionId),
		helpers.String("host", hostname))

	if s.isClient {
		// send ping
		go s.sendPing(ctx)
	}
	// adapter events
	for _, adapter := range s.adapters {
		err := adapter.Start(ctx)
		if err != nil {
			return fmt.Errorf("start adapter: %w", err)
		}
	}
	// synchronizer events

	if err := s.listenForSyncEvents(ctx); err != nil {
		return fmt.Errorf("listen for sync events: %w", err)
	}
	return nil
}

func (s *Synchronizer) Stop(ctx context.Context) error {
	hostname, _ := os.Hostname()
	identifier := utils.ClientIdentifierFromContext(ctx)

	logger.L().Info("stopping synchronization",
		helpers.String("account", identifier.Account),
		helpers.String("cluster", identifier.Cluster),
		helpers.String("connId", identifier.ConnectionId),
		helpers.String("host", hostname))
	if s.outPool != nil {
		logger.L().Info("releasing out pool",
			helpers.String("account", identifier.Account),
			helpers.String("cluster", identifier.Cluster),
			helpers.String("connId", identifier.ConnectionId),
			helpers.String("host", hostname))
		s.outPool.Release()
	}
	if s.inPool != nil {
		logger.L().Info("releasing in pool",
			helpers.String("account", identifier.Account),
			helpers.String("cluster", identifier.Cluster),
			helpers.String("connId", identifier.ConnectionId),
			helpers.String("host", hostname))
		s.inPool.Release()
	}

	return s.stopAdapters(ctx)
}

func (s *Synchronizer) stopAdapters(ctx context.Context) error {
	for idx, adapter := range s.adapters {
		err := adapter.Stop(ctx)
		if err != nil {
			return fmt.Errorf("stop adapter[%d]: %w", idx, err)
		}
	}
	return nil
}

func (s *Synchronizer) listenForSyncEvents(ctx context.Context) error {
	var err error
	clientId := utils.ClientIdentifierFromContext(ctx)
	// incoming message pool
	s.inPool, err = ants.NewPoolWithFunc(1, func(i interface{}) {
		data, ok := i.([]byte)
		if !ok {
			logger.L().Ctx(ctx).Error("failed to convert message to bytes", helpers.Interface("message", i))
			return
		}

		if len(data) == 0 {
			// connection closed
			return
		}

		// unmarshal message
		var generic domain.Generic
		err := json.Unmarshal(data, &generic)
		if err != nil {
			logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err), helpers.String("target", "domain.Generic"), helpers.String("data", string(data)))
			return
		}
		var kind string
		if generic.Kind != nil {
			kind = generic.Kind.String()
		}
		logger.L().Debug("received message",
			helpers.String("account", clientId.Account),
			helpers.String("cluster", clientId.Cluster),
			helpers.Interface("event", generic.Event.Value()),
			helpers.String("kind", kind),
			helpers.String("msgid", generic.MsgId),
			helpers.Int("depth", generic.Depth))
		// check message depth and ID
		if generic.Depth > maxMessageDepth {
			logger.L().Ctx(ctx).Error("message depth too high",
				helpers.String("account", clientId.Account),
				helpers.String("cluster", clientId.Cluster),
				helpers.Interface("event", generic.Event.Value()),
				helpers.String("kind", kind),
				helpers.String("msgid", generic.MsgId),
				helpers.Int("depth", generic.Depth))
			return
		}

		// store in context
		ctx = utils.ContextFromGeneric(ctx, generic)
		// handle message
		switch *generic.Event {
		case domain.EventBatch:
			var msg domain.Batch
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}

			err := s.handleSyncBatch(ctx, *msg.Kind, domain.BatchType(msg.BatchType), *msg.Items)
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		case domain.EventGetObject:
			var msg domain.GetObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}
			id := domain.KindName{
				Kind:            msg.Kind,
				Name:            msg.Name,
				Namespace:       msg.Namespace,
				ResourceVersion: msg.ResourceVersion,
			}
			err := s.handleSyncGetObject(ctx, id, []byte(msg.BaseObject))
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("id", id.String()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		case domain.EventNewChecksum:
			var msg domain.NewChecksum
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}
			id := domain.KindName{
				Kind:            msg.Kind,
				Name:            msg.Name,
				Namespace:       msg.Namespace,
				ResourceVersion: msg.ResourceVersion,
			}
			err := s.handleSyncNewChecksum(ctx, id, msg.Checksum)
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("id", id.String()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		case domain.EventObjectDeleted:
			var msg domain.ObjectDeleted
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}
			id := domain.KindName{
				Kind:            msg.Kind,
				Name:            msg.Name,
				Namespace:       msg.Namespace,
				ResourceVersion: msg.ResourceVersion,
			}
			err := s.handleSyncObjectDeleted(ctx, id)
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("id", id.String()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		case domain.EventPatchObject:
			var msg domain.PatchObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}
			id := domain.KindName{
				Kind:            msg.Kind,
				Name:            msg.Name,
				Namespace:       msg.Namespace,
				ResourceVersion: msg.ResourceVersion,
			}
			err := s.handleSyncPatchObject(ctx, id, msg.Checksum, []byte(msg.Patch))
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("id", id.String()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		case domain.EventPutObject:
			var msg domain.PutObject
			err = json.Unmarshal(data, &msg)
			if err != nil {
				logger.L().Ctx(ctx).Error("cannot unmarshal message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", generic.Event.Value()),
					helpers.String("kind", generic.Kind.String()),
					helpers.String("msgid", generic.MsgId))
				return
			}
			id := domain.KindName{
				Kind:            msg.Kind,
				Name:            msg.Name,
				Namespace:       msg.Namespace,
				ResourceVersion: msg.ResourceVersion,
			}
			err := s.handleSyncPutObject(ctx, id, msg.Checksum, []byte(msg.Object))
			if err != nil {
				logger.L().Ctx(ctx).Error("error handling message", helpers.Error(err),
					helpers.String("account", clientId.Account),
					helpers.String("cluster", clientId.Cluster),
					helpers.Interface("event", msg.Event.Value()),
					helpers.String("id", id.String()),
					helpers.String("msgid", msg.MsgId))
				return
			}
		}
	})
	if err != nil {
		return fmt.Errorf("unable to create incoming message pool: %w", err)
	}
	// process incoming messages
	for {
		if err := backoff.RetryNotify(func() error {
			data, err := s.readDataFunc(*s.Conn)
			if err != nil {
				// close connection
				_ = (*s.Conn).Close()
				if s.isClient {
					// let sendData() reconnect and return an error to retry
					return fmt.Errorf("cannot read data: %w", err)
				} else {
					return backoff.Permanent(fmt.Errorf("cannot read data: %w", err))
				}
			}
			err = s.inPool.Invoke(data)
			if err != nil {
				return fmt.Errorf("invoke inPool: %w", err)
			}
			return nil
		}, utils.NewBackOff(true), func(err error, d time.Duration) {
			logger.L().Ctx(ctx).Warning("process incoming messages", helpers.Error(err), helpers.String("retry in", d.String()))
		}); err != nil {
			return fmt.Errorf("giving up process incoming messages: %w", err)
		}
	}
}

func (s *Synchronizer) handleSyncBatch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	for _, adapter := range s.adapters {
		err := adapter.Batch(ctx, kind, batchType, items)
		if err != nil {
			return fmt.Errorf("batch: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) handleSyncGetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	for _, adapter := range s.adapters {
		err := adapter.GetObject(ctx, id, baseObject)
		if err != nil {
			return fmt.Errorf("get object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) handleSyncNewChecksum(ctx context.Context, id domain.KindName, newChecksum string) error {
	for _, adapter := range s.adapters {
		err := adapter.VerifyObject(ctx, id, newChecksum)
		if err != nil {
			return fmt.Errorf("verify object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) handleSyncObjectDeleted(ctx context.Context, id domain.KindName) error {
	for _, adapter := range s.adapters {
		err := adapter.DeleteObject(ctx, id)
		if err != nil {
			return fmt.Errorf("delete object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) handleSyncPatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	for _, adapter := range s.adapters {
		err := adapter.PatchObject(ctx, id, checksum, patch)
		if err != nil {
			return fmt.Errorf("patch object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) handleSyncPutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	for _, adapter := range s.adapters {
		err := adapter.PutObject(ctx, id, checksum, object)
		if err != nil {
			return fmt.Errorf("put object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) sendGetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	event := domain.EventGetObject
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	msg := domain.GetObject{
		BaseObject: string(baseObject),
		Depth:      depth + 1,
		Event:      &event,
		Kind:       id.Kind,
		MsgId:      msgId,
		Name:       id.Name,
		Namespace:  id.Namespace,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal get object message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on get object message: %w", err)
	}
	clientId := utils.ClientIdentifierFromContext(ctx)
	logger.L().Debug("sent get object message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("namespace", msg.Namespace),
		helpers.String("name", msg.Name),
		helpers.Int("base object size", len(msg.BaseObject)))
	return nil
}

func (s *Synchronizer) sendNewChecksum(ctx context.Context, id domain.KindName, checksum string) error {
	event := domain.EventNewChecksum
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	msg := domain.NewChecksum{
		Checksum:  checksum,
		Depth:     depth + 1,
		Event:     &event,
		Kind:      id.Kind,
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal checksum message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on checksum message: %w", err)
	}
	if msg.Kind == nil {
		return fmt.Errorf("invalid resource kind. name: %s", msg.Name)
	}
	clientId := utils.ClientIdentifierFromContext(ctx)
	logger.L().Debug("sent new checksum message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("namespace", msg.Namespace),
		helpers.String("name", msg.Name),
		helpers.String("checksum", msg.Checksum))
	return nil
}

func (s *Synchronizer) sendObjectDeleted(ctx context.Context, id domain.KindName) error {
	event := domain.EventObjectDeleted
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	msg := domain.ObjectDeleted{
		Depth:     depth + 1,
		Event:     &event,
		Kind:      id.Kind,
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal delete message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on delete message: %w", err)
	}
	clientId := utils.ClientIdentifierFromContext(ctx)
	logger.L().Debug("sent object deleted message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("namespace", msg.Namespace),
		helpers.String("name", msg.Name))
	return nil
}

func (s *Synchronizer) sendPatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	event := domain.EventPatchObject
	depth, msgId := utils.DeptMsgIdFromContext(ctx)

	msg := domain.PatchObject{
		Checksum:  checksum,
		Depth:     depth + 1,
		Event:     &event,
		Kind:      id.Kind,
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
		Patch:     string(patch),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal patch message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on patch message: %w", err)
	}

	clientId := utils.ClientIdentifierFromContext(ctx)

	logger.L().Debug("sent patch object message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("namespace", msg.Namespace),
		helpers.String("name", msg.Name),
		helpers.String("checksum", msg.Checksum),
		helpers.Int("patch size", len(msg.Patch)))
	return nil
}

func (s *Synchronizer) sendPing(ctx context.Context) {
	event := domain.EventPing
	for {
		msg := domain.Generic{
			Event: &event,
			MsgId: utils.NewMsgId(),
		}
		data, err := json.Marshal(msg)
		if err != nil {
			logger.L().Fatal("marshal ping message", helpers.Error(err))
		}
		err = s.outPool.Invoke(data)
		if err != nil {
			logger.L().Ctx(ctx).Error("invoke outPool on ping message", helpers.Error(err))
		}
		time.Sleep(50 * time.Second)
	}
}

func (s *Synchronizer) sendBatch(ctx context.Context, kind domain.Kind, batchType domain.BatchType, items domain.BatchItems) error {
	event := domain.EventBatch
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	msg := domain.Batch{
		Depth:     depth + 1,
		Event:     &event,
		Kind:      &kind,
		MsgId:     msgId,
		BatchType: string(batchType),
		Items:     &items,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal batch message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on batch message: %w", err)
	}
	clientId := utils.ClientIdentifierFromContext(ctx)
	logger.L().Debug("sent batch message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("batchType", msg.BatchType),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.Int("items", msg.Items.Length()))
	return nil
}

func (s *Synchronizer) sendPutObject(ctx context.Context, id domain.KindName, checksum string, object []byte) error {
	event := domain.EventPutObject
	depth, msgId := utils.DeptMsgIdFromContext(ctx)
	msg := domain.PutObject{
		Checksum:  checksum,
		Depth:     depth + 1,
		Event:     &event,
		Kind:      id.Kind,
		MsgId:     msgId,
		Name:      id.Name,
		Namespace: id.Namespace,
		Object:    string(object),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal put object message: %w", err)
	}
	err = s.outPool.Invoke(data)
	if err != nil {
		return fmt.Errorf("invoke outPool on put object message: %w", err)
	}

	clientId := utils.ClientIdentifierFromContext(ctx)
	logger.L().Debug("sent put object message",
		helpers.String("account", clientId.Account),
		helpers.String("cluster", clientId.Cluster),
		helpers.String("kind", msg.Kind.String()),
		helpers.String("msgid", msg.MsgId),
		helpers.String("namespace", msg.Namespace),
		helpers.String("name", msg.Name),
		helpers.Int("object size", len(msg.Object)))
	return nil
}
