package core

import (
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
		GetObject:     s.GetObjectCallback,
		ObjectAdded:   s.ObjectAddedCallback,
		ObjectDeleted: s.ObjectDeletedCallback,
		PatchObject:   s.PatchObjectCallback,
		PutObject:     s.PutObjectCallback,
	}
	adapter.RegisterCallbacks(callbacks)
	return s
}

func (s *Synchronizer) GetObjectCallback(id domain.ClusterKindName, baseObject []byte) error {
	if s.isClient {
		baseObject = nil
	}
	err := s.sendGetObject(id, baseObject)
	if err != nil {
		return fmt.Errorf("send get object: %w", err)
	}
	return nil
}

func (s *Synchronizer) ObjectAddedCallback(id domain.ClusterKindName, object []byte) error {
	if s.isClient {
		// calculate checksum
		checksum, err := utils.CanonicalHash(object)
		if err != nil {
			return fmt.Errorf("calculate checksum: %w", err)
		}
		err = s.sendNewChecksum(id, checksum)
		if err != nil {
			return fmt.Errorf("send checksum: %w", err)
		}
	} else {
		err := s.sendPutObject(id, object)
		if err != nil {
			return fmt.Errorf("send put object: %w", err)
		}
	}
	return nil
}

func (s *Synchronizer) ObjectDeletedCallback(id domain.ClusterKindName) error {
	err := s.sendObjectDeleted(id)
	if err != nil {
		return fmt.Errorf("send delete: %w", err)
	}
	return nil
}

func (s *Synchronizer) PatchObjectCallback(id domain.ClusterKindName, checksum string, patch []byte) error {
	err := s.sendPatchObject(id, checksum, patch)
	if err != nil {
		return fmt.Errorf("send patch: %w", err)
	}
	return nil
}

func (s *Synchronizer) PutObjectCallback(id domain.ClusterKindName, object []byte) error {
	err := s.sendPutObject(id, object)
	if err != nil {
		return fmt.Errorf("send put object: %w", err)
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
		logger.L().Info("received message", helpers.Interface("event", generic.Event.Value()))
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
			err := s.handleSyncGetObject(id, []byte(msg.BaseObject))
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
			err := s.handleSyncNewChecksum(id, msg.Checksum)
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
			err := s.handleSyncObjectDeleted(id)
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
			err := s.handleSyncPatchObject(id, msg.Checksum, []byte(msg.Patch))
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
			err := s.handleSyncPutObject(id, []byte(msg.Object))
			if err != nil {
				logger.L().Error("error handling message", helpers.Error(err),
					helpers.Interface("event", generic.Event.Value()))
				continue
			}
		}
	}
}

func (s *Synchronizer) handleSyncGetObject(id domain.ClusterKindName, baseObject []byte) error {
	err := s.adapter.GetObject(id, baseObject)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncNewChecksum(id domain.ClusterKindName, newChecksum string) error {
	err := s.adapter.VerifyObject(id, newChecksum)
	if err != nil {
		return fmt.Errorf("verify object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncObjectDeleted(id domain.ClusterKindName) error {
	err := s.adapter.DeleteObject(id)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncPatchObject(id domain.ClusterKindName, checksum string, patch []byte) error {
	err := s.adapter.PatchObject(id, checksum, patch)
	if err != nil {
		return fmt.Errorf("patch object: %w", err)
	}
	return nil
}

func (s *Synchronizer) handleSyncPutObject(id domain.ClusterKindName, object []byte) error {
	err := s.adapter.PutObject(id, object)
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *Synchronizer) sendGetObject(id domain.ClusterKindName, baseObject []byte) error {
	event := domain.EventGetObject
	msg := domain.GetObject{
		Event:      &event,
		Cluster:    id.Cluster,
		Kind:       id.Kind,
		Name:       id.Name,
		BaseObject: string(baseObject),
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

func (s *Synchronizer) sendNewChecksum(id domain.ClusterKindName, checksum string) error {
	event := domain.EventNewChecksum
	msg := domain.NewChecksum{
		Event:    &event,
		Cluster:  id.Cluster,
		Kind:     id.Kind,
		Name:     id.Name,
		Checksum: checksum,
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

func (s *Synchronizer) sendObjectDeleted(id domain.ClusterKindName) error {
	event := domain.EventObjectDeleted
	msg := domain.ObjectDeleted{
		Event:   &event,
		Cluster: id.Cluster,
		Kind:    id.Kind,
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

func (s *Synchronizer) sendPatchObject(id domain.ClusterKindName, checksum string, patch []byte) error {
	event := domain.EventPatchObject
	msg := domain.PatchObject{
		Event:    &event,
		Cluster:  id.Cluster,
		Kind:     id.Kind,
		Name:     id.Name,
		Checksum: checksum,
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

func (s *Synchronizer) sendPutObject(id domain.ClusterKindName, object []byte) error {
	event := domain.EventPutObject
	msg := domain.PutObject{
		Event:   &event,
		Cluster: id.Cluster,
		Kind:    id.Kind,
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
