package adapters

import (
	"context"
	"fmt"

	"github.com/armosec/utils-k8s-go/armometadata"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
)

type MockAdapter struct {
	callbacks            domain.Callbacks
	checkResourceVersion bool // false for client, true for server
	patchStrategy        bool // true for client, false for server
	Resources            map[string][]byte
	shadowObjects        map[string][]byte
}

func NewMockAdapter(isClient bool) *MockAdapter {
	return &MockAdapter{
		checkResourceVersion: !isClient,
		patchStrategy:        isClient,
		Resources:            map[string][]byte{},
		shadowObjects:        map[string][]byte{},
	}
}

var _ Adapter = (*MockAdapter)(nil)

func (m *MockAdapter) DeleteObject(_ context.Context, id domain.KindName) error {
	delete(m.Resources, id.String())
	return nil
}

func (m *MockAdapter) GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error {
	object, ok := m.Resources[id.String()]
	if !ok {
		return fmt.Errorf("object not found")
	}
	if m.patchStrategy {
		if len(baseObject) > 0 {
			// update reference object
			m.shadowObjects[id.String()] = baseObject
		}
		if oldObject, ok := m.shadowObjects[id.String()]; ok {
			// calculate checksum
			checksum, err := utils.CanonicalHash(object)
			if err != nil {
				return fmt.Errorf("calculate checksum: %w", err)
			}
			// calculate patch
			patch, err := jsonpatch.CreateMergePatch(oldObject, object)
			if err != nil {
				return fmt.Errorf("create merge patch: %w", err)
			}
			return m.callbacks.PatchObject(ctx, id, checksum, patch)
		} else {
			return m.callbacks.PutObject(ctx, id, object)
		}
	} else {
		return m.callbacks.PutObject(ctx, id, object)
	}
}

func (m *MockAdapter) PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error {
	baseObject, err := m.patchObject(id, checksum, patch)
	if err != nil {
		logger.L().Warning("patch object, sending get object", helpers.Error(err), helpers.String("id", id.String()))
		return m.callbacks.GetObject(ctx, id, baseObject)
	}
	return nil
}

func (m *MockAdapter) patchObject(id domain.KindName, checksum string, patch []byte) ([]byte, error) {
	object, ok := m.Resources[id.String()]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	modified, err := jsonpatch.MergePatch(object, patch)
	if err != nil {
		return object, fmt.Errorf("apply patch: %w", err)
	}
	newChecksum, err := utils.CanonicalHash(modified)
	if err != nil {
		return object, fmt.Errorf("calculate checksum: %w", err)
	}
	if newChecksum != checksum {
		return object, fmt.Errorf("checksum mismatch: %s != %s", newChecksum, checksum)
	}
	m.saveIfNewer(id, modified)
	m.shadowObjects[id.String()] = modified
	return nil, nil
}

func (m *MockAdapter) PutObject(_ context.Context, id domain.KindName, object []byte) error {
	m.saveIfNewer(id, object)
	return nil
}

// saveIfNewer saves the object only if it is newer than the existing one
// this reference implementation should be implemented in the ingester on the backend side
func (m *MockAdapter) saveIfNewer(id domain.KindName, newObject []byte) {
	if m.checkResourceVersion {
		new, err := armometadata.ExtractMetadataFromJsonBytes(newObject)
		if err == nil {
			if oldObject, ok := m.Resources[id.String()]; ok {
				old, err := armometadata.ExtractMetadataFromJsonBytes(oldObject)
				if err == nil {
					if !utils.StringValueBigger(new.ResourceVersion, old.ResourceVersion) {
						return
					}
				}
			}
		}
	}
	m.Resources[id.String()] = newObject
}

func (m *MockAdapter) RegisterCallbacks(_ context.Context, callbacks domain.Callbacks) {
	m.callbacks = callbacks
}

func (m *MockAdapter) Callbacks(_ context.Context) (domain.Callbacks, error) {
	return m.callbacks, nil
}

func (m *MockAdapter) Start(_ context.Context) error {
	return nil
}

func (m *MockAdapter) VerifyObject(ctx context.Context, id domain.KindName, newChecksum string) error {
	baseObject, err := m.verifyObject(id, newChecksum)
	if err != nil {
		logger.L().Warning("verify object, sending get object", helpers.Error(err), helpers.String("id", id.String()))
		return m.callbacks.GetObject(ctx, id, baseObject)
	}
	return nil
}

func (m *MockAdapter) verifyObject(id domain.KindName, newChecksum string) ([]byte, error) {
	object, ok := m.Resources[id.String()]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return nil, fmt.Errorf("calculate checksum: %w", err)
	}
	if checksum != newChecksum {
		return object, fmt.Errorf("checksum mismatch: %s != %s", newChecksum, checksum)
	}
	return object, nil
}

// TestCallDeleteObject is used for testing purposes only, it is similar to incluster.client response to watch.Deleted event
func (m *MockAdapter) TestCallDeleteObject(ctx context.Context, id domain.KindName) error {
	ctx = utils.ContextFromGeneric(ctx, domain.Generic{})
	// delete local object - this is only for testing purposes
	delete(m.Resources, id.String())
	// send delete
	err := m.callbacks.DeleteObject(ctx, id)
	if err != nil {
		return fmt.Errorf("send delete: %w", err)
	}
	if m.patchStrategy {
		// remove from known resources
		delete(m.shadowObjects, id.String())
	}
	return nil
}

// TestCallPutOrPatch is used for testing purposes only, it is similar to incluster.client.callPutOrPatch
func (m *MockAdapter) TestCallPutOrPatch(ctx context.Context, id domain.KindName, baseObject []byte, newObject []byte) error {
	ctx = utils.ContextFromGeneric(ctx, domain.Generic{})
	// store object locally - this is only for testing purposes
	m.Resources[id.String()] = newObject
	// send put/patch
	if m.patchStrategy {
		if len(baseObject) > 0 {
			// update reference object
			m.shadowObjects[id.Name] = baseObject
		}
		if oldObject, ok := m.shadowObjects[id.Name]; ok {
			// calculate checksum
			checksum, err := utils.CanonicalHash(newObject)
			if err != nil {
				return fmt.Errorf("calculate checksum: %w", err)
			}
			// calculate patch
			patch, err := jsonpatch.CreateMergePatch(oldObject, newObject)
			if err != nil {
				return fmt.Errorf("create merge patch: %w", err)
			}
			err = m.callbacks.PatchObject(ctx, id, checksum, patch)
			if err != nil {
				return fmt.Errorf("send patch object: %w", err)
			}
		} else {
			err := m.callbacks.PutObject(ctx, id, newObject)
			if err != nil {
				return fmt.Errorf("send put object: %w", err)
			}
		}
		// add/update known resources
		m.shadowObjects[id.Name] = newObject
	} else {
		err := m.callbacks.PutObject(ctx, id, newObject)
		if err != nil {
			return fmt.Errorf("send put object: %w", err)
		}
	}
	return nil
}

// TestCallVerifyObject is used for testing purposes only, it is similar to incluster.client.callVerifyObject
func (m *MockAdapter) TestCallVerifyObject(ctx context.Context, id domain.KindName, object []byte) error {
	ctx = utils.ContextFromGeneric(ctx, domain.Generic{})
	// store object locally - this is only for testing purposes
	m.Resources[id.String()] = object
	// calculate checksum
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return fmt.Errorf("calculate checksum: %w", err)
	}
	// send verify
	err = m.callbacks.VerifyObject(ctx, id, checksum)
	if err != nil {
		return fmt.Errorf("send checksum: %w", err)
	}
	return nil
}
