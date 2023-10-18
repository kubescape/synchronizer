package adapters

import (
	"fmt"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"
	"github.com/kubescape/synchronizer/domain"
	"github.com/kubescape/synchronizer/utils"
)

type MockAdapter struct {
	callbacks     domain.Callbacks
	patchStrategy bool
	resources     map[string][]byte
	shadowObjects map[string][]byte
}

func NewMockAdapter() *MockAdapter {
	return &MockAdapter{
		resources:     map[string][]byte{},
		shadowObjects: map[string][]byte{},
	}
}

var _ Adapter = (*MockAdapter)(nil)

func (m *MockAdapter) DeleteObject(id domain.ClusterKindName) error {
	delete(m.resources, id.String())
	return nil
}

func (m *MockAdapter) GetObject(id domain.ClusterKindName, baseObject []byte) error {
	object, ok := m.resources[id.String()]
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
			return m.callbacks.PatchObject(id, checksum, patch)
		} else {
			return m.callbacks.PutObject(id, object)
		}
	} else {
		return m.callbacks.PutObject(id, object)
	}
}

func (m *MockAdapter) PatchObject(id domain.ClusterKindName, checksum string, patch []byte) error {
	baseObject, err := m.patchObject(id, checksum, patch)
	if err != nil {
		logger.L().Warning("patch object, sending get object", helpers.Error(err))
		return m.callbacks.GetObject(id, baseObject)
	}
	return nil
}

func (m *MockAdapter) patchObject(id domain.ClusterKindName, checksum string, patch []byte) ([]byte, error) {
	object, ok := m.resources[id.String()]
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
	m.resources[id.String()] = modified
	m.shadowObjects[id.String()] = modified
	return object, nil
}

func (m *MockAdapter) PutObject(id domain.ClusterKindName, object []byte) error {
	m.resources[id.String()] = object
	return nil
}

func (m *MockAdapter) RegisterCallbacks(callbacks domain.Callbacks) {
	m.callbacks = callbacks
}

func (m *MockAdapter) Start() error {
	return nil
}

func (m *MockAdapter) VerifyObject(id domain.ClusterKindName, newChecksum string) error {
	baseObject, err := m.verifyObject(id, newChecksum)
	if err != nil {
		logger.L().Warning("verify object, sending get object", helpers.Error(err))
		return m.callbacks.GetObject(id, baseObject)
	}
	return nil
}

func (m *MockAdapter) verifyObject(id domain.ClusterKindName, newChecksum string) ([]byte, error) {
	object, ok := m.resources[id.String()]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	checksum, err := utils.CanonicalHash(object)
	if err != nil {
		return object, fmt.Errorf("calculate checksum: %w", err)
	}
	if checksum != newChecksum {
		return object, fmt.Errorf("checksum mismatch: %s != %s", newChecksum, checksum)
	}
	return object, nil
}
