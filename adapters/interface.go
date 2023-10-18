package adapters

import "github.com/kubescape/synchronizer/domain"

type Adapter interface {
	DeleteObject(id domain.ClusterKindName) error
	GetObject(id domain.ClusterKindName, baseObject []byte) error
	PatchObject(id domain.ClusterKindName, checksum string, patch []byte) error
	PutObject(id domain.ClusterKindName, object []byte) error
	RegisterCallbacks(callbacks domain.Callbacks)
	Start() error
	VerifyObject(id domain.ClusterKindName, checksum string) error
}
