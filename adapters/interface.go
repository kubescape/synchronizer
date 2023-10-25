package adapters

import (
	"context"

	"github.com/kubescape/synchronizer/domain"
)

type Adapter interface {
	DeleteObject(ctx context.Context, id domain.ClusterKindName) error
	GetObject(ctx context.Context, id domain.ClusterKindName, baseObject []byte) error
	PatchObject(ctx context.Context, id domain.ClusterKindName, checksum string, patch []byte) error
	PutObject(ctx context.Context, id domain.ClusterKindName, object []byte) error
	RegisterCallbacks(callbacks domain.Callbacks)
	Start() error
	VerifyObject(ctx context.Context, id domain.ClusterKindName, checksum string) error
}
