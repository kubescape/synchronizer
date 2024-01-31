package adapters

import (
	"context"

	"github.com/kubescape/synchronizer/domain"
)

type Adapter interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	IsRelated(ctx context.Context, id domain.ClientIdentifier) bool
	RegisterCallbacks(ctx context.Context, callbacks domain.Callbacks)
	Callbacks(ctx context.Context) (domain.Callbacks, error) // returns the callbacks for the given context
	DeleteObject(ctx context.Context, id domain.KindName) error
	GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error
	PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error
	PutObject(ctx context.Context, id domain.KindName, object []byte) error
	VerifyObject(ctx context.Context, id domain.KindName, checksum string) error
	Batch(ctx context.Context, batchType domain.BatchType, messages []domain.BatchItem) error
}

type Client Adapter
