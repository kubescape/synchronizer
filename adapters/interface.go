package adapters

import (
	"context"

	"github.com/kubescape/synchronizer/domain"
)

type Adapter interface {
	Start(ctx context.Context) error
	RegisterCallbacks(ctx context.Context, callbacks domain.Callbacks)
	DeleteObject(ctx context.Context, id domain.KindName) error
	GetObject(ctx context.Context, id domain.KindName, baseObject []byte) error
	PatchObject(ctx context.Context, id domain.KindName, checksum string, patch []byte) error
	PutObject(ctx context.Context, id domain.KindName, object []byte) error
	VerifyObject(ctx context.Context, id domain.KindName, checksum string) error
}

type Client Adapter
