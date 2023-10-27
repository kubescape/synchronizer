package domain

import "context"

type Callbacks struct {
	DeleteObject func(ctx context.Context, id ClusterKindName) error
	GetObject    func(ctx context.Context, id ClusterKindName, baseObject []byte) error
	PatchObject  func(ctx context.Context, id ClusterKindName, checksum string, patch []byte) error
	PutObject    func(ctx context.Context, id ClusterKindName, object []byte) error
	VerifyObject func(ctx context.Context, id ClusterKindName, checksum string) error
}
