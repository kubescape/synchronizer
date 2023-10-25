package domain

import "context"

type Callbacks struct {
	GetObject     func(ctx context.Context, id ClusterKindName, baseObject []byte) error
	ObjectAdded   func(ctx context.Context, id ClusterKindName, object []byte) error
	ObjectDeleted func(ctx context.Context, id ClusterKindName) error
	PatchObject   func(ctx context.Context, id ClusterKindName, checksum string, patch []byte) error
	PutObject     func(ctx context.Context, id ClusterKindName, object []byte) error
}
