package domain

import "context"

type Callbacks struct {
	// BackendAvailable reports whether outbound synchronization can accept HTTP traffic.
	BackendAvailable func() bool
	DeleteObject     func(ctx context.Context, id KindName) error
	GetObject        func(ctx context.Context, id KindName, baseObject []byte) error
	PatchObject      func(ctx context.Context, id KindName, checksum string, patch []byte) error
	PutObject        func(ctx context.Context, id KindName, checksum string, object []byte) error
	VerifyObject     func(ctx context.Context, id KindName, checksum string) error
	Batch            func(ctx context.Context, kind Kind, batchType BatchType, items BatchItems) error
}
