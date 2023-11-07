package domain

import "context"

type Callbacks struct {
	DeleteObject func(ctx context.Context, id KindName) error
	GetObject    func(ctx context.Context, id KindName, baseObject []byte) error
	PatchObject  func(ctx context.Context, id KindName, checksum string, patch []byte) error
	PutObject    func(ctx context.Context, id KindName, object []byte) error
	VerifyObject func(ctx context.Context, id KindName, checksum string) error
}
