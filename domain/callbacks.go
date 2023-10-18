package domain

type Callbacks struct {
	GetObject     func(id ClusterKindName, baseObject []byte) error
	ObjectAdded   func(id ClusterKindName, object []byte) error
	ObjectDeleted func(id ClusterKindName) error
	PatchObject   func(id ClusterKindName, checksum string, patch []byte) error
	PutObject     func(id ClusterKindName, object []byte) error
}
