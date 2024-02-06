package domain

// BatchItems represents a BatchItems model.
type BatchItems struct {
	PutObject            []PutObject
	NewChecksum          []NewChecksum
	ObjectDeleted        []ObjectDeleted
	GetObject            []GetObject
	PatchObject          []PatchObject
	AdditionalProperties map[string]interface{}
}
