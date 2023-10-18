package domain

// PatchObject represents a PatchObject model.
type PatchObject struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Checksum             string
	Patch                string
	AdditionalProperties map[string]interface{}
}
