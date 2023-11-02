package domain

// PatchObject represents a PatchObject model.
type PatchObject struct {
	Checksum             string
	Account              string
	Cluster              string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Patch                string
	AdditionalProperties map[string]interface{}
}
