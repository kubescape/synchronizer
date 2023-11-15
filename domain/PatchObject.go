package domain

// PatchObject represents a PatchObject model.
type PatchObject struct {
	Checksum             string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	Patch                string
	AdditionalProperties map[string]interface{}
}
