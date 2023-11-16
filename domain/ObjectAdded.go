package domain

// ObjectAdded represents a ObjectAdded model.
type ObjectAdded struct {
	Checksum             string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	Object               string
	AdditionalProperties map[string]interface{}
}
