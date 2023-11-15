package domain

// ObjectModified represents a ObjectModified model.
type ObjectModified struct {
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
