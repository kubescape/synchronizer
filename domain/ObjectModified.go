package domain

// ObjectModified represents a ObjectModified model.
type ObjectModified struct {
	Checksum             string
	Account              string
	Cluster              string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Object               string
	AdditionalProperties map[string]interface{}
}
