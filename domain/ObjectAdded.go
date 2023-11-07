package domain

// ObjectAdded represents a ObjectAdded model.
type ObjectAdded struct {
	Checksum string
	// Account              string
	// Cluster              string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Object               string
	AdditionalProperties map[string]interface{}
}
