package domain

// ObjectAdded represents a ObjectAdded model.
type ObjectAdded struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Checksum             string
	Object               string
	AdditionalProperties map[string]interface{}
}
