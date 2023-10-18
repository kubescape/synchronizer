package domain

// ObjectModified represents a ObjectModified model.
type ObjectModified struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Checksum             string
	Object               string
	AdditionalProperties map[string]interface{}
}
