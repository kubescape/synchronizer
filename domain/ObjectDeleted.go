package domain

// ObjectDeleted represents a ObjectDeleted model.
type ObjectDeleted struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	AdditionalProperties map[string]interface{}
}
