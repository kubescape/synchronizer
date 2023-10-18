package domain

// GetObject represents a GetObject model.
type GetObject struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Object               string
	AdditionalProperties map[string]interface{}
}
