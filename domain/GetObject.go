package domain

// GetObject represents a GetObject model.
type GetObject struct {
	BaseObject           string
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	AdditionalProperties map[string]interface{}
}
