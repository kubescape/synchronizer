package domain

// PutObject represents a PutObject model.
type PutObject struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Object               string
	AdditionalProperties map[string]interface{}
}
