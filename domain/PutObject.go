package domain

// PutObject represents a PutObject model.
type PutObject struct {
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
