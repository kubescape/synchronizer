package domain

// PutObject represents a PutObject model.
type PutObject struct {
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Object               string
	AdditionalProperties map[string]interface{}
}
