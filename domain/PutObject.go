package domain

// PutObject represents a PutObject model.
type PutObject struct {
	ResourceVersion      int
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	Object               string
	AdditionalProperties map[string]interface{}
}
