package domain

// GetObject represents a GetObject model.
type GetObject struct {
	ResourceVersion      int
	BaseObject           string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	AdditionalProperties map[string]interface{}
}
