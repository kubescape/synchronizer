package domain

// GetObject represents a GetObject model.
type GetObject struct {
	BaseObject           string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	AdditionalProperties map[string]interface{}
}
