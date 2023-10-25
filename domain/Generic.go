package domain

// Generic represents a Generic model.
type Generic struct {
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	AdditionalProperties map[string]interface{}
}
