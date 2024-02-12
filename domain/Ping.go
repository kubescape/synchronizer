package domain

// Ping represents a Ping model.
type Ping struct {
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	AppInfo              *AppInfo
	AdditionalProperties map[string]interface{}
}
