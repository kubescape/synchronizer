package domain

// Exit represents a Exit model.
type Exit struct {
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Reason               string
	StatusCode           int
	AdditionalProperties map[string]interface{}
}
