package domain

// Batch represents a Batch model.
type Batch struct {
	Depth                int
	Event                *Event
	MsgId                string
	BatchType            string
	Messages             []BatchItem
	AdditionalProperties map[string]interface{}
}
