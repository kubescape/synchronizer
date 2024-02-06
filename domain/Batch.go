package domain

// Batch represents a Batch model.
type Batch struct {
	Depth                int
	Event                *Event
	MsgId                string
	Kind                 *Kind
	BatchType            string
	Items                *BatchItems
	AdditionalProperties map[string]interface{}
}
