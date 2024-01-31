package domain

// BatchItem represents a BatchItem model.
type BatchItem struct {
	Event                *Event
	Payload              string
	AdditionalProperties map[string]interface{}
}
