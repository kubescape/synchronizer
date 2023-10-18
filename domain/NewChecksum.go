package domain

// NewChecksum represents a NewChecksum model.
type NewChecksum struct {
	Event                *Event
	Cluster              string
	Kind                 *Kind
	Name                 string
	Checksum             string
	AdditionalProperties map[string]interface{}
}
