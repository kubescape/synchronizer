package domain

// NewChecksum represents a NewChecksum model.
type NewChecksum struct {
	Checksum             string
	Cluster              string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	AdditionalProperties map[string]interface{}
}
