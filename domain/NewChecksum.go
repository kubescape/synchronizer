package domain

// NewChecksum represents a NewChecksum model.
type NewChecksum struct {
	ResourceVersion      int
	Checksum             string
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	AdditionalProperties map[string]interface{}
}
