package domain

// ObjectDeleted represents a ObjectDeleted model.
type ObjectDeleted struct {
	ResourceVersion      int
	Depth                int
	Event                *Event
	Kind                 *Kind
	MsgId                string
	Name                 string
	Namespace            string
	AdditionalProperties map[string]interface{}
}
