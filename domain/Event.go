package domain

// Event represents an enum of Event.
type Event uint

const (
	EventNewChecksum Event = iota
	EventObjectAdded
	EventObjectDeleted
	EventObjectModified
	EventGetObject
	EventPatchObject
	EventPutObject
	EventPing
	EventBatch
)

// Value returns the value of the enum.
func (op Event) Value() any {
	if op >= Event(len(EventValues)) {
		return nil
	}
	return EventValues[op]
}

var EventValues = []any{"newChecksum", "objectAdded", "objectDeleted", "objectModified", "getObject", "patchObject", "putObject", "ping", "batch"}
var ValuesToEvent = map[any]Event{
	EventValues[EventNewChecksum]:    EventNewChecksum,
	EventValues[EventObjectAdded]:    EventObjectAdded,
	EventValues[EventObjectDeleted]:  EventObjectDeleted,
	EventValues[EventObjectModified]: EventObjectModified,
	EventValues[EventGetObject]:      EventGetObject,
	EventValues[EventPatchObject]:    EventPatchObject,
	EventValues[EventPutObject]:      EventPutObject,
	EventValues[EventPing]:           EventPing,
	EventValues[EventBatch]:          EventBatch,
}
