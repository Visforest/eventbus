package basic

import (
	"encoding/json"
)

type Event struct {
	ID       string            // uniq identifier of the event
	CreateAt int64             // the unix timestamp when the event happens
	ExpireAt int64             // the unix timestamp before event becomes invalid
	Topic    string            // the topic that the event is associated with
	Meta     map[string]string // optional meta data
	Payload  []byte            // the encoded payload data
}

// Load parse the event payload into an object.
func (e *Event) Load(v any) error {
	return json.Unmarshal(e.Payload, v)
}

func (e *Event) Marshal() []byte {
	bytes, _ := json.Marshal(e)
	return bytes
}

// EventHandler is the function that handles event, if returns non-nil error, event msg will be rollback
type EventHandler func(event Event) error
