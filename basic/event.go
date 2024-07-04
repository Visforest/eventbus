package basic

import (
	"encoding/json"
)

type Event struct {
	ID       string            `json:"id"`        // uniq identifier of the event
	CreateAt int64             `json:"create_at"` // the unix timestamp when the event happens
	ExpireAt int64             `json:"expire_at"` // the unix timestamp before event becomes invalid
	Topic    string            `json:"topic"`     // the topic that the event is associated with
	OrderKey string            `json:"-"`
	Meta     map[string]string `json:"meta"`    // optional meta data
	Payload  []byte            `json:"payload"` // the encoded payload data
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
