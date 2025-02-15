package basic

import (
	"encoding/json"
)

type Event struct {
	ID            string            `json:"id"`                  // uniq identifier of the event
	CreateAt      int64             `json:"create_at"`           // the unix timestamp when the event happens
	ExpireAt      int64             `json:"expire_at,omitempty"` // the unix timestamp before event becomes invalid
	Topic         string            `json:"topic"`               // the topic that the event is associated with and stored
	OriginalTopic string            `json:"original_topic"`      // the user defined topic
	OrderKey      string            `json:"-"`                   // the key used for test broker to ensure event msg is processed sequentially
	Meta          map[string]string `json:"meta"`                // optional meta data
	Payload       any               `json:"payload"`             // the encoded payload data
}

func (e *Event) Marshal() []byte {
	bytes, _ := json.Marshal(e)
	return bytes
}

// EventHandler is an object that handles event
type EventHandler interface {
	Name() string
	OnEvent(event Event) error
}
