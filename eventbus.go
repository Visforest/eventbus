package eventbus

import (
	"github.com/visforest/eventbus/basic"
	"time"
)

var (
	defaultIdGenerator = basic.UUID{}
)

type NotifyOpt func(event *basic.Event)

// WithCreatedAt sets event msg create time, default now
func WithCreatedAt(t time.Time) NotifyOpt {
	return func(event *basic.Event) {
		event.CreateAt = t.Unix()
	}
}

// WithExpireDuration sets expire duration on event msg
func WithExpireDuration(d time.Duration) NotifyOpt {
	return func(event *basic.Event) {
		event.ExpireAt = time.Now().Add(d).Unix()
	}
}

// WithMeta attaches meta data on event msg
func WithMeta(field, value string) NotifyOpt {
	return func(event *basic.Event) {
		event.Meta[field] = value
	}
}

// WithOrderKey specifies a key to ensure that the event msgs with same key are consumed in original sequence
func WithOrderKey(key string) NotifyOpt {
	return func(event *basic.Event) {
		event.OrderKey = key
	}
}

type EventBusOpt func(eventbus Eventbus)

func WithLogger(logger basic.Logger) EventBusOpt {
	return func(eventbus Eventbus) {
		eventbus.SetLogger(logger)
	}
}

func WithIdGenerator(generator basic.Generator) EventBusOpt {
	return func(eventbus Eventbus) {
		eventbus.SetIdGenerator(generator)
	}
}

type Eventbus interface {
	SetLogger(logger basic.Logger)
	SetIdGenerator(generator basic.Generator)
	Register(topic string, handler basic.EventHandler) error
	Unregister(topic string, handler basic.EventHandler) error
	Registered() ([]string, error)
	Notify(topic string, data any, opt ...NotifyOpt) error
	// Listen is to listen to another eventbus notifies, only for cross-host eventbus
	Listen()
}
