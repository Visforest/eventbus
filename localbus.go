package eventbus

import (
	"fmt"
	"github.com/visforest/eventbus/basic"
	"github.com/visforest/goset/v2"
	"sync"
	"time"
)

// LocalBus is a bus that only works in one process
type LocalBus struct {
	m           sync.RWMutex
	handlers    map[string]*goset.Set[basic.EventHandler]
	idGenerator basic.Generator
	logger      basic.Logger
}

func NewLocalBus(opts ...EventBusOpt) Eventbus {
	bus := LocalBus{
		handlers:    make(map[string]*goset.Set[basic.EventHandler]),
		idGenerator: defaultIdGenerator,
	}

	for _, opt := range opts {
		opt(&bus)
	}
	return &bus
}

func (b *LocalBus) SetLogger(logger basic.Logger) {
	b.logger = logger
}

func (b *LocalBus) SetIdGenerator(generator basic.Generator) {
	b.idGenerator = generator
}

func (b *LocalBus) Register(topic string, handlers ...basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	if s, ok := b.handlers[topic]; ok {
		s.Add(handlers...)
	} else {
		b.handlers[topic] = goset.NewSet[basic.EventHandler](handlers...)
	}
	if b.logger != nil {
		var names = make([]string, len(handlers))
		for _, h := range handlers {
			names = append(names, h.Name())
		}
		b.logger.Debugf("registered handler %v on topic:%s", names, topic)
	}
	return nil
}

func (b *LocalBus) Unregister(topic string, handler basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	b.handlers[topic].Delete(handler)
	if b.logger != nil {
		b.logger.Debugf("unregistered handler '%s' on topic:%s", handler.Name(), topic)
	}
	return nil
}

func (b *LocalBus) Registered() ([]string, error) {
	b.m.RLock()
	defer b.m.RUnlock()

	var topics = make([]string, 0, len(b.handlers))
	for t := range b.handlers {
		topics = append(topics, t)
	}
	return topics, nil
}

func (b *LocalBus) Notify(topic string, data any, opts ...NotifyOpt) error {
	b.m.RLock()
	handlers := b.handlers[topic]
	b.m.RUnlock()
	if handlers.Length() == 0 {
		return fmt.Errorf("no handlers registerd on topic:%s", topic)
	}
	event := basic.Event{
		ID:       b.idGenerator.New(),
		CreateAt: time.Now().Unix(),
		Topic:    topic,
		Payload:  data,
		Meta:     make(map[string]string),
	}
	for _, opt := range opts {
		opt(&event)
	}
	event.Meta["User-Agent"] = fmt.Sprintf("eventbus_%s.local", Version)
	for _, h := range handlers.ToList() {
		go func(hd basic.EventHandler) {
			if b.logger != nil {
				b.logger.Debugf("%s handling event msg %s", hd.Name(), event.ID)
			}
			if err := hd.OnEvent(event); err != nil {
				if b.logger != nil {
					b.logger.Errorf("%s handle event msg %s err:%s", hd.Name(), event.ID, err.Error())
				}
			}
		}(h)
	}
	return nil
}

func (b *LocalBus) Listen() {}
