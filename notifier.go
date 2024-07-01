package eventbus

import (
	"encoding/json"
	"fmt"
	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/broker"
	"github.com/visforest/eventbus/config"
	"github.com/visforest/eventbus/id"
	"time"
)

var (
	defaultIdGenerator = id.UUID{}
)

type NotifyOpt func(event *basic.Event)

func WithCreatedAt(t time.Time) NotifyOpt {
	return func(event *basic.Event) {
		event.CreateAt = t.Unix()
	}
}
func WithExpireDuration(d time.Duration) NotifyOpt {
	return func(event *basic.Event) {
		event.ExpireAt = time.Now().Add(d).Unix()
	}
}

func WithMeta(field, value string) NotifyOpt {
	return func(event *basic.Event) {
		event.Meta[field] = value
	}
}

type Notifier struct {
	idGenerator id.Generator
	broker      broker.Broker
	version     string
	logger      basic.Logger
}

func NewNotifier(cfg config.NotifierConfig, logger basic.Logger) (*Notifier, error) {
	var b broker.Broker
	var err error
	switch cfg.BrokerType {
	case config.BrokerKafka:
		b, err = broker.NewKafkaBroker(cfg.KafkaConfig, logger)
	default:
		err = fmt.Errorf("unsupported broker type:%s", cfg.BrokerType)
	}
	if err != nil {
		return nil, err
	}
	return &Notifier{idGenerator: defaultIdGenerator, broker: b, logger: logger}, nil
}

func (n *Notifier) SetIdGenerator(generator id.Generator) {
	n.idGenerator = generator
}

func (n *Notifier) Notify(topic string, data any, opts ...NotifyOpt) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	event := basic.Event{
		ID:       n.idGenerator.New(),
		CreateAt: time.Now().Unix(),
		Topic:    topic,
		Payload:  payload,
		Meta:     make(map[string]string),
	}
	for _, opt := range opts {
		opt(&event)
	}
	event.Meta["User-Agent"] = fmt.Sprintf("eventbus:%s", Version)
	n.logger.Debugf("[Notifier] ready to notify topic: %s with %+v", topic, event)
	return n.broker.Write(event)
}
