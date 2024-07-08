package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/broker"
	"github.com/visforest/eventbus/config"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type KafkaBus struct {
	m           sync.RWMutex
	cfg         *config.KafkaBrokerConfig
	idGenerator basic.Generator
	broker      *broker.KafkaBroker
	version     string
	logger      basic.Logger
}

func NewKafkaBus(cfg *config.KafkaBrokerConfig, logger basic.Logger) (*KafkaBus, error) {
	b, err := broker.NewKafkaBroker(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &KafkaBus{cfg: cfg, idGenerator: defaultIdGenerator, broker: b, logger: logger}, nil
}

func (b *KafkaBus) formatTopic(topic string) string {
	if strings.HasSuffix(topic, ".") {
		topic = strings.TrimRight(topic, ".")
	}
	return fmt.Sprintf("%s.%s", b.cfg.TopicPrefix, topic)
}

func (b *KafkaBus) SetLogger(logger basic.Logger) {
	b.logger = logger
}

func (b *KafkaBus) SetIdGenerator(generator basic.Generator) {
	b.idGenerator = generator
}

func (b *KafkaBus) Register(topic string, handler basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	topic = b.formatTopic(topic)
	return b.broker.Subscribe(topic, handler)
}

func (b *KafkaBus) Unregister(topic string, _ basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	topic = b.formatTopic(topic)
	return b.broker.Unsubscribe(topic)
}

func (b *KafkaBus) Registered() ([]string, error) {
	topics := b.broker.Subscribed()
	return topics, nil
}

func (b *KafkaBus) Notify(topic string, data any, opts ...NotifyOpt) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	event := basic.Event{
		ID:       b.idGenerator.New(),
		CreateAt: time.Now().Unix(),
		Topic:    b.formatTopic(topic),
		Payload:  payload,
		Meta:     make(map[string]string),
	}
	for _, opt := range opts {
		opt(&event)
	}
	event.Meta["User-Agent"] = fmt.Sprintf("eventbus_%s.kafka", Version)
	if b.logger != nil {
		b.logger.Debugf("[eventbus] ready to notify topic: %s with %+v", topic, event)
	}
	return b.broker.Write(context.Background(), event)
}

func (b *KafkaBus) Listen() {
	if err := b.broker.Connect(); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.broker.Consume(ctx)
	// catch exit
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)
	s := <-quit
	if b.logger != nil {
		b.logger.Errorf("[eventbus] quit, received signal %s", s.String())
	}
	cancel()
	err := b.broker.Disconnect()
	if err != nil {
		b.logger.Errorf("[eventbus] failed to close, err:%s", err.Error())
		return
	}
	if b.logger != nil {
		b.logger.Infof("[eventbus] shutdown!")
	}
}
