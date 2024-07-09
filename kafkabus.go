package eventbus

import (
	"context"
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

func NewKafkaBus(cfg *config.KafkaBrokerConfig) (*KafkaBus, error) {
	b, err := broker.NewKafkaBroker(cfg)
	if err != nil {
		return nil, err
	}
	return &KafkaBus{cfg: cfg, idGenerator: defaultIdGenerator, broker: b}, nil
}

func (b *KafkaBus) formatTopic(topic string) string {
	if strings.HasSuffix(topic, ".") {
		topic = strings.TrimRight(topic, ".")
	}
	return fmt.Sprintf("%s.%s", b.cfg.TopicPrefix, topic)
}

func (b *KafkaBus) SetLogger(logger basic.Logger) {
	b.logger = logger
	b.broker.SetLogger(logger)
}

func (b *KafkaBus) SetIdGenerator(generator basic.Generator) {
	b.idGenerator = generator
}

func (b *KafkaBus) Register(topic string, handlers ...basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	topic = b.formatTopic(topic)
	var err error
	for _, handler := range handlers {
		err = b.broker.Subscribe(topic, handler)
		if err != nil {
			return err
		}
		if b.logger != nil {
			b.logger.Debugf("[eventbus] registered handler %s on %s", handler.Name(), topic)
		}
	}
	return nil
}

func (b *KafkaBus) Unregister(topic string, handler basic.EventHandler) error {
	b.m.Lock()
	defer b.m.Unlock()

	topic = b.formatTopic(topic)
	err := b.broker.Unsubscribe(topic, handler)
	if err == nil {
		if b.logger != nil {
			b.logger.Debugf("[eventbus] unregistered handler %v on topic:%s", handler.Name(), topic)
		}
	}
	return err
}

func (b *KafkaBus) Registered() ([]string, error) {
	topics := b.broker.Subscribed()
	return topics, nil
}

func (b *KafkaBus) Notify(topic string, data any, opts ...NotifyOpt) error {
	event := basic.Event{
		ID:            b.idGenerator.New(),
		CreateAt:      time.Now().Unix(),
		Topic:         b.formatTopic(topic),
		OriginalTopic: topic,
		Payload:       data,
		Meta:          make(map[string]string),
	}
	for _, opt := range opts {
		opt(&event)
	}
	event.Meta["User-Agent"] = fmt.Sprintf("eventbus_%s.test", Version)
	if event.ExpireAt > 0 && event.ExpireAt <= time.Now().Unix() {
		if b.logger != nil {
			b.logger.Warnf("[event] event msg is expired before written, msg:%+v", event)
		}
		return nil
	}
	if b.logger != nil {
		b.logger.Debugf("[eventbus] ready to notify event %+v on topic %s", event, topic)
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
		b.logger.Errorf("[eventbus] listen quit, received signal %s", s.String())
	}
	cancel()
	if err := b.broker.Disconnect(); err != nil {
		if b.logger != nil {
			b.logger.Errorf("[eventbus] failed to close, err:%s", err.Error())
		}
		return
	}
	if b.logger != nil {
		b.logger.Infof("[eventbus] shutdown!")
	}
}
