package eventbus

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/broker"
	"github.com/visforest/eventbus/config"
)

// Receiver is an object that consumes event msgs from subscribed topics and handles
type Receiver struct {
	cfg    *config.ReceiverConfig
	broker broker.Broker
	logger basic.Logger
}

func NewReceiver(cfg *config.ReceiverConfig, logger basic.Logger) (*Receiver, error) {
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

	return &Receiver{
		cfg:    cfg,
		broker: b,
		logger: logger,
	}, nil
}

func (r *Receiver) formatTopic(topic string) string {
	if strings.HasSuffix(topic, ".") {
		topic = strings.TrimRight(topic, ".")
	}
	return fmt.Sprintf("%s.%s", r.cfg.TopicPrefix, topic)
}

func (r *Receiver) RegisterHandler(topic string, handler basic.EventHandler) error {
	topic = r.formatTopic(topic)
	return r.broker.Subscribe(topic, handler)
}

func (r *Receiver) UnregisterHandler(topic string) error {
	topic = r.formatTopic(topic)
	return r.broker.Unsubscribe(topic)
}

// Close gracefully closes when server shutdown
func (r *Receiver) Close() error {
	return r.broker.Disconnect()
}

// Listen loops to read event from broker, and handle it
func (r *Receiver) Listen() {
	if err := r.broker.Connect(); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.broker.Consume(ctx)
	// catch exit
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)
	s := <-quit
	r.logger.Errorf("[Receiver] quit, received signal %s", s.String())
	cancel()
	err := r.Close()
	if err != nil {
		r.logger.Errorf("[Receiver] failed to close, err:%+v", err)
		return
	}
	r.logger.Infof("[Receiver] shutdown")
}
