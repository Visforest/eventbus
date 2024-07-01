package eventbus

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/broker"
	"github.com/visforest/eventbus/config"
)

// Receiver is an object that consumes event msgs from subscribed topics and handles
type Receiver struct {
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
		broker: b,
		logger: logger,
	}, nil
}

func (r *Receiver) RegisterHandler(topic string, handler basic.EventHandler) error {
	return r.broker.Subscribe(topic, handler)
}

func (r *Receiver) UnregisterHandler(topic string) error {
	return r.broker.Unsubscribe(topic)
}

func (r *Receiver) Topics() []string {
	return r.broker.Subscribed()
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
	r.broker.Consume()
	// catch exit
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, os.Kill)
	s := <-quit
	switch s {
	case os.Interrupt:
		r.logger.Errorf("[Receiver] is interrupted")
	case os.Kill:
		r.logger.Errorf("[Receiver] is killed")
	}
	err := r.Close()
	if err != nil {
		r.logger.Errorf("[Receiver] failed to close, err:%+v", err)
		return
	}
	r.logger.Infof("[Receiver] shutdown")
}
