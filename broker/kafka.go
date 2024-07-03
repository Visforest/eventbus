package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/config"
	"github.com/visforest/eventbus/id"
)

type topicHandler struct {
	cgID    string
	handler basic.EventHandler
	sub     *subscriber
}

func (h *topicHandler) Next(ctx context.Context) *kafka.Generation {
	h.sub.lock.RLock()
	cg := h.sub.cg
	h.sub.lock.RUnlock()
	generation, err := cg.Next(ctx)
	switch err {
	case nil:
		// do nothing
	case kafka.ErrGroupClosed:
		// consumer group is already closed
		h.sub.lock.RLock()
		closed := h.sub.closed
		h.sub.lock.RUnlock()
		if !closed {
			if err := cg.Close(); err != nil {
				h.sub.logger.Errorf("[Broker] subscriber close failed:%+v", err)
			}
			h.sub.newGroup(ctx)
		}
	default:
		h.sub.lock.RLock()
		closed := h.sub.closed
		h.sub.lock.RUnlock()
		if !closed {
			h.sub.logger.Infof("[Broker] recreate consumer group, as unexpected consumer error %v", err)
		}
		if err = cg.Close(); err != nil {
			h.sub.logger.Errorf("[Broker] consumer group failed to close: %+v", err)
		}
		h.sub.newGroup(ctx)
	}
	return generation
}

type KafkaBroker struct {
	lock      sync.RWMutex
	ctx       context.Context
	endpoints []string
	conn      *kafka.Conn
	writer    *kafka.Writer
	handlers  map[string]*topicHandler
	logger    basic.Logger
	connected bool
}

func NewKafkaBroker(cfg *config.KafkaBrokerConfig, logger basic.Logger) (*KafkaBroker, error) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Endpoints...),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		Compression:            kafka.Gzip,
		Logger:                 logger,
		ErrorLogger:            logger,
		AllowAutoTopicCreation: true,
	}
	return &KafkaBroker{
		ctx:       context.Background(),
		endpoints: cfg.Endpoints,
		writer:    writer,
		handlers:  make(map[string]*topicHandler),
		logger:    logger,
		connected: false,
	}, nil
}

func (b *KafkaBroker) Addrs() []string {
	return b.endpoints
}

func (b *KafkaBroker) Connect() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	var err error
	if !b.connected {
		b.conn, err = kafka.Dial("tcp", b.endpoints[0])
	}
	if err == nil {
		b.connected = true
		for topic, _ := range b.handlers {
			var partitions []kafka.Partition
			partitions, err = b.conn.ReadPartitions(topic)
			fmt.Println(partitions)
			if err == kafka.InvalidTopic {
				b.logger.Warnf("topic %s doesn't exist, create it", topic)
				// topic doesn't exist, create one
				return b.conn.CreateTopics(kafka.TopicConfig{
					Topic:             topic,
					NumPartitions:     1,
					ReplicationFactor: 3,
				})
			} else if err != nil {
				b.logger.Errorf("read %s partition err:%s", topic, err.Error())
			}
		}
	}
	return err
}

func (b *KafkaBroker) Disconnect() error {
	b.lock.RLock()
	if !b.connected {
		b.lock.RUnlock()
		return nil
	}
	b.lock.RUnlock()

	b.lock.Lock()

	var err = b.conn.Close()
	if err != nil {
		return err
	}
	err = b.writer.Close()
	if err != nil {
		return err
	}
	if len(b.handlers) > 0 {
		for _, h := range b.handlers {
			err = h.sub.cg.Close()
			if err != nil {
				return err
			}
		}
	}
	b.connected = false
	b.lock.Unlock()
	return nil
}

func (b *KafkaBroker) Subscribe(topic string, handler basic.EventHandler) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	var err error
	cgID := id.UUID{}.New()
	cgCfg := kafka.ConsumerGroupConfig{
		ID:                    cgID,
		Brokers:               b.endpoints,
		Topics:                []string{topic},
		GroupBalancers:        []kafka.GroupBalancer{kafka.RangeGroupBalancer{}},
		WatchPartitionChanges: true,
		StartOffset:           kafka.LastOffset,
		Logger:                b.logger,
		ErrorLogger:           b.logger,
	}
	if err = cgCfg.Validate(); err != nil {
		return err
	}
	// create consumer group
	var cg *kafka.ConsumerGroup
	cg, err = kafka.NewConsumerGroup(cgCfg)
	if err != nil {
		return err
	}

	// create subscriber
	sub := &subscriber{
		topic:  topic,
		cgCfg:  &cgCfg,
		cg:     cg,
		logger: b.logger,
	}
	b.handlers[topic] = &topicHandler{
		cgID:    cgID,
		handler: handler,
		sub:     sub,
	}
	return err
}

func (b *KafkaBroker) Unsubscribe(topic string) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if h, ok := b.handlers[topic]; ok {
		if err := h.sub.cg.Close(); err != nil {
			return err
		}
		delete(b.handlers, topic)
	}
	return nil
}

// Subscribed returns subscribed topics
func (b *KafkaBroker) Subscribed() []string {
	b.lock.RLock()
	defer b.lock.RUnlock()
	var topics = make([]string, 0, len(b.handlers))
	for t, _ := range b.handlers {
		topics = append(topics, t)
	}
	return topics
}

func (b *KafkaBroker) Write(e basic.Event) error {
	b.logger.Debugf("[Broker] write msg with id %s to topic:%s", e.ID, e.Topic)
	return b.writer.WriteMessages(context.Background(), kafka.Message{
		Topic: e.Topic,
		Value: e.Marshal(),
	})
}

func (b *KafkaBroker) Consume() {
	for topic, tHandler := range b.handlers {
		go func(t string, h *topicHandler) {
			for {
				select {
				case <-b.ctx.Done():
					return
				default:
					generation := h.Next(b.ctx)
					if generation == nil {
						continue
					}
					for _, t := range h.sub.cgCfg.Topics {
						assignments := generation.Assignments[t]
						for _, assignment := range assignments {
							rCfg := kafka.ReaderConfig{
								Brokers:     b.endpoints,
								GroupID:     "",
								Topic:       t,
								Partition:   assignment.ID,
								Logger:      b.logger,
								ErrorLogger: b.logger,
							}
							cgh := newCgHandler(rCfg, generation, assignment.Offset, h.handler, b.logger)
							generation.Start(cgh.run)
						}
					}
				}
			}
		}(topic, tHandler)
	}
}

type subscriber struct {
	topic     string
	gen       *kafka.Generation
	offset    int64
	partition int64
	handler   basic.EventHandler
	reader    *kafka.Reader
	done      chan struct{}
	cg        *kafka.ConsumerGroup
	cgCfg     *kafka.ConsumerGroupConfig
	logger    basic.Logger
	closed    bool
	lock      sync.RWMutex
}

func (s *subscriber) Unsubscribe() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return nil
	}
	var err error
	if s.cg != nil {
		err = s.cg.Close()
		s.closed = true
	}
	return err
}

func (s *subscriber) newGroup(ctx context.Context) {
	s.lock.RLock()
	cgCfg := s.cgCfg
	s.lock.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			cg, err := kafka.NewConsumerGroup(*cgCfg)
			if err != nil {
				s.logger.Errorf("[Subscriber] create consumer group failed:%+v", err)
				continue
			}
			s.lock.Lock()
			s.cg = cg
			s.logger.Infof("[Subscriber] recreated consumer group:%s", cgCfg.ID)
			s.lock.Unlock()
			return
		}
	}
}

type cgHandler struct {
	topic      string
	generation *kafka.Generation
	reader     *kafka.Reader
	handler    basic.EventHandler
	logger     basic.Logger
}

func newCgHandler(cfg kafka.ReaderConfig, generation *kafka.Generation, offset int64, handler basic.EventHandler, logger basic.Logger) *cgHandler {
	reader := kafka.NewReader(cfg)
	reader.SetOffset(offset)
	return &cgHandler{
		generation: generation,
		reader:     reader,
		handler:    handler,
		logger:     logger,
	}
}

func (h *cgHandler) run(ctx context.Context) {
	offsets := map[string]map[int]int64{
		h.reader.Config().Topic: make(map[int]int64),
	}

	defer h.reader.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// read message
			msg, err := h.reader.ReadMessage(ctx)
			switch err {
			case kafka.ErrGenerationEnded:
				h.logger.Infof("[Consumer] generation ended")
				return
			case nil:
				// read msg successfully
				offsets[msg.Topic][msg.Partition] = msg.Offset
				var e basic.Event
				if err := json.Unmarshal(msg.Value, &e); err != nil {
					h.logger.Errorf("[Consumer] failed to unmarshal event msg:%s, err:%+v", string(msg.Value), err)
					continue
				}
				if err := h.handler(e); err != nil {
					h.logger.Errorf("[Consumer] failed to handle event msg:%+v, err:%+v", e, err)
				}
				if err := h.generation.CommitOffsets(offsets); err != nil {
					h.logger.Errorf("[Consumer] failed to commit offset, err:%+v", err)
				}
			default:
				// other error
				h.logger.Errorf("[Consumer] failed to read event msg, unexpected err:%+v", err)
			}
		}
	}
}
