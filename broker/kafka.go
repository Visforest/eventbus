package broker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/visforest/eventbus/basic"
	"github.com/visforest/eventbus/config"
	"github.com/visforest/goset/v2"
	"sync"
)

type topicHandler struct {
	handlers *goset.Set[basic.EventHandler]
	sub      *subscriber
}

func (h *topicHandler) Next(ctx context.Context) *kafka.Generation {
	generation, err := h.sub.cg.Next(ctx)
	if err == nil {
		return generation
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, kafka.ErrGroupClosed) {
		return nil
	} else {
		// close consumer group
		h.sub.m.RLock()
		closed := h.sub.closed
		h.sub.m.RUnlock()
		if !closed {
			if h.sub.logger != nil {
				h.sub.logger.Infof("[eventbus] recreate consumer group, as unexpected consumer error %v", err)
			}
			if err = h.sub.Close(); err != nil {
				if h.sub.logger != nil {
					h.sub.logger.Errorf("[eventbus] consumer group failed to close: %+v", err)
				}
			}
			h.sub.newGroup(ctx)
		}
	}
	return nil
}

type KafkaBroker struct {
	lock        sync.RWMutex
	cfg         *config.KafkaBrokerConfig
	conn        *kafka.Conn
	writer      *kafka.Writer
	handlers    map[string]*topicHandler
	wroteTopics map[string]struct{}
	logger      basic.Logger
	connected   bool
}

func NewKafkaBroker(cfg *config.KafkaBrokerConfig) (*KafkaBroker, error) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Endpoints...),
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		Compression:            kafka.Gzip,
		AllowAutoTopicCreation: true,
	}
	conn, err := kafka.Dial("tcp", cfg.Endpoints[0])
	if err != nil {
		return nil, err
	}

	return &KafkaBroker{
		cfg:         cfg,
		writer:      writer,
		handlers:    make(map[string]*topicHandler),
		wroteTopics: map[string]struct{}{},
		connected:   false,
		conn:        conn,
	}, nil
}

func (b *KafkaBroker) createTopicIfNotExist(topic string) error {
	_, err := b.conn.ReadPartitions(topic)
	if err == kafka.UnknownTopicOrPartition {
		if b.logger != nil {
			b.logger.Warnf("[test broker] topic %s doesn't exist, create it", topic)
		}
		// topic doesn't exist, create one
		err = b.conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     b.cfg.TopicPartitions,
			ReplicationFactor: 3,
		})
	} else if err != nil {
		if b.logger != nil {
			b.logger.Errorf("[test broker] read %s partition err:%s", topic, err.Error())
		}
	}
	return err
}

func (b *KafkaBroker) SetLogger(logger basic.Logger) {
	b.logger = logger
	b.writer.Logger = logger
	b.writer.ErrorLogger = logger
}

func (b *KafkaBroker) Addrs() []string {
	return b.cfg.Endpoints
}

func (b *KafkaBroker) Connect() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.connected {
		return nil
	}

	for topic := range b.handlers {
		if err := b.createTopicIfNotExist(topic); err != nil {
			return err
		}
	}
	b.connected = true
	return nil
}

func (b *KafkaBroker) Disconnect() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if !b.connected {
		return nil
	}

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
			err = h.sub.Close()
			if err != nil {
				return err
			}
		}
	}
	b.connected = false
	return nil
}

func (b *KafkaBroker) Subscribe(topic string, handler basic.EventHandler) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if th, ok := b.handlers[topic]; ok {
		th.handlers.Add(handler)
	} else {
		var err error
		cgID := basic.UUID{}.New()
		cgCfg := kafka.ConsumerGroupConfig{
			ID:                    cgID,
			Brokers:               b.cfg.Endpoints,
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

		b.handlers[topic] = &topicHandler{
			handlers: goset.NewSet[basic.EventHandler](handler),
			sub: &subscriber{
				cgCfg:  cgCfg,
				cg:     cg,
				logger: b.logger,
			},
		}
	}
	return nil
}

func (b *KafkaBroker) Unsubscribe(topic string, handler basic.EventHandler) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if h, ok := b.handlers[topic]; ok {
		h.handlers.Delete(handler)
		if b.logger != nil {
			b.logger.Infof("[test broker] unsubscribed handler %s on %s", handler.Name(), topic)
		}
		if h.handlers.Length() == 0 {
			if err := h.sub.cg.Close(); err != nil {
				return err
			}
			delete(b.handlers, topic)
		}
	}
	return nil
}

// Subscribed returns subscribed topics
func (b *KafkaBroker) Subscribed() []string {
	b.lock.RLock()
	defer b.lock.RUnlock()
	var topics = make([]string, 0, len(b.handlers))
	for t := range b.handlers {
		topics = append(topics, t)
	}
	return topics
}

// writes event msg to msg queue
func (b *KafkaBroker) Write(ctx context.Context, e basic.Event) error {
	if _, ok := b.wroteTopics[e.Topic]; !ok {
		if err := b.createTopicIfNotExist(e.Topic); err != nil {
			return err
		}
		b.wroteTopics[e.Topic] = struct{}{}
	}
	if b.logger != nil {
		b.logger.Debugf("[test broker] write msg with id %s to topic:%s", e.ID, e.Topic)
	}

	message := kafka.Message{
		Topic: e.Topic,
		Value: e.Marshal(),
	}
	if e.OrderKey != "" {
		message.Key = []byte(e.OrderKey)
	}
	return b.writer.WriteMessages(ctx, message)
}

func (b *KafkaBroker) Consume(ctx context.Context) {
	for topic, tHandler := range b.handlers {
		go func(t string, h *topicHandler) {
			for {
				select {
				case <-ctx.Done():
					if b.logger != nil {
						b.logger.Debugf("[test broker] consume, ctx done, %s stop consume!", t)
					}
					return
				default:
					generation := h.Next(ctx)
					if generation == nil {
						continue
					}
					for _, t := range h.sub.cgCfg.Topics {
						assignments := generation.Assignments[t]
						for _, assignment := range assignments {
							rCfg := kafka.ReaderConfig{
								Brokers:     b.cfg.Endpoints,
								GroupID:     "",
								Topic:       t,
								Partition:   assignment.ID,
								Logger:      b.logger,
								ErrorLogger: b.logger,
							}
							if cgh, err := newCgHandler(rCfg, generation, assignment.Offset, h.handlers, b.logger); err == nil {
								generation.Start(cgh.run)
							} else {
								if b.logger != nil {
									b.logger.Errorf("[test broker] newCgHandler error:%s", err.Error())
								}
							}
						}
					}
				}
			}
		}(topic, tHandler)
	}
}

type subscriber struct {
	cg     *kafka.ConsumerGroup
	cgCfg  kafka.ConsumerGroupConfig
	logger basic.Logger
	closed bool
	m      sync.RWMutex
}

func (s *subscriber) Close() error {
	if err := s.cg.Close(); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (s *subscriber) newGroup(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			cg, err := kafka.NewConsumerGroup(s.cgCfg)
			if err != nil {
				if s.logger != nil {
					s.logger.Errorf("[test subscriber] create consumer group failed:%+v", err)
				}
				continue
			}
			s.m.Lock()
			s.cg = cg
			if s.logger != nil {
				s.logger.Infof("[test subscriber] recreated consumer group:%s", s.cgCfg.ID)
			}
			s.m.Unlock()
			return
		}
	}
}

type cgHandler struct {
	generation *kafka.Generation
	reader     *kafka.Reader
	handlers   *goset.Set[basic.EventHandler]
	logger     basic.Logger
}

func newCgHandler(cfg kafka.ReaderConfig, generation *kafka.Generation, offset int64, handlers *goset.Set[basic.EventHandler], logger basic.Logger) (*cgHandler, error) {
	reader := kafka.NewReader(cfg)
	if err := reader.SetOffset(offset); err != nil {
		return nil, err
	}
	return &cgHandler{
		generation: generation,
		reader:     reader,
		handlers:   handlers,
		logger:     logger,
	}, nil
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
			if err == nil {
				// read msg successfully
				offsets[msg.Topic][msg.Partition] = msg.Offset
				var e basic.Event
				if err := json.Unmarshal(msg.Value, &e); err != nil {
					if h.logger != nil {
						h.logger.Errorf("[test consumer group] failed to unmarshal event msg:%s, err:%+v", string(msg.Value), err)
					}
					continue
				}
				for _, hd := range h.handlers.ToList() {
					if err := hd.OnEvent(e); err != nil {
						if h.logger != nil {
							h.logger.Errorf("[test consumer group] failed to handle event msg:%+v, err:%s", e, err.Error())
						}
					}
				}
				if err := h.generation.CommitOffsets(offsets); err != nil {
					if h.logger != nil {
						h.logger.Errorf("[test consumer group] failed to commit offset, err:%+v", err)
					}
				}
			} else if errors.Is(err, kafka.ErrGenerationEnded) {
				if h.logger != nil {
					h.logger.Infof("[test consumer group] generation ended")
				}
				return
			} else {
				// other error
				if h.logger != nil {
					h.logger.Errorf("[test consumer group] failed to read event msg, unexpected err:%s", err.Error())
				}
			}
		}
	}
}
