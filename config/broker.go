package config

type BrokerType string

const (
	BrokerKafka BrokerType = "kafka"
)

type KafkaBrokerConfig struct {
	Endpoints       []string // kafka connect addresses
	TopicPartitions int      // default number of new topic
}
