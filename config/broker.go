package config

type KafkaBrokerConfig struct {
	Endpoints       []string // kafka connect addresses
	TopicPartitions int      // default number of new topic
	TopicPrefix     string
}
