package config

type KafkaBrokerConfig struct {
	Endpoints       []string // test connect addresses
	TopicPartitions int      // default number of new topic
	TopicPrefix     string
}
