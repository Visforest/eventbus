package config

type BrokerType string

const (
	BrokerKafka BrokerType = "kafka"
)

type KafkaBrokerConfig struct {
	Endpoints []string // kafka connect addresses
	Prefix    string   // topic prefix
}
