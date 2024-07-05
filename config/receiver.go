package config

type ReceiverConfig struct {
	BrokerType  BrokerType // broker type
	TopicPrefix string
	KafkaConfig *KafkaBrokerConfig // config for kafka broker
}
