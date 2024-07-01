package config

type ReceiverConfig struct {
	BrokerType  BrokerType         // broker type
	KafkaConfig *KafkaBrokerConfig // config for kafka broker
}
