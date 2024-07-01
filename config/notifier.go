package config

type NotifierConfig struct {
	BrokerType  BrokerType         // broker type
	KafkaConfig *KafkaBrokerConfig // config for kafka broker
}
