package config

type NotifierConfig struct {
	BrokerType  BrokerType         // broker type
	TopicPrefix string             // topic prefix
	KafkaConfig *KafkaBrokerConfig // config for kafka broker
}
