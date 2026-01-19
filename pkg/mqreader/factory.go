package mqreader

import (
	"fmt"
)

// ReaderFactory is a function type that creates a MessageQueueReader from a Config
type ReaderFactory func(config *Config) (MessageQueueReader, error)

// registry holds registered reader factories
var registry = make(map[string]ReaderFactory)

// RegisterReader registers a reader factory for a message queue type.
// This allows new backends to be added without modifying this package.
func RegisterReader(mqType string, factory ReaderFactory) {
	registry[mqType] = factory
}

// NewReader creates a new MessageQueueReader based on the config type.
// It first checks the registry for a registered factory, then falls back
// to built-in support for known types.
func NewReader(config *Config) (MessageQueueReader, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Check registry first
	if factory, ok := registry[config.Type]; ok {
		return factory(config)
	}

	// Return appropriate error for known but not yet implemented types
	switch config.Type {
	case TypeSQS:
		// SQS should be registered by the sqs package
		return nil, fmt.Errorf("SQS reader not registered. Import github.com/adrianolaselva/dataql/pkg/mqreader/sqs")
	case TypeKafka:
		// Kafka should be registered by the kafka package
		return nil, fmt.Errorf("Kafka reader not registered. Import github.com/adrianolaselva/dataql/pkg/mqreader/kafka")
	case TypeRabbitMQ:
		return nil, fmt.Errorf("rabbitmq support coming soon")
	case TypePulsar:
		return nil, fmt.Errorf("pulsar support coming soon")
	case TypePubSub:
		return nil, fmt.Errorf("google pub/sub support coming soon")
	default:
		return nil, fmt.Errorf("unsupported message queue type: %s", config.Type)
	}
}

// NewReaderFromURL creates a MessageQueueReader from a URL string.
// This is a convenience function that combines ParseURL and NewReader.
func NewReaderFromURL(urlStr string) (MessageQueueReader, error) {
	config, err := ParseURL(urlStr)
	if err != nil {
		return nil, err
	}
	return NewReader(config)
}
