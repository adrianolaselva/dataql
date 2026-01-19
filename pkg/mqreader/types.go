// Package mqreader provides interfaces and types for reading messages from
// various message queue systems without consuming them.
package mqreader

import (
	"context"
	"time"
)

// Message represents a generic message from any message queue system.
// The fields are designed to be common across different systems while
// allowing system-specific data in the Metadata field.
type Message struct {
	// ID is the unique identifier for the message
	ID string

	// Body is the message content (typically JSON or plain text)
	Body string

	// Timestamp is when the message was sent/published
	Timestamp time.Time

	// Metadata contains system-specific attributes
	// For SQS: MessageAttributes, MD5OfBody, ReceiptHandle, etc.
	// For Kafka: Headers, Partition, Offset, etc.
	// For RabbitMQ: Headers, Exchange, RoutingKey, etc.
	Metadata map[string]string

	// Source identifies the queue/topic/exchange the message came from
	Source string

	// ReceiveCount indicates how many times this message was received (if applicable)
	ReceiveCount int
}

// QueueMetadata contains information about a queue/topic.
type QueueMetadata struct {
	// Name is the queue/topic name
	Name string

	// ApproxMsgCount is the approximate number of messages in the queue
	ApproxMsgCount int64

	// Type identifies the message queue system (sqs, kafka, rabbitmq, etc.)
	Type string

	// AdditionalInfo contains system-specific metadata
	AdditionalInfo map[string]string
}

// MessageQueueReader is the interface for reading messages from a message queue
// without consuming/deleting them. Implementations should use techniques like
// visibility timeout=0 (SQS) or consumer groups with no commit (Kafka) to
// achieve peek functionality.
type MessageQueueReader interface {
	// Connect establishes connection to the message queue system
	Connect(ctx context.Context) error

	// Peek reads messages without removing/consuming them.
	// The maxMessages parameter limits the number of messages to retrieve.
	// Returns a slice of messages and any error encountered.
	Peek(ctx context.Context, maxMessages int) ([]Message, error)

	// GetMetadata returns information about the queue/topic
	GetMetadata(ctx context.Context) (*QueueMetadata, error)

	// Close terminates the connection and releases resources
	Close() error
}

// Config holds generic configuration for connecting to a message queue.
type Config struct {
	// Type identifies the message queue system (sqs, kafka, rabbitmq, pulsar)
	Type string

	// URL is the connection URL or resource identifier
	URL string

	// Region is the cloud region (for cloud providers like AWS, GCP)
	Region string

	// QueueName is the name of the queue/topic
	QueueName string

	// MaxMessages is the maximum number of messages to retrieve per Peek call
	MaxMessages int

	// WaitTimeSeconds is the long polling wait time (for systems that support it)
	WaitTimeSeconds int

	// Credentials contains authentication information
	Credentials map[string]string

	// Options contains additional system-specific options
	Options map[string]string
}

// Supported message queue types
const (
	TypeSQS      = "sqs"
	TypeKafka    = "kafka"
	TypeRabbitMQ = "rabbitmq"
	TypePulsar   = "pulsar"
	TypePubSub   = "pubsub"
)

// Default configuration values
const (
	DefaultMaxMessages       = 10
	DefaultWaitTimeSeconds   = 0
	MaxMessagesPerSQSRequest = 10
)
