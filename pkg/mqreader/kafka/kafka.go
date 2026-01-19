// Package kafka provides a Kafka implementation of the MessageQueueReader interface.
// It allows reading messages from Apache Kafka topics without committing offsets.
package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/segmentio/kafka-go"
)

func init() {
	// Register Kafka reader factory when this package is imported
	mqreader.RegisterReader(mqreader.TypeKafka, func(cfg *mqreader.Config) (mqreader.MessageQueueReader, error) {
		return NewKafkaReader(cfg)
	})
}

// KafkaReader implements MessageQueueReader for Apache Kafka
type KafkaReader struct {
	reader        *kafka.Reader
	brokers       []string
	topic         string
	consumerGroup string
	maxMessages   int
	waitTimeout   time.Duration
	connected     bool
	mu            sync.Mutex
}

// NewKafkaReader creates a new Kafka reader from a config
func NewKafkaReader(cfg *mqreader.Config) (*KafkaReader, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if cfg.URL == "" {
		return nil, fmt.Errorf("broker URL is required")
	}

	if cfg.QueueName == "" {
		return nil, fmt.Errorf("topic name is required")
	}

	// Parse brokers from URL (comma-separated)
	brokers := strings.Split(cfg.URL, ",")
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
	}

	maxMsgs := cfg.MaxMessages
	if maxMsgs <= 0 {
		maxMsgs = mqreader.DefaultMaxMessages
	}

	waitTimeout := time.Duration(cfg.WaitTimeSeconds) * time.Second
	if waitTimeout <= 0 {
		waitTimeout = 5 * time.Second // Default 5s timeout for Kafka
	}

	// Extract consumer group from options
	consumerGroup := ""
	if cfg.Options != nil {
		consumerGroup = cfg.Options["group_id"]
	}

	return &KafkaReader{
		brokers:       brokers,
		topic:         cfg.QueueName,
		consumerGroup: consumerGroup,
		maxMessages:   maxMsgs,
		waitTimeout:   waitTimeout,
	}, nil
}

// Connect establishes connection to Kafka
func (r *KafkaReader) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.connected {
		return nil
	}

	// Generate unique consumer group for peek to avoid affecting real consumers
	// Use a special prefix to identify peek readers
	peekGroupID := r.consumerGroup
	if peekGroupID == "" {
		peekGroupID = fmt.Sprintf("dataql-peek-%s-%d", r.topic, time.Now().UnixNano())
	} else {
		// Append peek suffix to avoid affecting real consumer group
		peekGroupID = fmt.Sprintf("%s-dataql-peek", peekGroupID)
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        r.brokers,
		Topic:          r.topic,
		GroupID:        peekGroupID,
		MinBytes:       1,    // Fetch immediately
		MaxBytes:       10e6, // 10MB max
		MaxWait:        r.waitTimeout,
		StartOffset:    kafka.FirstOffset, // Start from beginning for peek
		CommitInterval: 0,                 // CRITICAL: Disable auto-commit
	}

	r.reader = kafka.NewReader(readerConfig)
	r.connected = true
	return nil
}

// Peek reads messages without consuming them (no offset commits)
func (r *KafkaReader) Peek(ctx context.Context, maxMessages int) ([]mqreader.Message, error) {
	if !r.connected {
		if err := r.Connect(ctx); err != nil {
			return nil, err
		}
	}

	if maxMessages <= 0 {
		maxMessages = r.maxMessages
	}

	var messages []mqreader.Message
	seenIDs := make(map[string]bool) // partition:offset deduplication

	// Create a timeout context for the peek operation
	peekCtx, cancel := context.WithTimeout(ctx, r.waitTimeout)
	defer cancel()

	for len(messages) < maxMessages {
		// FetchMessage does NOT commit - ReadMessage would commit
		msg, err := r.reader.FetchMessage(peekCtx)
		if err != nil {
			if err == context.DeadlineExceeded || err == context.Canceled {
				// Timeout reached, return what we have
				break
			}
			// Check for other errors that indicate no more messages
			if strings.Contains(err.Error(), "context") {
				break
			}
			return nil, fmt.Errorf("failed to fetch message: %w", err)
		}

		// Generate unique ID: partition:offset
		msgID := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)

		// Skip duplicates
		if seenIDs[msgID] {
			continue
		}
		seenIDs[msgID] = true

		// Convert to generic message format
		messages = append(messages, convertKafkaMessage(msg, r.topic))
	}

	return messages, nil
}

// GetMetadata returns information about the topic
func (r *KafkaReader) GetMetadata(ctx context.Context) (*mqreader.QueueMetadata, error) {
	if !r.connected {
		if err := r.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Connect to get topic metadata
	conn, err := kafka.Dial("tcp", r.brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Get partition information
	partitions, err := conn.ReadPartitions(r.topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions: %w", err)
	}

	metadata := &mqreader.QueueMetadata{
		Name:           r.topic,
		Type:           mqreader.TypeKafka,
		AdditionalInfo: make(map[string]string),
	}

	// Calculate approximate message count from partition offsets
	var totalMessages int64
	for _, p := range partitions {
		leaderAddr := fmt.Sprintf("%s:%d", p.Leader.Host, p.Leader.Port)
		leaderConn, err := kafka.DialLeader(ctx, "tcp", leaderAddr, r.topic, p.ID)
		if err != nil {
			continue
		}

		// Get first and last offsets
		first, last, err := leaderConn.ReadOffsets()
		if err == nil && last > first {
			totalMessages += last - first
		}
		leaderConn.Close()
	}

	metadata.ApproxMsgCount = totalMessages
	metadata.AdditionalInfo["partitions"] = strconv.Itoa(len(partitions))
	metadata.AdditionalInfo["brokers"] = strings.Join(r.brokers, ",")

	if r.consumerGroup != "" {
		metadata.AdditionalInfo["consumer_group"] = r.consumerGroup
	}

	return metadata, nil
}

// Close releases resources
func (r *KafkaReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		r.connected = false
		return err
	}

	r.connected = false
	return nil
}

// convertKafkaMessage converts a Kafka message to the generic Message type
func convertKafkaMessage(msg kafka.Message, topic string) mqreader.Message {
	message := mqreader.Message{
		ID:        fmt.Sprintf("%d:%d", msg.Partition, msg.Offset),
		Body:      string(msg.Value),
		Source:    topic,
		Timestamp: msg.Time,
		Metadata:  make(map[string]string),
	}

	// Store partition and offset
	message.Metadata["partition"] = strconv.Itoa(msg.Partition)
	message.Metadata["offset"] = strconv.FormatInt(msg.Offset, 10)

	// Store key if present
	if len(msg.Key) > 0 {
		message.Metadata["key"] = string(msg.Key)
	}

	// Store headers
	for _, h := range msg.Headers {
		message.Metadata["header_"+h.Key] = string(h.Value)
	}

	// Store high watermark if available
	if msg.HighWaterMark > 0 {
		message.Metadata["high_watermark"] = strconv.FormatInt(msg.HighWaterMark, 10)
	}

	return message
}
