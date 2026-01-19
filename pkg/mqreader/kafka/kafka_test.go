package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/segmentio/kafka-go"
)

func TestNewKafkaReader(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *mqreader.Config
		wantErr bool
	}{
		{
			name:    "Nil config",
			cfg:     nil,
			wantErr: true,
		},
		{
			name: "Missing broker URL",
			cfg: &mqreader.Config{
				Type:      mqreader.TypeKafka,
				QueueName: "test-topic",
			},
			wantErr: true,
		},
		{
			name: "Missing topic",
			cfg: &mqreader.Config{
				Type: mqreader.TypeKafka,
				URL:  "localhost:9092",
			},
			wantErr: true,
		},
		{
			name: "Valid config",
			cfg: &mqreader.Config{
				Type:      mqreader.TypeKafka,
				URL:       "localhost:9092",
				QueueName: "test-topic",
				Options:   map[string]string{"group_id": "test-group"},
			},
			wantErr: false,
		},
		{
			name: "Multiple brokers",
			cfg: &mqreader.Config{
				Type:      mqreader.TypeKafka,
				URL:       "broker1:9092,broker2:9092,broker3:9092",
				QueueName: "test-topic",
			},
			wantErr: false,
		},
		{
			name: "Valid config without options",
			cfg: &mqreader.Config{
				Type:      mqreader.TypeKafka,
				URL:       "localhost:9092",
				QueueName: "test-topic",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewKafkaReader(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewKafkaReader() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("NewKafkaReader() unexpected error: %v", err)
				return
			}
			if reader == nil {
				t.Errorf("NewKafkaReader() returned nil reader")
			}
		})
	}
}

func TestKafkaReader_BrokerParsing(t *testing.T) {
	cfg := &mqreader.Config{
		Type:      mqreader.TypeKafka,
		URL:       "broker1:9092, broker2:9092 , broker3:9092",
		QueueName: "test-topic",
	}

	reader, err := NewKafkaReader(cfg)
	if err != nil {
		t.Fatalf("NewKafkaReader() error: %v", err)
	}

	if len(reader.brokers) != 3 {
		t.Errorf("Expected 3 brokers, got %d", len(reader.brokers))
	}

	// Check trimming
	for _, b := range reader.brokers {
		if b != strings.TrimSpace(b) {
			t.Errorf("Broker not trimmed: %q", b)
		}
	}

	expectedBrokers := []string{"broker1:9092", "broker2:9092", "broker3:9092"}
	for i, expected := range expectedBrokers {
		if reader.brokers[i] != expected {
			t.Errorf("Broker[%d] = %q, want %q", i, reader.brokers[i], expected)
		}
	}
}

func TestKafkaReader_DefaultValues(t *testing.T) {
	cfg := &mqreader.Config{
		Type:      mqreader.TypeKafka,
		URL:       "localhost:9092",
		QueueName: "test-topic",
	}

	reader, err := NewKafkaReader(cfg)
	if err != nil {
		t.Fatalf("NewKafkaReader() error: %v", err)
	}

	if reader.maxMessages != mqreader.DefaultMaxMessages {
		t.Errorf("maxMessages = %d, want %d", reader.maxMessages, mqreader.DefaultMaxMessages)
	}

	if reader.waitTimeout != 5*time.Second {
		t.Errorf("waitTimeout = %v, want 5s", reader.waitTimeout)
	}

	if reader.consumerGroup != "" {
		t.Errorf("consumerGroup = %q, want empty string", reader.consumerGroup)
	}
}

func TestKafkaReader_CustomValues(t *testing.T) {
	cfg := &mqreader.Config{
		Type:            mqreader.TypeKafka,
		URL:             "localhost:9092",
		QueueName:       "test-topic",
		MaxMessages:     50,
		WaitTimeSeconds: 10,
		Options:         map[string]string{"group_id": "my-consumer-group"},
	}

	reader, err := NewKafkaReader(cfg)
	if err != nil {
		t.Fatalf("NewKafkaReader() error: %v", err)
	}

	if reader.maxMessages != 50 {
		t.Errorf("maxMessages = %d, want 50", reader.maxMessages)
	}

	if reader.waitTimeout != 10*time.Second {
		t.Errorf("waitTimeout = %v, want 10s", reader.waitTimeout)
	}

	if reader.consumerGroup != "my-consumer-group" {
		t.Errorf("consumerGroup = %q, want %q", reader.consumerGroup, "my-consumer-group")
	}
}

func TestConvertKafkaMessage(t *testing.T) {
	now := time.Now()
	msg := kafka.Message{
		Partition:     2,
		Offset:        100,
		Key:           []byte("test-key"),
		Value:         []byte(`{"event":"test"}`),
		Time:          now,
		HighWaterMark: 150,
		Headers: []kafka.Header{
			{Key: "correlation-id", Value: []byte("abc123")},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}

	result := convertKafkaMessage(msg, "test-topic")

	// Check ID format
	if result.ID != "2:100" {
		t.Errorf("ID = %q, want %q", result.ID, "2:100")
	}

	// Check body
	if result.Body != `{"event":"test"}` {
		t.Errorf("Body = %q, want %q", result.Body, `{"event":"test"}`)
	}

	// Check source
	if result.Source != "test-topic" {
		t.Errorf("Source = %q, want %q", result.Source, "test-topic")
	}

	// Check timestamp
	if !result.Timestamp.Equal(now) {
		t.Errorf("Timestamp = %v, want %v", result.Timestamp, now)
	}

	// Check metadata
	if result.Metadata["partition"] != "2" {
		t.Errorf("partition = %q, want %q", result.Metadata["partition"], "2")
	}
	if result.Metadata["offset"] != "100" {
		t.Errorf("offset = %q, want %q", result.Metadata["offset"], "100")
	}
	if result.Metadata["key"] != "test-key" {
		t.Errorf("key = %q, want %q", result.Metadata["key"], "test-key")
	}
	if result.Metadata["header_correlation-id"] != "abc123" {
		t.Errorf("header_correlation-id = %q, want %q", result.Metadata["header_correlation-id"], "abc123")
	}
	if result.Metadata["header_content-type"] != "application/json" {
		t.Errorf("header_content-type = %q, want %q", result.Metadata["header_content-type"], "application/json")
	}
	if result.Metadata["high_watermark"] != "150" {
		t.Errorf("high_watermark = %q, want %q", result.Metadata["high_watermark"], "150")
	}
}

func TestConvertKafkaMessage_MinimalMessage(t *testing.T) {
	msg := kafka.Message{
		Partition: 0,
		Offset:    0,
		Value:     []byte("simple message"),
	}

	result := convertKafkaMessage(msg, "minimal-topic")

	// Check ID format
	if result.ID != "0:0" {
		t.Errorf("ID = %q, want %q", result.ID, "0:0")
	}

	// Check body
	if result.Body != "simple message" {
		t.Errorf("Body = %q, want %q", result.Body, "simple message")
	}

	// Check that key is not present (empty key)
	if _, ok := result.Metadata["key"]; ok {
		t.Errorf("key should not be present for empty key")
	}

	// Check that high_watermark is not present (zero value)
	if _, ok := result.Metadata["high_watermark"]; ok {
		t.Errorf("high_watermark should not be present for zero value")
	}
}

func TestKafkaReader_Close(t *testing.T) {
	cfg := &mqreader.Config{
		Type:      mqreader.TypeKafka,
		URL:       "localhost:9092",
		QueueName: "test-topic",
	}

	reader, _ := NewKafkaReader(cfg)

	// Close without connecting should not error
	err := reader.Close()
	if err != nil {
		t.Errorf("Close() error: %v", err)
	}

	if reader.connected {
		t.Errorf("connected should be false after Close()")
	}
}

func TestKafkaReader_CloseMultipleTimes(t *testing.T) {
	cfg := &mqreader.Config{
		Type:      mqreader.TypeKafka,
		URL:       "localhost:9092",
		QueueName: "test-topic",
	}

	reader, _ := NewKafkaReader(cfg)

	// Close multiple times should not error
	for i := 0; i < 3; i++ {
		err := reader.Close()
		if err != nil {
			t.Errorf("Close() call %d error: %v", i+1, err)
		}
	}
}

func TestKafkaReaderFactoryRegistration(t *testing.T) {
	// Test that the factory is registered
	cfg := &mqreader.Config{
		Type:      mqreader.TypeKafka,
		URL:       "localhost:9092",
		QueueName: "test-topic",
	}

	reader, err := mqreader.NewReader(cfg)
	if err != nil {
		t.Fatalf("NewReader() error: %v", err)
	}

	if reader == nil {
		t.Fatal("NewReader() returned nil reader")
	}

	// Verify it's a KafkaReader
	_, ok := reader.(*KafkaReader)
	if !ok {
		t.Errorf("NewReader() returned wrong type, expected *KafkaReader")
	}
}

// Integration test - skipped unless KAFKA_BROKERS env is set
func TestKafkaReader_Integration(t *testing.T) {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")

	if brokers == "" || topic == "" {
		t.Skip("Set KAFKA_BROKERS and KAFKA_TOPIC env vars for integration tests")
	}

	cfg := &mqreader.Config{
		Type:            mqreader.TypeKafka,
		URL:             brokers,
		QueueName:       topic,
		MaxMessages:     5,
		WaitTimeSeconds: 3,
	}

	reader, err := NewKafkaReader(cfg)
	if err != nil {
		t.Fatalf("NewKafkaReader() error: %v", err)
	}
	defer reader.Close()

	ctx := context.Background()

	// Test Connect
	err = reader.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	// Test GetMetadata
	metadata, err := reader.GetMetadata(ctx)
	if err != nil {
		t.Errorf("GetMetadata() error: %v", err)
	} else {
		t.Logf("Metadata: Name=%s, Type=%s, MsgCount=%d, Partitions=%s",
			metadata.Name, metadata.Type, metadata.ApproxMsgCount,
			metadata.AdditionalInfo["partitions"])
	}

	// Test Peek
	messages, err := reader.Peek(ctx, 5)
	if err != nil {
		t.Errorf("Peek() error: %v", err)
	} else {
		t.Logf("Peeked %d messages", len(messages))
		for i, msg := range messages {
			t.Logf("  Message %d: ID=%s, Body=%s", i+1, msg.ID, msg.Body[:min(50, len(msg.Body))])
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
