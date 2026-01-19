package mqreader

import (
	"testing"
)

func TestIsMQURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"SQS URL", "sqs://my-queue?region=us-east-1", true},
		{"SQS URL uppercase", "SQS://my-queue?region=us-east-1", true},
		{"Kafka URL", "kafka://broker:9092/topic", true},
		{"RabbitMQ URL", "rabbitmq://user:pass@host/queue", true},
		{"AMQP URL", "amqp://host/queue", true},
		{"Pulsar URL", "pulsar://host:6650/tenant/ns/topic", true},
		{"PubSub URL", "pubsub://project/subscription", true},
		{"HTTP URL", "https://example.com/data.csv", false},
		{"S3 URL", "s3://bucket/key", false},
		{"File path", "/path/to/file.csv", false},
		{"Postgres URL", "postgres://user:pass@host/db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsMQURL(tt.url)
			if result != tt.expected {
				t.Errorf("IsMQURL(%q) = %v, want %v", tt.url, result, tt.expected)
			}
		})
	}
}

func TestParseSQSURL_SimpleFormat(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		wantQueueName string
		wantRegion    string
		wantMaxMsgs   int
		wantWaitTime  int
		wantErr       bool
	}{
		{
			name:          "Simple queue name with region",
			url:           "sqs://my-queue?region=us-east-1",
			wantQueueName: "my-queue",
			wantRegion:    "us-east-1",
			wantMaxMsgs:   DefaultMaxMessages,
			wantWaitTime:  DefaultWaitTimeSeconds,
			wantErr:       false,
		},
		{
			name:          "Queue with all options",
			url:           "sqs://my-queue?region=eu-west-1&max_messages=50&wait_time=10",
			wantQueueName: "my-queue",
			wantRegion:    "eu-west-1",
			wantMaxMsgs:   50,
			wantWaitTime:  10,
			wantErr:       false,
		},
		{
			name:          "Queue with hyphen",
			url:           "sqs://my-event-queue?region=us-west-2",
			wantQueueName: "my-event-queue",
			wantRegion:    "us-west-2",
			wantMaxMsgs:   DefaultMaxMessages,
			wantWaitTime:  DefaultWaitTimeSeconds,
			wantErr:       false,
		},
		{
			name:    "Missing region",
			url:     "sqs://my-queue",
			wantErr: true,
		},
		{
			name:    "Empty queue name",
			url:     "sqs://?region=us-east-1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseURL(%q) expected error, got nil", tt.url)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseURL(%q) unexpected error: %v", tt.url, err)
				return
			}

			if config.Type != TypeSQS {
				t.Errorf("Type = %q, want %q", config.Type, TypeSQS)
			}
			if config.QueueName != tt.wantQueueName {
				t.Errorf("QueueName = %q, want %q", config.QueueName, tt.wantQueueName)
			}
			if config.Region != tt.wantRegion {
				t.Errorf("Region = %q, want %q", config.Region, tt.wantRegion)
			}
			if config.MaxMessages != tt.wantMaxMsgs {
				t.Errorf("MaxMessages = %d, want %d", config.MaxMessages, tt.wantMaxMsgs)
			}
			if config.WaitTimeSeconds != tt.wantWaitTime {
				t.Errorf("WaitTimeSeconds = %d, want %d", config.WaitTimeSeconds, tt.wantWaitTime)
			}
		})
	}
}

func TestParseSQSURL_FullAWSFormat(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		wantQueueName string
		wantRegion    string
		wantURL       string
	}{
		{
			name:          "Full AWS URL",
			url:           "sqs://https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
			wantQueueName: "my-queue",
			wantRegion:    "us-east-1",
			wantURL:       "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
		},
		{
			name:          "Full AWS URL with different region",
			url:           "sqs://https://sqs.eu-west-1.amazonaws.com/987654321/events-queue",
			wantQueueName: "events-queue",
			wantRegion:    "eu-west-1",
			wantURL:       "https://sqs.eu-west-1.amazonaws.com/987654321/events-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseURL(tt.url)
			if err != nil {
				t.Errorf("ParseURL(%q) unexpected error: %v", tt.url, err)
				return
			}

			if config.Type != TypeSQS {
				t.Errorf("Type = %q, want %q", config.Type, TypeSQS)
			}
			if config.QueueName != tt.wantQueueName {
				t.Errorf("QueueName = %q, want %q", config.QueueName, tt.wantQueueName)
			}
			if config.Region != tt.wantRegion {
				t.Errorf("Region = %q, want %q", config.Region, tt.wantRegion)
			}
			if config.URL != tt.wantURL {
				t.Errorf("URL = %q, want %q", config.URL, tt.wantURL)
			}
		})
	}
}

func TestParseKafkaURL(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		wantBroker    string
		wantQueueName string
		wantErr       bool
	}{
		{
			name:          "Simple Kafka URL",
			url:           "kafka://broker:9092/my-topic",
			wantBroker:    "broker:9092",
			wantQueueName: "my-topic",
			wantErr:       false,
		},
		{
			name:          "Kafka with multiple brokers",
			url:           "kafka://broker1:9092,broker2:9092/events",
			wantBroker:    "broker1:9092,broker2:9092",
			wantQueueName: "events",
			wantErr:       false,
		},
		{
			name:    "Kafka without topic",
			url:     "kafka://broker:9092",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseURL(tt.url)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseURL(%q) expected error, got nil", tt.url)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseURL(%q) unexpected error: %v", tt.url, err)
				return
			}

			if config.Type != TypeKafka {
				t.Errorf("Type = %q, want %q", config.Type, TypeKafka)
			}
			if config.URL != tt.wantBroker {
				t.Errorf("URL = %q, want %q", config.URL, tt.wantBroker)
			}
			if config.QueueName != tt.wantQueueName {
				t.Errorf("QueueName = %q, want %q", config.QueueName, tt.wantQueueName)
			}
		})
	}
}

func TestConfigGetTableName(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		wantTable string
	}{
		{
			name:      "Simple name",
			queueName: "myqueue",
			wantTable: "myqueue",
		},
		{
			name:      "Name with hyphens",
			queueName: "my-event-queue",
			wantTable: "my_event_queue",
		},
		{
			name:      "Name with dots",
			queueName: "my.queue.name",
			wantTable: "my_queue_name",
		},
		{
			name:      "Name starting with number",
			queueName: "123queue",
			wantTable: "mq_123queue",
		},
		{
			name:      "Empty name",
			queueName: "",
			wantTable: "messages",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{QueueName: tt.queueName}
			result := config.GetTableName()
			if result != tt.wantTable {
				t.Errorf("GetTableName() = %q, want %q", result, tt.wantTable)
			}
		})
	}
}

func TestParseUnsupportedURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"HTTP URL", "https://example.com/data"},
		{"File path", "/path/to/file.csv"},
		{"Unknown protocol", "ftp://server/file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseURL(tt.url)
			if err == nil {
				t.Errorf("ParseURL(%q) expected error for unsupported URL", tt.url)
			}
		})
	}
}
