// Package sqs provides an SQS implementation of the MessageQueueReader interface.
// It allows reading messages from AWS SQS queues without deleting them.
package sqs

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func init() {
	// Register SQS reader factory when this package is imported
	mqreader.RegisterReader(mqreader.TypeSQS, func(cfg *mqreader.Config) (mqreader.MessageQueueReader, error) {
		return NewSQSReader(cfg)
	})
}

// SQSReader implements MessageQueueReader for AWS SQS
type SQSReader struct {
	client          *sqs.Client
	queueURL        string
	queueName       string
	region          string
	endpoint        string
	maxMessages     int
	waitTimeSeconds int32
	connected       bool
	mu              sync.Mutex
}

// NewSQSReader creates a new SQS reader from a config
func NewSQSReader(cfg *mqreader.Config) (*SQSReader, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if cfg.QueueName == "" && cfg.URL == "" {
		return nil, fmt.Errorf("queue name or URL is required")
	}

	maxMsgs := cfg.MaxMessages
	if maxMsgs <= 0 {
		maxMsgs = mqreader.DefaultMaxMessages
	}

	waitTime := cfg.WaitTimeSeconds
	if waitTime < 0 {
		waitTime = 0
	}
	if waitTime > 20 {
		waitTime = 20 // SQS maximum
	}

	// Extract endpoint from options if provided
	endpoint := ""
	if cfg.Options != nil {
		endpoint = cfg.Options["endpoint"]
	}

	return &SQSReader{
		queueURL:        cfg.URL,
		queueName:       cfg.QueueName,
		region:          cfg.Region,
		endpoint:        endpoint,
		maxMessages:     maxMsgs,
		waitTimeSeconds: int32(waitTime),
	}, nil
}

// Connect establishes connection to SQS
func (r *SQSReader) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.connected {
		return nil
	}

	// Load AWS configuration
	var opts []func(*config.LoadOptions) error

	// Set region
	region := r.region
	if region == "" {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
	}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Check for explicit credentials (useful for LocalStack)
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Check for custom endpoint (LocalStack support)
	endpointURL := r.endpoint
	if endpointURL == "" {
		endpointURL = os.Getenv("AWS_ENDPOINT_URL_SQS")
	}
	if endpointURL == "" {
		endpointURL = os.Getenv("AWS_ENDPOINT_URL")
	}

	// Create SQS client with optional custom endpoint
	var sqsOpts []func(*sqs.Options)

	if endpointURL != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(endpointURL)
		})
	}

	r.client = sqs.NewFromConfig(cfg, sqsOpts...)

	// If we have a queue name but no URL, resolve it
	if r.queueURL == "" && r.queueName != "" {
		urlOutput, err := r.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(r.queueName),
		})
		if err != nil {
			return fmt.Errorf("failed to get queue URL for %s: %w", r.queueName, err)
		}
		r.queueURL = *urlOutput.QueueUrl
	}

	r.connected = true
	return nil
}

// Peek reads messages from the queue without deleting them.
// It uses VisibilityTimeout=0 so messages are immediately available for other consumers.
func (r *SQSReader) Peek(ctx context.Context, maxMessages int) ([]mqreader.Message, error) {
	if !r.connected {
		if err := r.Connect(ctx); err != nil {
			return nil, err
		}
	}

	if maxMessages <= 0 {
		maxMessages = r.maxMessages
	}

	var allMessages []mqreader.Message

	// SQS allows max 10 messages per request, so we may need multiple requests
	remaining := maxMessages
	seenIDs := make(map[string]bool) // Deduplicate messages

	for remaining > 0 {
		batchSize := remaining
		if batchSize > mqreader.MaxMessagesPerSQSRequest {
			batchSize = mqreader.MaxMessagesPerSQSRequest
		}

		// Receive messages with VisibilityTimeout=0
		// This makes messages immediately visible again after receiving
		output, err := r.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(r.queueURL),
			MaxNumberOfMessages:   int32(batchSize),
			VisibilityTimeout:     0, // Messages remain visible
			WaitTimeSeconds:       r.waitTimeSeconds,
			AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to receive messages: %w", err)
		}

		// If no more messages, stop
		if len(output.Messages) == 0 {
			break
		}

		for _, msg := range output.Messages {
			// Skip duplicates (can happen with multiple requests)
			if seenIDs[*msg.MessageId] {
				continue
			}
			seenIDs[*msg.MessageId] = true

			message := convertSQSMessage(msg, r.queueURL)
			allMessages = append(allMessages, message)
		}

		remaining -= len(output.Messages)

		// If we got fewer messages than requested, no more available
		if len(output.Messages) < batchSize {
			break
		}
	}

	return allMessages, nil
}

// GetMetadata returns information about the queue
func (r *SQSReader) GetMetadata(ctx context.Context) (*mqreader.QueueMetadata, error) {
	if !r.connected {
		if err := r.Connect(ctx); err != nil {
			return nil, err
		}
	}

	// Get queue attributes
	output, err := r.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(r.queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			types.QueueAttributeNameCreatedTimestamp,
			types.QueueAttributeNameLastModifiedTimestamp,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get queue attributes: %w", err)
	}

	metadata := &mqreader.QueueMetadata{
		Name:           r.queueName,
		Type:           mqreader.TypeSQS,
		AdditionalInfo: make(map[string]string),
	}

	// Parse message count
	if countStr, ok := output.Attributes["ApproximateNumberOfMessages"]; ok {
		if count, err := strconv.ParseInt(countStr, 10, 64); err == nil {
			metadata.ApproxMsgCount = count
		}
	}

	// Add all attributes to additional info
	for k, v := range output.Attributes {
		metadata.AdditionalInfo[k] = v
	}

	return metadata, nil
}

// Close releases resources (no-op for SQS as we don't maintain persistent connections)
func (r *SQSReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.connected = false
	r.client = nil
	return nil
}

// convertSQSMessage converts an SQS message to the generic Message type
func convertSQSMessage(msg types.Message, queueURL string) mqreader.Message {
	message := mqreader.Message{
		ID:       aws.ToString(msg.MessageId),
		Body:     aws.ToString(msg.Body),
		Source:   queueURL,
		Metadata: make(map[string]string),
	}

	// Store receipt handle (might be useful for debugging)
	if msg.ReceiptHandle != nil {
		message.Metadata["receipt_handle"] = *msg.ReceiptHandle
	}

	// Store MD5 of body
	if msg.MD5OfBody != nil {
		message.Metadata["md5_of_body"] = *msg.MD5OfBody
	}

	// Parse system attributes
	for k, v := range msg.Attributes {
		message.Metadata[k] = v

		switch k {
		case "SentTimestamp":
			if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
				message.Timestamp = time.UnixMilli(ts)
			}
		case "ApproximateReceiveCount":
			if count, err := strconv.Atoi(v); err == nil {
				message.ReceiveCount = count
			}
		}
	}

	// Parse message attributes (user-defined)
	for k, v := range msg.MessageAttributes {
		if v.StringValue != nil {
			message.Metadata["attr_"+k] = *v.StringValue
		}
	}

	return message
}
