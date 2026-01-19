package mqreader

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Supported URL prefixes for message queue systems
var mqPrefixes = []string{
	"sqs://",
	"kafka://",
	"rabbitmq://",
	"amqp://",
	"pulsar://",
	"pubsub://",
}

// IsMQURL checks if the given URL is a message queue URL
func IsMQURL(urlStr string) bool {
	lower := strings.ToLower(urlStr)
	for _, prefix := range mqPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

// ParseURL parses a message queue URL and returns a Config.
// Supported URL formats:
//   - sqs://queue-name?region=us-east-1
//   - sqs://https://sqs.us-east-1.amazonaws.com/123456789/queue-name
//   - kafka://broker:9092/topic
//   - rabbitmq://user:pass@host:5672/vhost/queue
//   - pulsar://host:6650/tenant/namespace/topic
func ParseURL(urlStr string) (*Config, error) {
	lower := strings.ToLower(urlStr)

	switch {
	case strings.HasPrefix(lower, "sqs://"):
		return parseSQSURL(urlStr)
	case strings.HasPrefix(lower, "kafka://"):
		return parseKafkaURL(urlStr)
	case strings.HasPrefix(lower, "rabbitmq://"), strings.HasPrefix(lower, "amqp://"):
		return parseRabbitMQURL(urlStr)
	case strings.HasPrefix(lower, "pulsar://"):
		return parsePulsarURL(urlStr)
	case strings.HasPrefix(lower, "pubsub://"):
		return parsePubSubURL(urlStr)
	default:
		return nil, fmt.Errorf("unsupported message queue URL: %s", urlStr)
	}
}

// parseSQSURL parses an SQS URL in two formats:
// 1. sqs://queue-name?region=us-east-1&max_messages=10
// 2. sqs://https://sqs.us-east-1.amazonaws.com/123456789/queue-name
func parseSQSURL(urlStr string) (*Config, error) {
	// Remove the sqs:// prefix
	remainder := strings.TrimPrefix(urlStr, "sqs://")

	config := &Config{
		Type:            TypeSQS,
		MaxMessages:     DefaultMaxMessages,
		WaitTimeSeconds: DefaultWaitTimeSeconds,
		Options:         make(map[string]string),
	}

	// Check if it's a full AWS URL format
	if strings.HasPrefix(remainder, "https://") || strings.HasPrefix(remainder, "http://") {
		// Format: sqs://https://sqs.us-east-1.amazonaws.com/123456789/queue-name
		return parseSQSFullURL(remainder, config)
	}

	// Format: sqs://queue-name?region=us-east-1
	return parseSQSSimpleURL(remainder, config)
}

// parseSQSFullURL parses the full AWS SQS URL format
func parseSQSFullURL(urlStr string, config *Config) (*Config, error) {
	// Parse the AWS URL to extract region and queue name
	// Format: https://sqs.us-east-1.amazonaws.com/123456789/queue-name
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SQS URL: %w", err)
	}

	// Extract region from hostname (sqs.REGION.amazonaws.com)
	regionRegex := regexp.MustCompile(`sqs\.([a-z0-9-]+)\.amazonaws\.com`)
	matches := regionRegex.FindStringSubmatch(u.Host)
	if len(matches) >= 2 {
		config.Region = matches[1]
	}

	// Extract queue name from path (last segment)
	pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pathParts) >= 1 {
		config.QueueName = pathParts[len(pathParts)-1]
	}

	// The full URL is used as-is for the AWS SDK
	config.URL = urlStr

	// Parse query parameters for options
	parseQueryParams(u.Query(), config)

	if config.QueueName == "" {
		return nil, fmt.Errorf("could not extract queue name from URL: %s", urlStr)
	}

	return config, nil
}

// parseSQSSimpleURL parses the simple queue-name format
func parseSQSSimpleURL(urlStr string, config *Config) (*Config, error) {
	// Split by ? to get queue name and query params
	parts := strings.SplitN(urlStr, "?", 2)
	config.QueueName = parts[0]

	if config.QueueName == "" {
		return nil, fmt.Errorf("queue name is required in SQS URL")
	}

	// Parse query parameters
	if len(parts) == 2 {
		params, err := url.ParseQuery(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid query parameters: %w", err)
		}
		parseQueryParams(params, config)
	}

	// Region is required for simple format
	if config.Region == "" {
		return nil, fmt.Errorf("region is required for SQS URL format sqs://queue-name?region=REGION")
	}

	return config, nil
}

// parseKafkaURL parses a Kafka URL
// Format: kafka://broker:9092/topic?group_id=mygroup
func parseKafkaURL(urlStr string) (*Config, error) {
	remainder := strings.TrimPrefix(urlStr, "kafka://")

	config := &Config{
		Type:        TypeKafka,
		MaxMessages: DefaultMaxMessages,
		Options:     make(map[string]string),
	}

	// Split by ? to get broker/topic and query params
	parts := strings.SplitN(remainder, "?", 2)
	brokerTopic := parts[0]

	// Parse broker and topic
	// Format: broker:9092/topic or broker1:9092,broker2:9092/topic
	slashIdx := strings.Index(brokerTopic, "/")
	if slashIdx == -1 {
		return nil, fmt.Errorf("topic is required in Kafka URL: kafka://broker:port/topic")
	}

	config.URL = brokerTopic[:slashIdx]         // broker:port
	config.QueueName = brokerTopic[slashIdx+1:] // topic name

	// Parse query parameters
	if len(parts) == 2 {
		params, err := url.ParseQuery(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid query parameters: %w", err)
		}
		parseQueryParams(params, config)
	}

	return config, nil
}

// parseRabbitMQURL parses a RabbitMQ URL
// Format: rabbitmq://user:pass@host:5672/vhost/queue
func parseRabbitMQURL(urlStr string) (*Config, error) {
	// Normalize prefix
	urlStr = strings.Replace(urlStr, "rabbitmq://", "amqp://", 1)

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid RabbitMQ URL: %w", err)
	}

	config := &Config{
		Type:        TypeRabbitMQ,
		MaxMessages: DefaultMaxMessages,
		Options:     make(map[string]string),
		Credentials: make(map[string]string),
	}

	// Extract host and port
	config.URL = u.Host

	// Extract credentials
	if u.User != nil {
		config.Credentials["username"] = u.User.Username()
		if pwd, ok := u.User.Password(); ok {
			config.Credentials["password"] = pwd
		}
	}

	// Parse path: /vhost/queue
	pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pathParts) >= 1 && pathParts[0] != "" {
		config.Options["vhost"] = pathParts[0]
	}
	if len(pathParts) >= 2 {
		config.QueueName = pathParts[1]
	}

	// Parse query parameters
	parseQueryParams(u.Query(), config)

	return config, nil
}

// parsePulsarURL parses a Pulsar URL
// Format: pulsar://host:6650/tenant/namespace/topic
func parsePulsarURL(urlStr string) (*Config, error) {
	remainder := strings.TrimPrefix(urlStr, "pulsar://")

	config := &Config{
		Type:        TypePulsar,
		MaxMessages: DefaultMaxMessages,
		Options:     make(map[string]string),
	}

	// Split by ? to get path and query params
	parts := strings.SplitN(remainder, "?", 2)
	hostPath := parts[0]

	// Parse host and path
	slashIdx := strings.Index(hostPath, "/")
	if slashIdx == -1 {
		config.URL = hostPath
	} else {
		config.URL = hostPath[:slashIdx]
		// Path format: tenant/namespace/topic
		pathParts := strings.Split(hostPath[slashIdx+1:], "/")
		if len(pathParts) >= 3 {
			config.Options["tenant"] = pathParts[0]
			config.Options["namespace"] = pathParts[1]
			config.QueueName = pathParts[2]
		} else if len(pathParts) >= 1 {
			config.QueueName = pathParts[len(pathParts)-1]
		}
	}

	// Parse query parameters
	if len(parts) == 2 {
		params, err := url.ParseQuery(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid query parameters: %w", err)
		}
		parseQueryParams(params, config)
	}

	return config, nil
}

// parsePubSubURL parses a Google Pub/Sub URL
// Format: pubsub://project/subscription
func parsePubSubURL(urlStr string) (*Config, error) {
	remainder := strings.TrimPrefix(urlStr, "pubsub://")

	config := &Config{
		Type:        TypePubSub,
		MaxMessages: DefaultMaxMessages,
		Options:     make(map[string]string),
	}

	// Split by ? to get project/subscription and query params
	parts := strings.SplitN(remainder, "?", 2)

	// Parse project and subscription
	pathParts := strings.Split(parts[0], "/")
	if len(pathParts) >= 2 {
		config.Options["project"] = pathParts[0]
		config.QueueName = pathParts[1]
	} else if len(pathParts) == 1 {
		config.QueueName = pathParts[0]
	}

	// Parse query parameters
	if len(parts) == 2 {
		params, err := url.ParseQuery(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid query parameters: %w", err)
		}
		parseQueryParams(params, config)
	}

	return config, nil
}

// parseQueryParams extracts common parameters from URL query
func parseQueryParams(params url.Values, config *Config) {
	if region := params.Get("region"); region != "" {
		config.Region = region
	}

	if maxMsgs := params.Get("max_messages"); maxMsgs != "" {
		if n, err := strconv.Atoi(maxMsgs); err == nil && n > 0 {
			config.MaxMessages = n
		}
	}

	if waitTime := params.Get("wait_time"); waitTime != "" {
		if n, err := strconv.Atoi(waitTime); err == nil && n >= 0 {
			config.WaitTimeSeconds = n
		}
	}

	// Store all other parameters in Options
	for key, values := range params {
		if key != "region" && key != "max_messages" && key != "wait_time" && len(values) > 0 {
			config.Options[key] = values[0]
		}
	}
}

// GetTableName extracts a clean table name from the config
func (c *Config) GetTableName() string {
	name := c.QueueName

	// Clean up the name for use as a SQL table name
	// Replace hyphens and dots with underscores
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, ".", "_")

	// Remove any non-alphanumeric characters except underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	name = reg.ReplaceAllString(name, "")

	// Ensure it doesn't start with a number
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "mq_" + name
	}

	if name == "" {
		name = "messages"
	}

	return name
}
