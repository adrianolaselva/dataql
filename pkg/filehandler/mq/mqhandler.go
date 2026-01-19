// Package mq provides a FileHandler implementation for message queue sources.
// It supports any message queue system that implements the MessageQueueReader interface.
package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/mqreader"
	// Import message queue backends to register them
	_ "github.com/adrianolaselva/dataql/pkg/mqreader/kafka"
	_ "github.com/adrianolaselva/dataql/pkg/mqreader/sqs"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/schollz/progressbar/v3"
)

var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9_ ]+`)

// MQHandler implements FileHandler for message queue sources
type MQHandler struct {
	bar        *progressbar.ProgressBar
	storage    storage.Storage
	reader     mqreader.MessageQueueReader
	config     *mqreader.Config
	tableName  string
	totalLines int
	limitLines int
}

// NewMQHandler creates a new message queue file handler
func NewMQHandler(
	mqURL string,
	bar *progressbar.ProgressBar,
	storage storage.Storage,
	limitLines int,
	collection string,
) (filehandler.FileHandler, error) {
	// Parse the URL to get configuration
	config, err := mqreader.ParseURL(mqURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message queue URL: %w", err)
	}

	// Create the reader
	reader, err := mqreader.NewReader(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create message queue reader: %w", err)
	}

	// Determine table name
	tableName := collection
	if tableName == "" {
		tableName = config.GetTableName()
	} else {
		tableName = sanitizeTableName(tableName)
	}

	return &MQHandler{
		bar:        bar,
		storage:    storage,
		reader:     reader,
		config:     config,
		tableName:  tableName,
		limitLines: limitLines,
	}, nil
}

// Import reads messages from the queue and imports them into storage
func (h *MQHandler) Import() error {
	ctx := context.Background()

	// Connect to the queue
	if err := h.reader.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to message queue: %w", err)
	}

	// Determine how many messages to read
	maxMessages := h.config.MaxMessages
	if h.limitLines > 0 && h.limitLines < maxMessages {
		maxMessages = h.limitLines
	}

	// Read messages (peek, don't consume)
	messages, err := h.reader.Peek(ctx, maxMessages)
	if err != nil {
		return fmt.Errorf("failed to read messages: %w", err)
	}

	if len(messages) == 0 {
		// Create empty table with placeholder column
		if err := h.storage.BuildStructure(h.tableName, []string{"_empty"}); err != nil {
			return fmt.Errorf("failed to build structure for empty queue: %w", err)
		}
		return nil
	}

	// Convert messages to records and collect all columns
	records := make([]map[string]string, 0, len(messages))
	columnsSet := make(map[string]struct{})

	for _, msg := range messages {
		record := h.messageToRecord(msg)
		records = append(records, record)
		for col := range record {
			columnsSet[col] = struct{}{}
		}
	}

	// Sort columns for consistent ordering
	columns := make([]string, 0, len(columnsSet))
	for col := range columnsSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build table structure
	if err := h.storage.BuildStructure(h.tableName, columns); err != nil {
		return fmt.Errorf("failed to build structure: %w", err)
	}

	h.totalLines = len(records)
	if h.bar != nil {
		h.bar.ChangeMax(h.totalLines)
	}

	// Insert records
	for i, record := range records {
		values := make([]any, len(columns))
		for idx, col := range columns {
			if val, ok := record[col]; ok {
				values[idx] = val
			} else {
				values[idx] = ""
			}
		}

		if err := h.storage.InsertRow(h.tableName, columns, values); err != nil {
			return fmt.Errorf("failed to insert message %d: %w", i+1, err)
		}

		if h.bar != nil {
			_ = h.bar.Add(1)
		}
	}

	return nil
}

// messageToRecord converts a Message to a flat map for storage
func (h *MQHandler) messageToRecord(msg mqreader.Message) map[string]string {
	record := make(map[string]string)

	// Add standard message fields
	record["message_id"] = msg.ID
	record["source"] = msg.Source
	record["receive_count"] = fmt.Sprintf("%d", msg.ReceiveCount)

	if !msg.Timestamp.IsZero() {
		record["timestamp"] = msg.Timestamp.Format("2006-01-02 15:04:05")
		record["timestamp_unix"] = fmt.Sprintf("%d", msg.Timestamp.Unix())
	}

	// Add metadata fields with prefix
	for k, v := range msg.Metadata {
		colName := sanitizeColumnName("meta_" + k)
		record[colName] = v
	}

	// Store raw body
	record["body"] = msg.Body

	// Try to parse body as JSON and flatten
	var bodyData map[string]interface{}
	if err := json.Unmarshal([]byte(msg.Body), &bodyData); err == nil {
		// Flatten the JSON body
		flattened := flattenMap(bodyData, "body")
		for k, v := range flattened {
			record[k] = v
		}
	}

	return record
}

// flattenMap flattens a nested map into a single-level map
func flattenMap(data map[string]interface{}, prefix string) map[string]string {
	result := make(map[string]string)

	for key, value := range data {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "_" + key
		}
		fullKey = sanitizeColumnName(fullKey)

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := flattenMap(v, fullKey)
			for k, val := range nested {
				result[k] = val
			}
		case []interface{}:
			// Convert arrays to JSON string
			jsonBytes, _ := json.Marshal(v)
			result[fullKey] = string(jsonBytes)
		case nil:
			result[fullKey] = ""
		case float64:
			// Handle numbers - check if it's an integer
			if v == float64(int64(v)) {
				result[fullKey] = fmt.Sprintf("%d", int64(v))
			} else {
				result[fullKey] = fmt.Sprintf("%v", v)
			}
		case bool:
			if v {
				result[fullKey] = "true"
			} else {
				result[fullKey] = "false"
			}
		default:
			result[fullKey] = fmt.Sprintf("%v", v)
		}
	}

	return result
}

// sanitizeColumnName sanitizes a string to be used as a SQL column name
func sanitizeColumnName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// sanitizeTableName sanitizes a string to be used as a SQL table name
func sanitizeTableName(name string) string {
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)
	return nonAlphanumericRegex.ReplaceAllString(name, "")
}

// Lines returns total lines (messages) count
func (h *MQHandler) Lines() int {
	return h.totalLines
}

// Close releases resources
func (h *MQHandler) Close() error {
	if h.reader != nil {
		return h.reader.Close()
	}
	return nil
}
