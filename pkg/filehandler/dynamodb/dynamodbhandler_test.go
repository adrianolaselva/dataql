package dynamodb

import (
	"testing"
)

func TestParseDynamoDBURL_Valid(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantRegion string
		wantTable  string
		wantErr    bool
	}{
		{
			name:       "simple URL",
			url:        "dynamodb://us-east-1/my-table",
			wantRegion: "us-east-1",
			wantTable:  "my-table",
			wantErr:    false,
		},
		{
			name:       "different region",
			url:        "dynamodb://eu-west-1/users-table",
			wantRegion: "eu-west-1",
			wantTable:  "users-table",
			wantErr:    false,
		},
		{
			name:       "table with dashes",
			url:        "dynamodb://us-west-2/my-data-table",
			wantRegion: "us-west-2",
			wantTable:  "my-data-table",
			wantErr:    false,
		},
		{
			name:       "table with underscores",
			url:        "dynamodb://ap-south-1/user_events_table",
			wantRegion: "ap-south-1",
			wantTable:  "user_events_table",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseDynamoDBURL(tt.url)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseDynamoDBURL() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseDynamoDBURL() unexpected error: %v", err)
			}

			if info.Region != tt.wantRegion {
				t.Errorf("Expected region %s, got %s", tt.wantRegion, info.Region)
			}

			if info.TableName != tt.wantTable {
				t.Errorf("Expected table %s, got %s", tt.wantTable, info.TableName)
			}
		})
	}
}

func TestParseDynamoDBURL_WithEndpoint(t *testing.T) {
	url := "dynamodb://us-east-1/test-table?endpoint=http://localhost:8000"

	info, err := ParseDynamoDBURL(url)
	if err != nil {
		t.Fatalf("ParseDynamoDBURL() unexpected error: %v", err)
	}

	if info.Region != "us-east-1" {
		t.Errorf("Expected region us-east-1, got %s", info.Region)
	}

	if info.TableName != "test-table" {
		t.Errorf("Expected table test-table, got %s", info.TableName)
	}

	if info.Endpoint != "http://localhost:8000" {
		t.Errorf("Expected endpoint http://localhost:8000, got %s", info.Endpoint)
	}
}

func TestParseDynamoDBURL_LocalStackEndpoint(t *testing.T) {
	url := "dynamodb://us-east-1/dataql-test-table?endpoint=http://localhost:4566"

	info, err := ParseDynamoDBURL(url)
	if err != nil {
		t.Fatalf("ParseDynamoDBURL() unexpected error: %v", err)
	}

	if info.Endpoint != "http://localhost:4566" {
		t.Errorf("Expected endpoint http://localhost:4566, got %s", info.Endpoint)
	}
}

func TestParseDynamoDBURL_Invalid(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "wrong scheme",
			url:  "postgres://us-east-1/table",
		},
		{
			name: "missing region",
			url:  "dynamodb:///table",
		},
		{
			name: "missing table",
			url:  "dynamodb://us-east-1/",
		},
		{
			name: "missing table with no slash",
			url:  "dynamodb://us-east-1",
		},
		{
			name: "http URL",
			url:  "http://example.com/table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseDynamoDBURL(tt.url)
			if err == nil {
				t.Errorf("ParseDynamoDBURL(%s) expected error, got nil", tt.url)
			}
		})
	}
}

func TestIsDynamoDBURL(t *testing.T) {
	tests := []struct {
		url      string
		expected bool
	}{
		{"dynamodb://us-east-1/table", true},
		{"dynamodb://region/my-table", true},
		{"dynamodb://us-west-2/test?endpoint=http://localhost", true},
		{"postgres://user:pass@host/db", false},
		{"mysql://user:pass@host/db", false},
		{"mongodb://host/db/collection", false},
		{"s3://bucket/key", false},
		{"http://example.com/data.csv", false},
		{"file.csv", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			result := IsDynamoDBURL(tt.url)
			if result != tt.expected {
				t.Errorf("IsDynamoDBURL(%s) = %v, expected %v", tt.url, result, tt.expected)
			}
		})
	}
}

func TestDynamodbHandler_sanitizeName(t *testing.T) {
	handler := &dynamodbHandler{}

	tests := []struct {
		input    string
		expected string
	}{
		{"my-table", "my_table"},
		{"My Table", "my_table"},
		{"users.events", "users_events"},
		{"data-with-dashes", "data_with_dashes"},
		{"  trimmed  ", "trimmed"},
		{"MixedCase", "mixedcase"},
		{"special@chars!", "specialchars"},
		{"under_score", "under_score"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := handler.sanitizeName(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeName(%s) = %s, expected %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestDynamodbHandler_formatValue(t *testing.T) {
	handler := &dynamodbHandler{}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil value", nil, ""},
		{"string value", "hello", "hello"},
		{"int value", 42, "42"},
		{"int64 value", int64(1234567890), "1234567890"},
		{"float value", 3.14, "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.formatValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatValue(%v) = %s, expected %s", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNewDynamoDBHandler(t *testing.T) {
	connInfo := ConnectionInfo{
		Region:    "us-east-1",
		TableName: "test-table",
	}

	handler := NewDynamoDBHandler(connInfo, nil, nil, 100, "custom_name")

	if handler == nil {
		t.Fatal("NewDynamoDBHandler should return a handler")
	}

	// Type assertion to access internal fields
	h, ok := handler.(*dynamodbHandler)
	if !ok {
		t.Fatal("Handler should be *dynamodbHandler")
	}

	if h.connInfo.Region != "us-east-1" {
		t.Errorf("Expected region us-east-1, got %s", h.connInfo.Region)
	}

	if h.connInfo.TableName != "test-table" {
		t.Errorf("Expected table test-table, got %s", h.connInfo.TableName)
	}

	if h.limitLines != 100 {
		t.Errorf("Expected limitLines 100, got %d", h.limitLines)
	}

	if h.collection != "custom_name" {
		t.Errorf("Expected collection custom_name, got %s", h.collection)
	}
}

func TestDynamodbHandler_Lines(t *testing.T) {
	handler := &dynamodbHandler{
		totalLines: 42,
	}

	if handler.Lines() != 42 {
		t.Errorf("Expected Lines() = 42, got %d", handler.Lines())
	}
}

func TestDynamodbHandler_Close(t *testing.T) {
	handler := &dynamodbHandler{}

	err := handler.Close()
	if err != nil {
		t.Errorf("Close() should not return error, got: %v", err)
	}
}
