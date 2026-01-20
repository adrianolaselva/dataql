package dbconnector

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBConnector implements the Connector interface for DynamoDB
type DynamoDBConnector struct {
	client    *dynamodb.Client
	tableName string
	region    string
	endpoint  string
	ctx       context.Context
	cancel    context.CancelFunc
}

// DynamoDBConfig holds DynamoDB-specific configuration
type DynamoDBConfig struct {
	Region    string
	TableName string
	Endpoint  string // Optional: for LocalStack or local DynamoDB
}

// NewDynamoDBConnector creates a new DynamoDB connector
func NewDynamoDBConnector(cfg DynamoDBConfig) (*DynamoDBConnector, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	return &DynamoDBConnector{
		tableName: cfg.TableName,
		region:    cfg.Region,
		endpoint:  cfg.Endpoint,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

// Connect establishes a connection to DynamoDB
func (d *DynamoDBConnector) Connect() error {
	// Build AWS config options
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(d.region),
	}

	// Check for custom endpoint (LocalStack, local DynamoDB)
	endpoint := d.endpoint
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if endpoint == "" {
		endpoint = os.Getenv("AWS_ENDPOINT_URL_DYNAMODB")
	}

	cfg, err := config.LoadDefaultConfig(d.ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create DynamoDB client with optional custom endpoint
	if endpoint != "" {
		d.client = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	} else {
		d.client = dynamodb.NewFromConfig(cfg)
	}

	// Verify connection by describing the table
	_, err = d.client.DescribeTable(d.ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	})
	if err != nil {
		return fmt.Errorf("failed to describe DynamoDB table '%s': %w", d.tableName, err)
	}

	return nil
}

// Close closes the DynamoDB connection
func (d *DynamoDBConnector) Close() error {
	if d.cancel != nil {
		d.cancel()
	}
	return nil
}

// ListTables lists all tables in DynamoDB
func (d *DynamoDBConnector) ListTables() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := d.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list DynamoDB tables: %w", err)
	}

	return result.TableNames, nil
}

// GetTableSchema returns the schema for a DynamoDB table (inferred from first item)
func (d *DynamoDBConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Scan to get first item for schema inference
	result, err := d.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan DynamoDB table for schema: %w", err)
	}

	if len(result.Items) == 0 {
		return []ColumnInfo{}, nil
	}

	// Extract column info from first item's keys
	item := result.Items[0]
	columns := make([]ColumnInfo, 0, len(item))
	keys := make([]string, 0, len(item))

	for key := range item {
		keys = append(keys, key)
	}
	// Sort keys for consistent ordering
	sort.Strings(keys)

	for _, key := range keys {
		columns = append(columns, ColumnInfo{
			Name:     key,
			DataType: "TEXT",
			Nullable: true,
		})
	}

	return columns, nil
}

// ReadTable reads all items from a DynamoDB table
func (d *DynamoDBConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	// DynamoDB doesn't return sql.Rows, so this is a compatibility shim
	return nil, fmt.Errorf("use ReadItems for DynamoDB")
}

// ReadItems reads items from a DynamoDB table and returns them as maps
func (d *DynamoDBConnector) ReadItems(tableName string, limit int) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var results []map[string]interface{}
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
		}

		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		// Apply limit if specified
		if limit > 0 {
			remaining := limit - len(results)
			if remaining <= 0 {
				break
			}
			input.Limit = aws.Int32(int32(remaining))
		}

		result, err := d.client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to scan DynamoDB table: %w", err)
		}

		// Convert items to map[string]interface{}
		for _, item := range result.Items {
			doc := make(map[string]interface{})
			for key, val := range item {
				doc[key] = attributeValueToInterface(val)
			}
			results = append(results, doc)
		}

		// Check if we've reached the limit
		if limit > 0 && len(results) >= limit {
			break
		}

		// Check for pagination
		if result.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = result.LastEvaluatedKey
	}

	return results, nil
}

// Query executes a query (not directly supported in DynamoDB for arbitrary SQL)
func (d *DynamoDBConnector) Query(query string) (*sql.Rows, error) {
	return nil, fmt.Errorf("direct SQL queries not supported on DynamoDB")
}

// CreateTable creates a DynamoDB table
func (d *DynamoDBConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	// DynamoDB table creation requires key schema, which we don't have from generic columns
	// This is a no-op for DynamoDB as tables should be created externally
	return fmt.Errorf("DynamoDB tables must be created with proper key schema via AWS console or CLI")
}

// InsertRow inserts an item into a DynamoDB table
func (d *DynamoDBConnector) InsertRow(tableName string, columns []string, values []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	item := make(map[string]types.AttributeValue)
	for i, col := range columns {
		if i < len(values) {
			item[col] = interfaceToAttributeValue(values[i])
		}
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to insert item into DynamoDB: %w", err)
	}

	return nil
}

// attributeValueToInterface converts a DynamoDB attribute value to a Go interface{}
func attributeValueToInterface(av types.AttributeValue) interface{} {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberBOOL:
		return v.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberL:
		list := make([]interface{}, len(v.Value))
		for i, item := range v.Value {
			list[i] = attributeValueToInterface(item)
		}
		return list
	case *types.AttributeValueMemberM:
		m := make(map[string]interface{})
		for key, val := range v.Value {
			m[key] = attributeValueToInterface(val)
		}
		return m
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	default:
		return nil
	}
}

// interfaceToAttributeValue converts a Go interface{} to a DynamoDB attribute value
func interfaceToAttributeValue(val interface{}) types.AttributeValue {
	if val == nil {
		return &types.AttributeValueMemberNULL{Value: true}
	}

	switch v := val.(type) {
	case string:
		if v == "" {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		return &types.AttributeValueMemberS{Value: v}
	case int, int32, int64, float32, float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%v", v)}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}
	case []byte:
		return &types.AttributeValueMemberB{Value: v}
	default:
		return &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", v)}
	}
}
