package dbconnector

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestNewDynamoDBConnector(t *testing.T) {
	config := DynamoDBConfig{
		Region:    "us-east-1",
		TableName: "test-table",
	}

	connector, err := NewDynamoDBConnector(config)

	if err != nil {
		t.Fatalf("NewDynamoDBConnector should not return error, got: %v", err)
	}

	if connector == nil {
		t.Fatal("NewDynamoDBConnector should return a connector")
	}

	if connector.tableName != "test-table" {
		t.Errorf("Expected tableName test-table, got: %s", connector.tableName)
	}

	if connector.region != "us-east-1" {
		t.Errorf("Expected region us-east-1, got: %s", connector.region)
	}
}

func TestNewDynamoDBConnector_WithEndpoint(t *testing.T) {
	config := DynamoDBConfig{
		Region:    "us-east-1",
		TableName: "test-table",
		Endpoint:  "http://localhost:8000",
	}

	connector, err := NewDynamoDBConnector(config)

	if err != nil {
		t.Fatalf("NewDynamoDBConnector should not return error, got: %v", err)
	}

	if connector == nil {
		t.Fatal("NewDynamoDBConnector should return a connector")
	}

	if connector.endpoint != "http://localhost:8000" {
		t.Errorf("Expected endpoint http://localhost:8000, got: %s", connector.endpoint)
	}
}

func TestAttributeValueToInterface_String(t *testing.T) {
	av := &types.AttributeValueMemberS{Value: "test-value"}
	result := attributeValueToInterface(av)

	if result != "test-value" {
		t.Errorf("Expected 'test-value', got: %v", result)
	}
}

func TestAttributeValueToInterface_Number(t *testing.T) {
	av := &types.AttributeValueMemberN{Value: "42"}
	result := attributeValueToInterface(av)

	if result != "42" {
		t.Errorf("Expected '42', got: %v", result)
	}
}

func TestAttributeValueToInterface_Bool(t *testing.T) {
	av := &types.AttributeValueMemberBOOL{Value: true}
	result := attributeValueToInterface(av)

	if result != true {
		t.Errorf("Expected true, got: %v", result)
	}
}

func TestAttributeValueToInterface_Null(t *testing.T) {
	av := &types.AttributeValueMemberNULL{Value: true}
	result := attributeValueToInterface(av)

	if result != nil {
		t.Errorf("Expected nil, got: %v", result)
	}
}

func TestAttributeValueToInterface_List(t *testing.T) {
	av := &types.AttributeValueMemberL{
		Value: []types.AttributeValue{
			&types.AttributeValueMemberS{Value: "item1"},
			&types.AttributeValueMemberS{Value: "item2"},
		},
	}
	result := attributeValueToInterface(av)

	list, ok := result.([]interface{})
	if !ok {
		t.Fatalf("Expected []interface{}, got: %T", result)
	}

	if len(list) != 2 {
		t.Errorf("Expected list of length 2, got: %d", len(list))
	}

	if list[0] != "item1" || list[1] != "item2" {
		t.Errorf("Expected [item1, item2], got: %v", list)
	}
}

func TestAttributeValueToInterface_Map(t *testing.T) {
	av := &types.AttributeValueMemberM{
		Value: map[string]types.AttributeValue{
			"key1": &types.AttributeValueMemberS{Value: "value1"},
			"key2": &types.AttributeValueMemberN{Value: "123"},
		},
	}
	result := attributeValueToInterface(av)

	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got: %T", result)
	}

	if m["key1"] != "value1" {
		t.Errorf("Expected key1='value1', got: %v", m["key1"])
	}

	if m["key2"] != "123" {
		t.Errorf("Expected key2='123', got: %v", m["key2"])
	}
}

func TestAttributeValueToInterface_Binary(t *testing.T) {
	av := &types.AttributeValueMemberB{Value: []byte("binary-data")}
	result := attributeValueToInterface(av)

	bytes, ok := result.([]byte)
	if !ok {
		t.Fatalf("Expected []byte, got: %T", result)
	}

	if string(bytes) != "binary-data" {
		t.Errorf("Expected 'binary-data', got: %s", string(bytes))
	}
}

func TestAttributeValueToInterface_StringSet(t *testing.T) {
	av := &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}}
	result := attributeValueToInterface(av)

	ss, ok := result.([]string)
	if !ok {
		t.Fatalf("Expected []string, got: %T", result)
	}

	if len(ss) != 3 {
		t.Errorf("Expected 3 elements, got: %d", len(ss))
	}
}

func TestAttributeValueToInterface_NumberSet(t *testing.T) {
	av := &types.AttributeValueMemberNS{Value: []string{"1", "2", "3"}}
	result := attributeValueToInterface(av)

	ns, ok := result.([]string)
	if !ok {
		t.Fatalf("Expected []string, got: %T", result)
	}

	if len(ns) != 3 {
		t.Errorf("Expected 3 elements, got: %d", len(ns))
	}
}

func TestInterfaceToAttributeValue_String(t *testing.T) {
	result := interfaceToAttributeValue("test-value")

	s, ok := result.(*types.AttributeValueMemberS)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberS, got: %T", result)
	}

	if s.Value != "test-value" {
		t.Errorf("Expected 'test-value', got: %s", s.Value)
	}
}

func TestInterfaceToAttributeValue_EmptyString(t *testing.T) {
	result := interfaceToAttributeValue("")

	_, ok := result.(*types.AttributeValueMemberNULL)
	if !ok {
		t.Errorf("Expected *types.AttributeValueMemberNULL for empty string, got: %T", result)
	}
}

func TestInterfaceToAttributeValue_Int(t *testing.T) {
	result := interfaceToAttributeValue(42)

	n, ok := result.(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberN, got: %T", result)
	}

	if n.Value != "42" {
		t.Errorf("Expected '42', got: %s", n.Value)
	}
}

func TestInterfaceToAttributeValue_Int64(t *testing.T) {
	result := interfaceToAttributeValue(int64(9999999999))

	n, ok := result.(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberN, got: %T", result)
	}

	if n.Value != "9999999999" {
		t.Errorf("Expected '9999999999', got: %s", n.Value)
	}
}

func TestInterfaceToAttributeValue_Float64(t *testing.T) {
	result := interfaceToAttributeValue(3.14)

	n, ok := result.(*types.AttributeValueMemberN)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberN, got: %T", result)
	}

	if n.Value != "3.14" {
		t.Errorf("Expected '3.14', got: %s", n.Value)
	}
}

func TestInterfaceToAttributeValue_Bool_True(t *testing.T) {
	result := interfaceToAttributeValue(true)

	b, ok := result.(*types.AttributeValueMemberBOOL)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberBOOL, got: %T", result)
	}

	if b.Value != true {
		t.Errorf("Expected true, got: %v", b.Value)
	}
}

func TestInterfaceToAttributeValue_Bool_False(t *testing.T) {
	result := interfaceToAttributeValue(false)

	b, ok := result.(*types.AttributeValueMemberBOOL)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberBOOL, got: %T", result)
	}

	if b.Value != false {
		t.Errorf("Expected false, got: %v", b.Value)
	}
}

func TestInterfaceToAttributeValue_Nil(t *testing.T) {
	result := interfaceToAttributeValue(nil)

	_, ok := result.(*types.AttributeValueMemberNULL)
	if !ok {
		t.Errorf("Expected *types.AttributeValueMemberNULL for nil, got: %T", result)
	}
}

func TestInterfaceToAttributeValue_Bytes(t *testing.T) {
	bytes := []byte("test-bytes")
	result := interfaceToAttributeValue(bytes)

	b, ok := result.(*types.AttributeValueMemberB)
	if !ok {
		t.Fatalf("Expected *types.AttributeValueMemberB, got: %T", result)
	}

	if string(b.Value) != "test-bytes" {
		t.Errorf("Expected 'test-bytes', got: %s", string(b.Value))
	}
}

func TestInterfaceToAttributeValue_Unknown(t *testing.T) {
	// Custom type should be converted to string
	type CustomType struct {
		Field string
	}
	custom := CustomType{Field: "value"}
	result := interfaceToAttributeValue(custom)

	_, ok := result.(*types.AttributeValueMemberS)
	if !ok {
		t.Errorf("Expected *types.AttributeValueMemberS for unknown type, got: %T", result)
	}
}
