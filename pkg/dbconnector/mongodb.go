package dbconnector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBConnector implements the Connector interface for MongoDB
type MongoDBConnector struct {
	config     Config
	client     *mongo.Client
	database   *mongo.Database
	collection string
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewMongoDBConnector creates a new MongoDB connector
func NewMongoDBConnector(config Config) (*MongoDBConnector, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	return &MongoDBConnector{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Connect establishes a connection to MongoDB
func (m *MongoDBConnector) Connect() error {
	// Build connection string
	connStr := m.buildConnectionString()

	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(m.ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	if err := client.Ping(m.ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	m.client = client
	m.database = client.Database(m.config.Database)
	return nil
}

// buildConnectionString builds a MongoDB connection string
func (m *MongoDBConnector) buildConnectionString() string {
	var auth string
	if m.config.User != "" {
		if m.config.Password != "" {
			auth = fmt.Sprintf("%s:%s@", m.config.User, m.config.Password)
		} else {
			auth = fmt.Sprintf("%s@", m.config.User)
		}
	}

	host := m.config.Host
	if host == "" {
		host = "localhost"
	}

	port := m.config.Port
	if port == 0 {
		port = 27017
	}

	return fmt.Sprintf("mongodb://%s%s:%d", auth, host, port)
}

// Close closes the MongoDB connection
func (m *MongoDBConnector) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return m.client.Disconnect(ctx)
	}
	return nil
}

// SetCollection sets the collection to work with
func (m *MongoDBConnector) SetCollection(collection string) {
	m.collection = collection
}

// ListTables lists all collections in the database
func (m *MongoDBConnector) ListTables() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collections, err := m.database.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// GetTableSchema returns the schema for a collection (inferred from first document)
func (m *MongoDBConnector) GetTableSchema(tableName string) ([]ColumnInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := m.database.Collection(tableName)

	// Get first document to infer schema
	var doc bson.M
	err := coll.FindOne(ctx, bson.M{}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return []ColumnInfo{}, nil
		}
		return nil, fmt.Errorf("failed to get document for schema: %w", err)
	}

	// Extract column info from document keys
	columns := make([]ColumnInfo, 0, len(doc))
	for key := range doc {
		columns = append(columns, ColumnInfo{
			Name:     key,
			DataType: "TEXT",
			Nullable: true,
		})
	}

	return columns, nil
}

// ReadTable reads all documents from a collection
func (m *MongoDBConnector) ReadTable(tableName string, limit int) (*sql.Rows, error) {
	// MongoDB doesn't return sql.Rows, so this is a compatibility shim
	// We'll return nil and use a custom method for MongoDB
	return nil, fmt.Errorf("use ReadCollection for MongoDB")
}

// ReadCollection reads documents from a MongoDB collection and returns them as maps
func (m *MongoDBConnector) ReadCollection(tableName string, limit int) ([]map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	coll := m.database.Collection(tableName)

	findOptions := options.Find()
	if limit > 0 {
		findOptions.SetLimit(int64(limit))
	}

	cursor, err := coll.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("failed to decode document: %w", err)
		}
		results = append(results, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return results, nil
}

// Query executes a query (not directly supported in MongoDB)
func (m *MongoDBConnector) Query(query string) (*sql.Rows, error) {
	return nil, fmt.Errorf("direct SQL queries not supported on MongoDB")
}

// CreateTable creates a collection (MongoDB creates collections automatically)
func (m *MongoDBConnector) CreateTable(tableName string, columns []ColumnInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := m.database.CreateCollection(ctx, tableName)
	if err != nil {
		// Collection might already exist, which is OK
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create collection: %w", err)
		}
	}
	return nil
}

// InsertRow inserts a document into a collection
func (m *MongoDBConnector) InsertRow(tableName string, columns []string, values []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	coll := m.database.Collection(tableName)

	doc := bson.M{}
	for i, col := range columns {
		if i < len(values) {
			doc[col] = values[i]
		}
	}

	_, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	return nil
}
