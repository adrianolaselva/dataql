#!/bin/bash
# DataQL E2E Test LocalStack Initialization
# This script runs after LocalStack is ready

set -e

echo "Initializing LocalStack services..."

# Wait a moment for services to stabilize
sleep 3

# ============================================
# S3 Bucket Setup
# ============================================
echo "Creating S3 buckets..."
awslocal s3 mb s3://dataql-test-bucket || true

# Create sample CSV content
cat > /tmp/simple.csv << 'EOF'
id,name,email
1,John,john@example.com
2,Jane,jane@example.com
3,Bob,bob@example.com
EOF

# Create sample JSON content
cat > /tmp/array.json << 'EOF'
[
  {"id": 1, "name": "John", "email": "john@example.com"},
  {"id": 2, "name": "Jane", "email": "jane@example.com"},
  {"id": 3, "name": "Bob", "email": "bob@example.com"}
]
EOF

# Create sample JSONL content
cat > /tmp/data.jsonl << 'EOF'
{"id": 1, "name": "John", "email": "john@example.com"}
{"id": 2, "name": "Jane", "email": "jane@example.com"}
{"id": 3, "name": "Bob", "email": "bob@example.com"}
EOF

# Upload files to S3
echo "Uploading test fixtures to S3..."
awslocal s3 cp /tmp/simple.csv s3://dataql-test-bucket/fixtures/simple.csv
awslocal s3 cp /tmp/array.json s3://dataql-test-bucket/fixtures/array.json
awslocal s3 cp /tmp/data.jsonl s3://dataql-test-bucket/fixtures/data.jsonl

# List bucket contents to verify
echo "S3 bucket contents:"
awslocal s3 ls s3://dataql-test-bucket/fixtures/ --recursive

# ============================================
# SQS Queue Setup
# ============================================
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name dataql-test-queue || true
awslocal sqs create-queue --queue-name dataql-json-queue || true

# Get queue URL
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name dataql-test-queue --query 'QueueUrl' --output text)
echo "Queue URL: $QUEUE_URL"

# Send sample messages to SQS
echo "Sending sample messages to SQS..."
awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 1, "name": "Alice", "email": "alice@example.com"}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 2, "name": "Bob", "email": "bob@example.com"}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 3, "name": "Charlie", "email": "charlie@example.com"}'

# ============================================
# DynamoDB Table Setup
# ============================================
echo "Creating DynamoDB tables..."
awslocal dynamodb create-table \
    --table-name dataql-test-table \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST || true

# Insert sample data
echo "Inserting DynamoDB test data..."
awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "1"}, "name": {"S": "Alice"}, "email": {"S": "alice@example.com"}}'

awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "2"}, "name": {"S": "Bob"}, "email": {"S": "bob@example.com"}}'

awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "3"}, "name": {"S": "Charlie"}, "email": {"S": "charlie@example.com"}}'

echo "LocalStack initialization complete!"
