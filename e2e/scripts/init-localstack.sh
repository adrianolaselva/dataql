#!/bin/bash
# DataQL E2E Test LocalStack Initialization
# This script runs after LocalStack is ready
#
# Creates:
# - S3 bucket with CSV, JSON, JSONL fixtures
# - SQS queues with sample messages
# - DynamoDB table with sample data

set -e

echo "============================================"
echo "Initializing LocalStack services..."
echo "============================================"

# Wait a moment for services to stabilize
sleep 3

# ============================================
# S3 Bucket Setup
# ============================================
echo ""
echo "[S3] Creating buckets and uploading fixtures..."

# Create bucket
awslocal s3 mb s3://dataql-test-bucket 2>/dev/null || echo "[S3] Bucket already exists"

# Create sample CSV content (5 rows)
cat > /tmp/simple.csv << 'EOF'
id,name,email,age
1,Alice,alice@example.com,28
2,Bob,bob@example.com,35
3,Charlie,charlie@example.com,42
4,Diana,diana@example.com,31
5,Eve,eve@example.com,25
EOF

# Create sample JSON array content
cat > /tmp/array.json << 'EOF'
[
  {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 28},
  {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35},
  {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 42},
  {"id": 4, "name": "Diana", "email": "diana@example.com", "age": 31},
  {"id": 5, "name": "Eve", "email": "eve@example.com", "age": 25}
]
EOF

# Create sample JSONL content (newline-delimited JSON)
cat > /tmp/data.jsonl << 'EOF'
{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 28}
{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35}
{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 42}
{"id": 4, "name": "Diana", "email": "diana@example.com", "age": 31}
{"id": 5, "name": "Eve", "email": "eve@example.com", "age": 25}
EOF

# Create nested JSON structure
cat > /tmp/nested.json << 'EOF'
{
  "users": [
    {"id": 1, "name": "Alice", "contact": {"email": "alice@example.com", "phone": "555-0101"}},
    {"id": 2, "name": "Bob", "contact": {"email": "bob@example.com", "phone": "555-0102"}}
  ],
  "metadata": {
    "version": "1.0",
    "generated": "2024-01-01"
  }
}
EOF

# Upload files to S3
echo "[S3] Uploading test fixtures..."
awslocal s3 cp /tmp/simple.csv s3://dataql-test-bucket/fixtures/simple.csv
awslocal s3 cp /tmp/array.json s3://dataql-test-bucket/fixtures/array.json
awslocal s3 cp /tmp/data.jsonl s3://dataql-test-bucket/fixtures/data.jsonl
awslocal s3 cp /tmp/nested.json s3://dataql-test-bucket/fixtures/nested.json

# Verify bucket contents
echo "[S3] Bucket contents:"
awslocal s3 ls s3://dataql-test-bucket/fixtures/ --recursive

# ============================================
# SQS Queue Setup
# ============================================
echo ""
echo "[SQS] Creating queues and sending messages..."

# Create standard queue
awslocal sqs create-queue --queue-name dataql-test-queue 2>/dev/null || echo "[SQS] Queue already exists"

# Get queue URL
QUEUE_URL=$(awslocal sqs get-queue-url --queue-name dataql-test-queue --query 'QueueUrl' --output text)
echo "[SQS] Queue URL: $QUEUE_URL"

# Send sample messages to SQS (JSON format)
echo "[SQS] Sending sample messages..."

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 28}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 35}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 42}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 4, "name": "Diana", "email": "diana@example.com", "age": 31}'

awslocal sqs send-message --queue-url "$QUEUE_URL" \
    --message-body '{"id": 5, "name": "Eve", "email": "eve@example.com", "age": 25}'

# Verify queue attributes
echo "[SQS] Queue attributes:"
awslocal sqs get-queue-attributes --queue-url "$QUEUE_URL" \
    --attribute-names ApproximateNumberOfMessages

# ============================================
# DynamoDB Table Setup (Optional - for future use)
# ============================================
echo ""
echo "[DynamoDB] Creating tables..."

awslocal dynamodb create-table \
    --table-name dataql-test-table \
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST 2>/dev/null || echo "[DynamoDB] Table already exists"

# Insert sample data
echo "[DynamoDB] Inserting test data..."
awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "1"}, "name": {"S": "Alice"}, "email": {"S": "alice@example.com"}}'

awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "2"}, "name": {"S": "Bob"}, "email": {"S": "bob@example.com"}}'

awslocal dynamodb put-item --table-name dataql-test-table \
    --item '{"id": {"S": "3"}, "name": {"S": "Charlie"}, "email": {"S": "charlie@example.com"}}'

# ============================================
# Verification Summary
# ============================================
echo ""
echo "============================================"
echo "LocalStack initialization complete!"
echo "============================================"
echo ""
echo "Created resources:"
echo "  - S3 Bucket: dataql-test-bucket"
echo "    - fixtures/simple.csv"
echo "    - fixtures/array.json"
echo "    - fixtures/data.jsonl"
echo "    - fixtures/nested.json"
echo "  - SQS Queue: dataql-test-queue (5 messages)"
echo "  - DynamoDB Table: dataql-test-table (3 items)"
echo ""
