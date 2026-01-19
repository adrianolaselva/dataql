// DataQL E2E Test MongoDB Initialization
// This script runs automatically when the container starts

// Switch to test database
db = db.getSiblingDB('dataql_test');

// Create users collection with sample data
db.users.insertMany([
    { name: 'Alice', email: 'alice@example.com', age: 28, role: 'admin' },
    { name: 'Bob', email: 'bob@example.com', age: 35, role: 'user' },
    { name: 'Charlie', email: 'charlie@example.com', age: 42, role: 'user' },
    { name: 'Diana', email: 'diana@example.com', age: 31, role: 'moderator' },
    { name: 'Eve', email: 'eve@example.com', age: 25, role: 'user' }
]);

// Create orders collection with sample data
db.orders.insertMany([
    { user_email: 'alice@example.com', product: 'Laptop', amount: 1299.99, date: new Date() },
    { user_email: 'bob@example.com', product: 'Mouse', amount: 29.99, date: new Date() },
    { user_email: 'alice@example.com', product: 'Keyboard', amount: 79.99, date: new Date() }
]);

// Create test_data collection (matches relational structure)
db.test_data.insertMany([
    { id: 1, name: 'Alice', email: 'alice@example.com', age: 28 },
    { id: 2, name: 'Bob', email: 'bob@example.com', age: 35 },
    { id: 3, name: 'Charlie', email: 'charlie@example.com', age: 42 },
    { id: 4, name: 'Diana', email: 'diana@example.com', age: 31 },
    { id: 5, name: 'Eve', email: 'eve@example.com', age: 25 }
]);

// Create indexes
db.users.createIndex({ email: 1 }, { unique: true });
db.users.createIndex({ name: 1 });
db.orders.createIndex({ user_email: 1 });
db.test_data.createIndex({ id: 1 }, { unique: true });

print('MongoDB initialization complete!');
