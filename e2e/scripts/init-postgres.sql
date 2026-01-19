-- DataQL E2E Test PostgreSQL Initialization
-- This script runs automatically when the container starts

-- Create test_data table
CREATE TABLE IF NOT EXISTS test_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO test_data (name, email, age) VALUES
    ('Alice', 'alice@example.com', 28),
    ('Bob', 'bob@example.com', 35),
    ('Charlie', 'charlie@example.com', 42),
    ('Diana', 'diana@example.com', 31),
    ('Eve', 'eve@example.com', 25);

-- Create users table (for join tests)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    department_id INTEGER
);

-- Create departments table (for join tests)
CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Insert users
INSERT INTO users (username, department_id) VALUES
    ('john_doe', 1),
    ('jane_smith', 2),
    ('bob_wilson', 1);

-- Insert departments
INSERT INTO departments (name) VALUES
    ('Engineering'),
    ('Marketing'),
    ('Sales');

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_test_data_name ON test_data(name);
CREATE INDEX IF NOT EXISTS idx_users_department ON users(department_id);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataql;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataql;
