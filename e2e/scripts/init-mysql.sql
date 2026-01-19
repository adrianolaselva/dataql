-- DataQL E2E Test MySQL Initialization
-- This script runs automatically when the container starts

USE dataql_test;

-- Create test_data table
CREATE TABLE IF NOT EXISTS test_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO test_data (name, email, age) VALUES
    ('Alice', 'alice@example.com', 28),
    ('Bob', 'bob@example.com', 35),
    ('Charlie', 'charlie@example.com', 42),
    ('Diana', 'diana@example.com', 31),
    ('Eve', 'eve@example.com', 25);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    department_id INT
);

-- Create departments table
CREATE TABLE IF NOT EXISTS departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
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

-- Create indexes
CREATE INDEX idx_test_data_name ON test_data(name);
CREATE INDEX idx_users_department ON users(department_id);
