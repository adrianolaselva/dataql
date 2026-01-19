-- DataQL E2E Test MySQL Initialization
-- This script runs automatically when the container starts

USE dataql_test;

-- Set default charset for session
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- Create test_data table with explicit charset
CREATE TABLE IF NOT EXISTS test_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert sample data
INSERT INTO test_data (name, email, age) VALUES
    ('Alice', 'alice@example.com', 28),
    ('Bob', 'bob@example.com', 35),
    ('Charlie', 'charlie@example.com', 42),
    ('Diana', 'diana@example.com', 31),
    ('Eve', 'eve@example.com', 25);

-- Create users table with explicit charset
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    department_id INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create departments table with explicit charset
CREATE TABLE IF NOT EXISTS departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
