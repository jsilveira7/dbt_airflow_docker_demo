-- Single database architecture: all schemas in sales_demo_analytics
-- Schemas: raw (ingested data), staging (cleaned data), marts (analytics tables)
CREATE DATABASE sales_demo_analytics;

\c sales_demo_analytics

CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION airflow;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION airflow;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION airflow;

-- Create raw table for customer transactions
CREATE TABLE IF NOT EXISTS raw.customer_transactions_raw (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    transaction_date VARCHAR(50),
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    quantity VARCHAR(50),
    price VARCHAR(50),
    tax VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255) DEFAULT 'customer_transactions.csv'
);

-- Create dq_issues table for tracking data quality problems
CREATE TABLE IF NOT EXISTS raw.dq_issues (
    issue_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    row_number INTEGER,
    issue_type VARCHAR(50),
    issue_description TEXT,
    raw_value VARCHAR(500),
    corrected_value VARCHAR(500),
    severity VARCHAR(20),
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);

-- Grant permissions
GRANT ALL ON SCHEMA raw TO airflow;
GRANT ALL ON SCHEMA staging TO airflow;
GRANT ALL ON SCHEMA marts TO airflow;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO airflow;

-- Indexes for common query patterns (created here; dbt tables will inherit schema permissions)
CREATE INDEX IF NOT EXISTS idx_raw_txn_id ON raw.customer_transactions_raw (transaction_id);
CREATE INDEX IF NOT EXISTS idx_raw_loaded_at ON raw.customer_transactions_raw (loaded_at);
