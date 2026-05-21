-- Post-Deployment Validation Queries
-- Run these to verify the pipeline was set up correctly

-- ============================================
-- 1. DATABASE AND SCHEMA VERIFICATION
-- ============================================

-- Check if databases exist
SELECT datname FROM pg_database WHERE datname IN ('sales_demo_raw', 'sales_demo_analytics', 'airflow');

-- Check schemas in sales_demo_raw
\c sales_demo_raw
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('raw', 'staging', 'marts');

-- Check schemas in sales_demo_analytics
\c sales_demo_analytics
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name IN ('staging', 'marts');

-- ============================================
-- 2. TABLE VERIFICATION
-- ============================================

-- Check raw table
\c sales_demo_raw
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'raw' AND table_name LIKE '%transaction%';

-- Check dq_issues table
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_schema = 'raw' AND table_name = 'dq_issues';

-- ============================================
-- 3. DATA LOADING VERIFICATION
-- ============================================

-- Count raw records
SELECT COUNT(*) as raw_transaction_count FROM raw.customer_transactions_raw;

-- Sample raw data
SELECT * FROM raw.customer_transactions_raw LIMIT 3;

-- Check for data quality issues logged
SELECT COUNT(*) FROM raw.dq_issues;
SELECT issue_type, COUNT(*) FROM raw.dq_issues GROUP BY issue_type;

-- ============================================
-- 4. DBT TRANSFORMATION VERIFICATION
-- ============================================

-- Check staging model
SELECT COUNT(*) as staging_row_count FROM staging.stg_transactions;

-- Sample staging data with quality flags
SELECT 
    transaction_id,
    customer_id,
    price,
    data_quality_flag
FROM staging.stg_transactions 
LIMIT 5;

-- Distribution of quality flags
SELECT 
    data_quality_flag,
    COUNT(*) as count
FROM staging.stg_transactions
GROUP BY data_quality_flag;

-- ============================================
-- 5. DIMENSION TABLE VERIFICATION
-- ============================================

-- Check dimension table
SELECT COUNT(*) as dim_customer_count FROM marts.dim_customer;

-- Sample dimension data
SELECT 
    customer_id,
    total_transactions,
    customer_status,
    days_since_last_purchase
FROM marts.dim_customer 
LIMIT 5;

-- Customer status distribution
SELECT 
    customer_status,
    COUNT(*) as count
FROM marts.dim_customer
GROUP BY customer_status;

-- ============================================
-- 6. FACT TABLE VERIFICATION
-- ============================================

-- Check fact table
SELECT COUNT(*) as fact_transaction_count FROM marts.fact_transactions;

-- Sample fact data
SELECT 
    transaction_id,
    customer_id,
    transaction_date,
    total_amount,
    volume_category,
    price_category
FROM marts.fact_transactions 
LIMIT 5;

-- Aggregates by month
SELECT 
    transaction_year,
    transaction_month,
    COUNT(*) as transaction_count,
    ROUND(SUM(total_amount)::NUMERIC, 2) as monthly_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2) as avg_transaction_value
FROM marts.fact_transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year DESC, transaction_month DESC;

-- ============================================
-- 7. DATA QUALITY METRICS
-- ============================================

-- Records by quality flag in fact table
SELECT 
    data_quality_flag,
    COUNT(*) as record_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM marts.fact_transactions) * 100, 2) as percentage
FROM marts.fact_transactions
GROUP BY data_quality_flag;

-- Missing values analysis
SELECT 
    COUNT(CASE WHEN transaction_id IS NULL THEN 1 END) as null_transaction_id,
    COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_id,
    COUNT(CASE WHEN transaction_date IS NULL THEN 1 END) as null_date,
    COUNT(CASE WHEN quantity IS NULL THEN 1 END) as null_quantity
FROM staging.stg_transactions;

-- ============================================
-- 8. REFERENTIAL INTEGRITY CHECKS
-- ============================================

-- Verify all fact transactions have valid customer references
SELECT COUNT(*) as orphaned_facts
FROM marts.fact_transactions f
LEFT JOIN marts.dim_customer d ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL;

-- Verify unique transaction IDs
SELECT COUNT(*) as duplicate_ids
FROM marts.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- ============================================
-- 9. TEMPORAL RANGE VERIFICATION
-- ============================================

-- Date range in fact table (switch to sales_demo_analytics if not already)
\c sales_demo_analytics
SELECT 
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date,
    COUNT(DISTINCT transaction_date) as unique_dates
FROM marts.fact_transactions;

-- Year-month coverage
SELECT DISTINCT 
    transaction_year,
    transaction_month,
    COUNT(*) as records
FROM marts.fact_transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;

-- ============================================
-- 10. PERFORMANCE BASELINE
-- ============================================

-- Check indexes and query plans
\d marts.fact_transactions
\d marts.dim_customer

-- Query performance test
EXPLAIN ANALYZE
SELECT 
    d.customer_id,
    d.customer_status,
    COUNT(*) as transaction_count,
    SUM(f.total_amount) as total_spend
FROM marts.fact_transactions f
JOIN marts.dim_customer d ON f.customer_id = d.customer_id
WHERE f.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.customer_id, d.customer_status;

-- ============================================
-- 11. SUMMARY STATISTICS
-- ============================================

-- Overall statistics
SELECT 
    (SELECT COUNT(*) FROM marts.dim_customer) as unique_customers,
    (SELECT COUNT(DISTINCT product_id) FROM marts.fact_transactions) as unique_products,
    (SELECT COUNT(*) FROM marts.fact_transactions) as total_transactions,
    (SELECT ROUND(SUM(total_amount)::NUMERIC, 2) FROM marts.fact_transactions) as total_revenue,
    (SELECT ROUND(AVG(total_amount)::NUMERIC, 2) FROM marts.fact_transactions) as avg_transaction_amount,
    (SELECT ROUND(MAX(total_amount)::NUMERIC, 2) FROM marts.fact_transactions) as max_transaction_amount;

-- ============================================
-- 12. DATA LOADING TIMESTAMP VERIFICATION
-- ============================================

-- Most recent load time
SELECT 
    MAX(loaded_at) as last_load_time,
    COUNT(*) as records_in_last_load
FROM raw.customer_transactions_raw
GROUP BY DATE(loaded_at)
ORDER BY DATE(loaded_at) DESC
LIMIT 1;

-- ============================================
-- CLEANUP (Optional - Comment out if needed for investigation)
-- ============================================

-- To reset and reload from scratch:
-- DROP SCHEMA IF EXISTS raw CASCADE;
-- DROP SCHEMA IF EXISTS staging CASCADE;
-- DROP SCHEMA IF EXISTS marts CASCADE;
-- Then re-run the Airflow DAG
