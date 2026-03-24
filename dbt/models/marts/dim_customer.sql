-- Dimension table: Customers
-- This table contains unique customer information

{{ config(
    materialized='table',
    schema='marts',
    unique_key='customer_id'
) }}

WITH distinct_customers AS (
    SELECT DISTINCT
        customer_id,
        MAX(product_name) as last_purchased_product,
        COUNT(DISTINCT transaction_id) as total_transactions,
        MIN(transaction_date) as first_transaction_date,
        MAX(transaction_date) as last_transaction_date
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
),

enriched AS (
    SELECT
        customer_id,
        last_purchased_product,
        total_transactions,
        first_transaction_date,
        last_transaction_date,
        CURRENT_DATE - CAST(last_transaction_date AS DATE) as days_since_last_purchase,
        CASE
            WHEN CURRENT_DATE - CAST(last_transaction_date AS DATE) <= 30 THEN 'Active'
            WHEN CURRENT_DATE - CAST(last_transaction_date AS DATE) <= 90 THEN 'At Risk'
            ELSE 'Inactive'
        END as customer_status,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    FROM distinct_customers
)

SELECT * FROM enriched
