-- Dimension table: Products
-- Contains unique products with summary metrics

{{ config(
    materialized='table',
    schema='marts',
    unique_key='product_id'
) }}

WITH product_stats AS (
    SELECT
        product_id,
        product_name,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT customer_id) AS total_customers,
        ROUND(AVG(price), 2) AS avg_unit_price,
        ROUND(SUM(quantity * price), 2) AS total_revenue
    FROM {{ ref('stg_transactions') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id, product_name
)

SELECT
    product_id,
    product_name,
    total_transactions,
    total_customers,
    avg_unit_price,
    total_revenue,
    CURRENT_TIMESTAMP AS created_at
FROM product_stats
