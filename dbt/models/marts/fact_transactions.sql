-- Fact table: Transactions with aggregations
-- This table contains transaction-level facts with related dimensions and metrics

{{ config(
    materialized='table',
    schema='marts',
    unique_key='transaction_id'
) }}

WITH stg_transactions AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        product_id,
        product_name,
        quantity,
        price as unit_price,
        tax_amount,
        total_amount,
        data_quality_flag,
        processed_at
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_id IS NOT NULL
),

with_aggregations AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        product_name,
        transaction_date,
        EXTRACT(YEAR FROM transaction_date) as transaction_year,
        EXTRACT(MONTH FROM transaction_date) as transaction_month,
        EXTRACT(QUARTER FROM transaction_date) as transaction_quarter,
        TO_CHAR(transaction_date, 'Month') as transaction_month_name,
        EXTRACT(DAY FROM transaction_date) as transaction_day,
        EXTRACT(WEEK FROM transaction_date) as transaction_week,
        quantity,
        unit_price,
        tax_amount,
        total_amount,
        (tax_amount / NULLIF(total_amount, 0)) * 100 as tax_percentage,
        CASE
            WHEN quantity >= 5 THEN 'High Volume'
            WHEN quantity >= 3 THEN 'Medium Volume'
            ELSE 'Low Volume'
        END as volume_category,
        CASE
            WHEN unit_price >= 250 THEN 'Premium'
            WHEN unit_price >= 100 THEN 'Standard'
            ELSE 'Budget'
        END as price_category,
        data_quality_flag,
        processed_at,
        CURRENT_TIMESTAMP as created_at
    FROM stg_transactions
),

final AS (
    SELECT
        *,
        SUM(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as customer_cumulative_spend,
        SUM(quantity) OVER (
            PARTITION BY product_id
            ORDER BY transaction_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as product_cumulative_quantity
    FROM with_aggregations
)

SELECT * FROM final
