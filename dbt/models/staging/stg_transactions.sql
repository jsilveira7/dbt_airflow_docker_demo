-- Staging model: Clean transactions that pass all quality checks
-- Materialized as table for performance and independent verification

{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    quantity,
    price,
    tax,
    quantity * price as total_amount,
    tax as tax_amount,
    'CLEAN' as data_quality_flag,
    loaded_at,
    CURRENT_TIMESTAMP as processed_at
FROM {{ ref('_stg_transactions_base') }}
WHERE quantity IS NOT NULL
    AND price IS NOT NULL
    AND tax IS NOT NULL
