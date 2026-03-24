-- Audit table: Rows rejected from stg_transactions due to NULL quantity, price, or tax
-- References the shared ephemeral base model for cleaning logic

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
    CASE
        WHEN quantity IS NULL THEN 'NULL quantity'
        WHEN price IS NULL THEN 'NULL price'
        WHEN tax IS NULL THEN 'NULL tax'
    END as rejection_reason,
    loaded_at,
    source_file,
    CURRENT_TIMESTAMP as audited_at
FROM {{ ref('_stg_transactions_base') }}
WHERE quantity IS NULL OR price IS NULL OR tax IS NULL
