-- Base model: Clean and standardize raw transaction data
-- Ephemeral model shared by stg_transactions and audit_transactions

{{ config(materialized='ephemeral') }}

WITH raw_data AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        product_id,
        product_name,
        quantity,
        price,
        tax,
        loaded_at,
        source_file
    FROM {{ source('ebury_raw', 'customer_transactions_raw') }}
),

data_quality_checks AS (
    SELECT
        CASE 
            WHEN transaction_id ~ '^[0-9]+$' THEN CAST(transaction_id AS INTEGER)
            ELSE NULL
        END as transaction_id,
        
        CASE
            WHEN customer_id ~ '^[0-9]+\.0$' THEN CAST(SPLIT_PART(customer_id, '.', 1) AS INTEGER)
            WHEN customer_id ~ '^[0-9]+$' THEN CAST(customer_id AS INTEGER)
            ELSE NULL
        END as customer_id,
        
        CASE
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                TO_DATE(transaction_date, 'YYYY-MM-DD')
            WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 
                TO_DATE(transaction_date, 'DD-MM-YYYY')
            ELSE NULL
        END as transaction_date,
        
        CASE
            WHEN product_id ~ '^[0-9]+$' THEN CAST(product_id AS INTEGER)
            ELSE NULL
        END as product_id,
        
        product_name,
        
        CASE
            WHEN quantity ~ '^[0-9]+\.?[0-9]*$' AND quantity != '' THEN 
                CAST(quantity AS DECIMAL(10, 2))
            ELSE NULL
        END as quantity,
        
        CASE
            WHEN price ~ '^[0-9]+\.?[0-9]*$' THEN CAST(price AS DECIMAL(12, 2))
            ELSE NULL
        END as price,
        
        CASE
            WHEN tax ~ '^[0-9]+\.?[0-9]*$' THEN CAST(tax AS DECIMAL(12, 2))
            ELSE NULL
        END as tax,
        
        loaded_at,
        source_file,
        
        -- Data quality flags
        CASE WHEN transaction_id IS NULL THEN TRUE ELSE FALSE END as is_transaction_id_missing,
        CASE WHEN customer_id IS NULL THEN TRUE ELSE FALSE END as is_customer_id_missing,
        CASE WHEN transaction_date IS NULL THEN TRUE ELSE FALSE END as is_date_missing,
        CASE WHEN product_id IS NULL THEN TRUE ELSE FALSE END as is_product_id_missing,
        CASE WHEN quantity IS NULL THEN TRUE ELSE FALSE END as is_quantity_missing,
        CASE WHEN transaction_id !~ '^[0-9]+$' THEN TRUE ELSE FALSE END as has_invalid_transaction_id,
        CASE WHEN price ~ '[A-Za-z]' THEN TRUE ELSE FALSE END as has_non_numeric_price,
        CASE WHEN tax ~ '[A-Za-z]' THEN TRUE ELSE FALSE END as has_non_numeric_tax
    FROM raw_data
)

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    COALESCE(product_id, CASE TRIM(product_name)
        WHEN 'Product A' THEN 101
        WHEN 'Product B' THEN 102
        WHEN 'Product C' THEN 103
        WHEN 'Product D' THEN 104
        WHEN 'Product E' THEN 105
        ELSE NULL
    END) as product_id,
    COALESCE(product_name, CASE product_id
        WHEN 101 THEN 'Product A'
        WHEN 102 THEN 'Product B'
        WHEN 103 THEN 'Product C'
        WHEN 104 THEN 'Product D'
        WHEN 105 THEN 'Product E'
        ELSE NULL
    END) as product_name,
    quantity,
    price,
    tax,
    is_transaction_id_missing,
    is_customer_id_missing,
    is_date_missing,
    is_product_id_missing,
    is_quantity_missing,
    has_invalid_transaction_id,
    has_non_numeric_price,
    has_non_numeric_tax,
    loaded_at,
    source_file
FROM data_quality_checks
WHERE transaction_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND transaction_date IS NOT NULL
