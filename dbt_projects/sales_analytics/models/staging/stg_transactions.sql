{{
    config(
        materialized='view',
        schema='sales_staging'
    )
}}

WITH source AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        transaction_date,
        quantity,
        unit_price,
        total_amount,
        created_at
    FROM {{ source('sales_raw', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        CAST(quantity AS INT64) AS quantity,
        CAST(unit_price AS NUMERIC) AS unit_price,
        CAST(total_amount AS NUMERIC) AS total_amount,
        created_at
    FROM source
    WHERE transaction_date IS NOT NULL
)

SELECT * FROM cleaned
