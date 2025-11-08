{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Example staging model for orders
-- Replace with your actual source table
WITH source AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_amount,
        order_status,
        created_at,
        updated_at
    FROM {{ source('raw', 'orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_date AS DATE) AS order_date,
        CAST(order_amount AS NUMERIC) AS order_amount,
        LOWER(TRIM(order_status)) AS order_status,
        created_at,
        updated_at
    FROM source
)

SELECT * FROM renamed
