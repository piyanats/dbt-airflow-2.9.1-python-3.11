{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Example staging model for customers
-- Replace with your actual source table
WITH source AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        customer_phone,
        customer_address,
        created_at,
        updated_at
    FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        TRIM(customer_name) AS customer_name,
        LOWER(TRIM(customer_email)) AS customer_email,
        customer_phone,
        customer_address,
        created_at,
        updated_at
    FROM source
)

SELECT * FROM renamed
