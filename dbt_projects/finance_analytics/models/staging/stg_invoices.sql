{{
    config(
        materialized='view',
        schema='finance_staging'
    )
}}

SELECT
    invoice_id,
    customer_id,
    invoice_date,
    due_date,
    total_amount,
    status,
    created_at
FROM {{ source('finance_raw', 'invoices') }}
