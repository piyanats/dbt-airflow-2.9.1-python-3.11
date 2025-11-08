{{
    config(
        materialized='table',
        schema='sales_marts'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

daily_summary AS (
    SELECT
        transaction_date,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        SUM(quantity) AS total_quantity,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_transaction_value
    FROM transactions
    GROUP BY transaction_date
)

SELECT * FROM daily_summary
ORDER BY transaction_date DESC
