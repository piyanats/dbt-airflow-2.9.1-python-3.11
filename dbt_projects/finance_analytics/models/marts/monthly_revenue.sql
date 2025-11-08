{{
    config(
        materialized='table',
        schema='finance_marts'
    )
}}

WITH invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
),

monthly_summary AS (
    SELECT
        DATE_TRUNC(invoice_date, MONTH) AS month,
        COUNT(DISTINCT invoice_id) AS total_invoices,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_invoice_amount
    FROM invoices
    WHERE status = 'paid'
    GROUP BY 1
)

SELECT * FROM monthly_summary
ORDER BY month DESC
