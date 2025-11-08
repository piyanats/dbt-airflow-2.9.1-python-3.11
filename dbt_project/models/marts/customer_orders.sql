{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Mart model aggregating customer order data
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders_agg AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.order_amount) AS total_revenue,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        AVG(o.order_amount) AS avg_order_amount
    FROM orders o
    GROUP BY o.customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_email,
    COALESCE(co.total_orders, 0) AS total_orders,
    COALESCE(co.total_revenue, 0) AS total_revenue,
    co.first_order_date,
    co.last_order_date,
    co.avg_order_amount
FROM customers c
LEFT JOIN customer_orders_agg co
    ON c.customer_id = co.customer_id
