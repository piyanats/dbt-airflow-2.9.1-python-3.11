{{
    config(
        materialized='table',
        schema='marketing_marts'
    )
}}

WITH campaigns AS (
    SELECT * FROM {{ ref('stg_campaigns') }}
)

SELECT
    campaign_id,
    campaign_name,
    campaign_type,
    start_date,
    end_date,
    budget,
    CURRENT_TIMESTAMP() AS updated_at
FROM campaigns
