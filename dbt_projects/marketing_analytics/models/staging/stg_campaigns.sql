{{
    config(
        materialized='view',
        schema='marketing_staging'
    )
}}

SELECT
    campaign_id,
    campaign_name,
    campaign_type,
    start_date,
    end_date,
    budget,
    created_at
FROM {{ source('marketing_raw', 'campaigns') }}
