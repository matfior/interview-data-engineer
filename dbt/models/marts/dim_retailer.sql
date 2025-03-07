{{
    config(
        materialized='table'
    )
}}

WITH distinct_retailers AS (
    SELECT DISTINCT
        retailer_id,
        retailer_name,
        channel,
        location
    FROM {{ ref('stg_sales') }}
),

retailer_with_attributes AS (
    SELECT
        retailer_id,
        retailer_name,
        channel,
        location,
        -- Add derived fields for retailer categorization
        CASE
            WHEN channel = 'Online' THEN 'E-commerce'
            WHEN channel = 'Offline' THEN 'Brick and Mortar'
            ELSE 'Other'
        END as business_model,
        -- Region categorization based on location
        CASE
            WHEN location = 'New York' THEN 'East Coast'
            WHEN location = 'San Francisco' THEN 'West Coast'
            WHEN location = 'Unknown' THEN 'Unknown'
            ELSE 'Other'
        END as region
    FROM distinct_retailers
)

SELECT * FROM retailer_with_attributes 