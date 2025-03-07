{{
    config(
        materialized='table'
    )
}}

WITH distinct_products AS (
    SELECT DISTINCT
        product_id,
        product_name,
        brand,
        category
    FROM {{ ref('stg_sales') }}
)

SELECT
    product_id,
    product_name,
    brand,
    category,
    -- Add additional product attributes or derived fields
    CASE
        WHEN category = 'Electronics' THEN 'Tech'
        WHEN category = 'Appliances' THEN 'Home'
        WHEN category = 'Furniture' THEN 'Home'
        ELSE 'Other'
    END as product_group
FROM distinct_products 