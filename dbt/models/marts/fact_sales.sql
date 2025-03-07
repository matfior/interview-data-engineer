{{
    config(
        materialized='table'
    )
}}

WITH sales_with_dimensions AS (
    SELECT
        s.sale_id,
        s.product_id,
        s.retailer_id,
        s.quantity,
        s.price,
        s.sale_amount,
        s.sale_date,
        -- Join with product dimension
        p.product_group,
        -- Join with retailer dimension
        r.business_model,
        r.region,
        -- Extract date parts for time-based analysis
        EXTRACT(YEAR FROM s.sale_date) as sale_year,
        EXTRACT(MONTH FROM s.sale_date) as sale_month,
        EXTRACT(DAY FROM s.sale_date) as sale_day,
        EXTRACT(DOW FROM s.sale_date) as sale_day_of_week
    FROM {{ ref('stg_sales') }} s
    LEFT JOIN {{ ref('dim_product') }} p ON s.product_id = p.product_id
    LEFT JOIN {{ ref('dim_retailer') }} r ON s.retailer_id = r.retailer_id
)

SELECT
    sale_id,
    product_id,
    retailer_id,
    quantity,
    price,
    sale_amount,
    sale_date,
    product_group,
    business_model,
    region,
    sale_year,
    sale_month,
    sale_day,
    sale_day_of_week
FROM sales_with_dimensions 