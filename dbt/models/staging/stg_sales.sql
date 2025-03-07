{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT
        "SaleID" as sale_id,
        "ProductID" as product_id,
        "ProductName" as product_name,
        "Brand" as brand,
        "Category" as category,
        "RetailerID" as retailer_id,
        "RetailerName" as retailer_name,
        "Channel" as channel,
        "Location" as location,
        "Quantity" as quantity,
        "Price" as price,
        "Date" as sale_date,
        "LoadTimestamp" as load_timestamp
    FROM raw_sales
),

-- Additional data cleaning and validation
cleaned AS (
    SELECT
        sale_id,
        product_id,
        product_name,
        brand,
        category,
        retailer_id,
        retailer_name,
        channel,
        CASE 
            WHEN location = '' THEN 'Unknown'
            WHEN location IS NULL THEN 'Unknown'
            ELSE location
        END as location,
        -- Handle negative quantities by taking absolute value
        ABS(quantity) as quantity,
        price,
        sale_date,
        load_timestamp,
        -- Calculate total sale amount with absolute quantity
        ABS(quantity) * price as sale_amount
    FROM source
)

SELECT * FROM cleaned 