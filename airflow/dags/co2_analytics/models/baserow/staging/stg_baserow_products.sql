{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    baserow_id,
    JSONExtractString(raw_data, 'Product ID') AS product_id,
    trim(JSONExtractString(raw_data, 'Product Name')) AS product_name,
    JSONExtractString(
        JSONExtractRaw(raw_data, 'Category'),
        'value'
    ) AS category,
    toDecimal64OrNull(
        JSONExtractString(raw_data, 'Unit Price'),
        2
    ) AS unit_price,
    toUInt32OrNull(
        JSONExtractString(raw_data, 'Stock Quantity')
    ) AS stock_quantity,
    ingested_at

FROM {{ source('baserow_bronze', 'products_raw') }}

QUALIFY row_number() OVER (
    PARTITION BY baserow_id
    ORDER BY ingested_at DESC
) = 1