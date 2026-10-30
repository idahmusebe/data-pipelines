{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    baserow_id,

    JSONExtractString(raw_data, 'Order ID') AS order_id,

    toDateOrNull(
        JSONExtractString(raw_data, 'Order Date')
    ) AS order_date,

    toUInt32OrNull(
        JSONExtractString(raw_data, 'Quantity')
    ) AS quantity,

    JSONExtractString(
        JSONExtractRaw(raw_data, 'Order Status'),
        'value'
    ) AS order_status,

    JSONExtractString(
        arrayElement(
            JSONExtractArrayRaw(raw_data, 'Customer ID'),
            1
        ),
        'value'
    ) AS customer_id,

    JSONExtractString(
        arrayElement(
            JSONExtractArrayRaw(raw_data, 'Product ID'),
            1
        ),
        'value'
    ) AS product_id,

    ingested_at

FROM {{ source('baserow_bronze', 'orders_raw') }}

QUALIFY row_number() OVER (
    PARTITION BY baserow_id
    ORDER BY ingested_at DESC
) = 1