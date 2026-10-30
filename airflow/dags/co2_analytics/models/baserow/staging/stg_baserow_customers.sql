{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    baserow_id,
    JSONExtractString(raw_data, 'Customer ID') AS customer_id,
    trim(JSONExtractString(raw_data, 'Customer Name')) AS customer_name,
    lower(trim(JSONExtractString(raw_data, 'Email'))) AS email,
    trim(JSONExtractString(raw_data, 'Location')) AS location,
    toDateOrNull(
        JSONExtractString(raw_data, 'Registration Date')
    ) AS registration_date,
    ingested_at

FROM {{ source('baserow_bronze', 'customers_raw') }}

QUALIFY row_number() OVER (
    PARTITION BY baserow_id
    ORDER BY ingested_at DESC
) = 1