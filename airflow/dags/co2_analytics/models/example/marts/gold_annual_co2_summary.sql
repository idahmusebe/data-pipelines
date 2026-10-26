{{ config(
    materialized='table',
    schema='gold',
    alias='annual_co2_summary',
    order_by='year'
) }}

SELECT
    year,

    count() AS total_records,

    countIf(isNotNull(co2)) AS records_with_co2_data,

    uniqExact(country) AS countries_and_regions,

    round(sumIf(co2, isNotNull(co2)), 2) AS total_co2,

    round(avg(co2), 4) AS average_co2,

    round(sumIf(coal_co2, isNotNull(coal_co2)), 2) AS total_coal_co2,

    round(sumIf(oil_co2, isNotNull(oil_co2)), 2) AS total_oil_co2,

    round(sumIf(gas_co2, isNotNull(gas_co2)), 2) AS total_gas_co2,

    round(sumIf(cement_co2, isNotNull(cement_co2)), 2) AS total_cement_co2

FROM {{ ref('silver_co2_emissions') }}

GROUP BY year