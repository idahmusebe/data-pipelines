{{ config(
    materialized='view',
    schema='silver',
    alias='co2_emissions'
) }}

SELECT
    trim(country) AS country,
    year,
    nullIf(trim(ifNull(iso_code, '')), '') AS iso_code,

    population,
    gdp,
    co2,
    co2_per_capita,
    co2_growth_abs,
    co2_growth_prct,
    coal_co2,
    oil_co2,
    gas_co2,
    cement_co2,
    land_use_change_co2,
    total_ghg,
    methane,
    nitrous_oxide,

    ingested_at

FROM bronze.co2_emissions

WHERE
    trim(country) != ''
    AND year BETWEEN 1750 AND 2100