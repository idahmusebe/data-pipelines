-- ============================================================
-- BATCH PIPELINE: CO2 EMISSIONS DATASET
-- ClickHouse: Medallion Architecture
-- ============================================================

-- ============================================================
-- BRONZE LAYER: Raw CO2 emissions data
-- Selected columns from the Our World in Data dataset
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.co2_emissions
(
    country String,
    year UInt16,
    iso_code Nullable(String),
    population Nullable(Float64),
    gdp Nullable(Float64),
    co2 Nullable(Float64),
    co2_per_capita Nullable(Float64),
    co2_growth_abs Nullable(Float64),
    co2_growth_prct Nullable(Float64),
    coal_co2 Nullable(Float64),
    oil_co2 Nullable(Float64),
    gas_co2 Nullable(Float64),
    cement_co2 Nullable(Float64),
    land_use_change_co2 Nullable(Float64),
    total_ghg Nullable(Float64),
    methane Nullable(Float64),
    nitrous_oxide Nullable(Float64),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (country, year);