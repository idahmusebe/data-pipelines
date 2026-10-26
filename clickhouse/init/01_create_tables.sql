-- ============================================================
-- DATA ENGINEERING PIPELINE
-- ClickHouse: Medallion Architecture
-- ============================================================

-- ============================================================
-- BRONZE LAYER: Raw ingested earthquake data
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.earthquakes
(
    earthquake_id String,
    magnitude Nullable(Float64),
    place Nullable(String),
    event_time DateTime64(3, 'UTC'),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    depth_km Nullable(Float64),
    event_type Nullable(String),
    source_url Nullable(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_time, earthquake_id);


-- ============================================================
-- SILVER LAYER: Cleaned and validated earthquake data
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.earthquakes
(
    earthquake_id String,
    magnitude Nullable(Float64),
    place Nullable(String),
    event_time DateTime64(3, 'UTC'),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    depth_km Nullable(Float64),
    event_type Nullable(String),
    source_url Nullable(String),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (event_time, earthquake_id);


-- ============================================================
-- GOLD LAYER: Earthquake analytics
-- Aggregated earthquake counts by day and magnitude category
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.daily_earthquake_summary
(
    event_date Date,
    magnitude_category String,
    earthquake_count UInt64,
    average_magnitude Nullable(Float64),
    maximum_magnitude Nullable(Float64),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (event_date, magnitude_category);