Data Pipelines: Earthquakes and CO₂ Emissions

For this project, I built two data pipelines: one that ingests recent earthquake events and another that processes CO₂ emissions data in batches. I wanted to work with both real-time and batch data and understand how the data moves from ingestion through transformation to useful summaries.

I used Apache NiFi for earthquake ingestion, Apache Airflow to orchestrate the workflows, ClickHouse for storage, and dbt for the CO₂ transformations and tests. The pipelines use a Bronze, Silver, and Gold structure.

What I built

Real-time earthquake pipeline

This pipeline gets earthquake events from the USGS GeoJSON feed.

The NiFi flow is:

InvokeHTTP → SplitJson → JoltTransformJSON → UpdateAttribute → PutDatabaseRecord

InvokeHTTP requests the USGS feed.

SplitJson separates the response into individual earthquake events.

JoltTransformJSON reshapes the JSON fields into the structure used by the database.

UpdateAttribute adds source metadata.

PutDatabaseRecord writes the records to ClickHouse.

The records land in Bronze. An Airflow DAG called earthquake_pipeline refreshes the Silver and Gold tables on a five-minute schedule.

Layer

ClickHouse table

Purpose

Bronze

bronze.earthquakes_dedup

Stores ingested earthquake events

Silver

silver.earthquakes

Holds the transformed earthquake records

Gold

gold.daily_earthquake_summary

Summarizes earthquakes by date and magnitude category

Batch CO₂ emissions pipeline

For the batch pipeline, I used the Our World in Data CO₂ dataset in data/owid-co2-data.csv.

Airflow runs the co2_batch_pipeline DAG to load and validate the data. I used dbt to build the transformation models and run tests.

Layer

ClickHouse table

Purpose

Bronze

bronze.co2_emissions

Stores the source data

Silver

analytics_silver.co2_emissions

Holds the transformed emissions data

Gold

analytics_gold.annual_co2_summary

Contains annual CO₂ summary results

Tools I used

Apache NiFi

Apache Airflow

ClickHouse

dbt

Docker Compose

Python and SQL

Pandas

Project structure

data-pipelines/
├── docker-compose.yml
├── airflow/
│   └── dags/
│       ├── co2_batch_pipeline.py
│       ├── earthquake_pipeline.py
│       ├── load_co2_staging.py
│       ├── requirements.txt
│       └── co2_analytics/
│           ├── dbt_project.yml
│           ├── profiles.yml
│           └── models/
├── clickhouse/
│   └── init/
│       ├── 01_create_tables.sql
│       └── 02_create_co2_tables.sql
├── data/
│   └── owid-co2-data.csv
└── nifi/
    └── clickhouse-jdbc.jar

Running the project

I ran the project locally using Docker Compose from the project root.

Start the services:

docker compose up -d
docker compose ps

The local interfaces are:

Airflow: http://localhost:8085

NiFi: https://localhost:8443/nifi

ClickHouse HTTP: http://localhost:8124

In NiFi, the processors are started from the database destination backwards toward the HTTP source: PutDatabaseRecord, UpdateAttribute, JoltTransformJSON, SplitJson, and then InvokeHTTP.

To check the Airflow DAGs:

docker exec data-pipelines-airflow airflow dags list

To trigger the earthquake refresh:

docker exec data-pipelines-airflow airflow dags trigger earthquake_pipeline

To trigger the CO₂ batch workflow:

docker exec data-pipelines-airflow airflow dags trigger co2_batch_pipeline

To check their runs:

docker exec data-pipelines-airflow airflow dags list-runs -d earthquake_pipeline
docker exec data-pipelines-airflow airflow dags list-runs -d co2_batch_pipeline

To run the dbt models and tests:

docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt run --profiles-dir ."
docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt test --profiles-dir ."

Validation results

These are the results I observed during testing on October 26, 2026. Earthquake counts can change because the source feed is live.

Earthquake pipeline

Check

Result

Bronze records

70

Silver records

70

Gold summary rows

6

Unique earthquake IDs in the deduplicated Bronze read

70

Latest ingestion timestamp observed

2026-10-26 06:25:29 UTC

Recent Airflow runs

Successful

Bronze and Silver had matching record counts in the check. Gold has fewer rows because it groups the events into date and magnitude summaries.

CO₂ batch pipeline

Check

Result

Bronze records

50,411

Silver records

50,411

Gold summary rows

275

dbt models

4 succeeded in the recorded run

dbt tests

4 passed in the recorded test run

Airflow

A scheduled CO₂ task completed successfully

Queries I used to check the tables

Earthquake counts:

docker exec data-pipelines-clickhouse clickhouse-client --user pipeline_user ----password "$env:CLICKHOUSE_PASSWORD" --query "SELECT 'Bronze' AS layer, count() AS records FROM bronze.earthquakes_dedup FINAL UNION ALL SELECT 'Silver', count() FROM silver.earthquakes UNION ALL SELECT 'Gold', count() FROM gold.daily_earthquake_summary"

CO₂ counts:

docker exec data-pipelines-clickhouse clickhouse-client --user pipeline_user ----password "$env:CLICKHOUSE_PASSWORD" --query "SELECT 'Bronze CO2' AS layer, count() AS records FROM bronze.co2_emissions UNION ALL SELECT 'Silver CO2', count() FROM analytics_silver.co2_emissions UNION ALL SELECT 'Gold CO2', count() FROM analytics_gold.annual_co2_summary"

Data sources

https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson

https://github.com/owid/co2-data