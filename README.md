# Data Pipelines: Earthquakes and CO₂ Emissions

For this project, I built two data pipelines: one that ingests recent earthquake events and another that processes CO₂ emissions data in batches. I wanted to work with both real-time and batch data and understand how the data moves from ingestion through transformation to useful summaries.

I used Apache NiFi for earthquake ingestion, Apache Airflow to orchestrate the workflows, ClickHouse for storage, and dbt for the CO₂ transformations and tests. The pipelines use a Bronze, Silver, and Gold structure.

## What I Built

### 1. Real-Time Earthquake Pipeline

This pipeline gets earthquake events from the USGS GeoJSON feed.

The NiFi flow is:

`InvokeHTTP → SplitJson → JoltTransformJSON → UpdateAttribute → PutDatabaseRecord`

* **InvokeHTTP:** Requests the USGS earthquake feed.
* **SplitJson:** Separates the response into individual earthquake events.
* **JoltTransformJSON:** Reshapes the JSON fields into the structure used by the database.
* **UpdateAttribute:** Adds source metadata.
* **PutDatabaseRecord:** Writes the records to ClickHouse.

The records land in Bronze. An Airflow DAG called `earthquake_pipeline` refreshes the Silver and Gold tables on a five-minute schedule.

| Layer  | ClickHouse table                | Purpose                                               |
| ------ | ------------------------------- | ----------------------------------------------------- |
| Bronze | `bronze.earthquakes_dedup`      | Stores ingested earthquake events                     |
| Silver | `silver.earthquakes`            | Holds transformed earthquake records                  |
| Gold   | `gold.daily_earthquake_summary` | Summarizes earthquakes by date and magnitude category |

### 2. Batch CO₂ Emissions Pipeline

For the batch pipeline, I used the Our World in Data CO₂ dataset stored in `data/owid-co2-data.csv`.

Airflow runs the `co2_batch_pipeline` DAG to load and validate the data. I used dbt to build the transformation models and run tests.

| Layer  | ClickHouse table                    | Purpose                             |
| ------ | ----------------------------------- | ----------------------------------- |
| Bronze | `bronze.co2_emissions`              | Stores the source data              |
| Silver | `analytics_silver.co2_emissions`    | Holds transformed emissions data    |
| Gold   | `analytics_gold.annual_co2_summary` | Contains annual CO₂ summary results |

## Tools I Used

* Apache NiFi
* Apache Airflow
* ClickHouse
* dbt
* Docker Compose
* Python
* SQL
* Pandas

## Project Structure

```text
data-pipelines/
├── docker-compose.yml
├── README.md
├── airflow/
│   └── dags/
│       ├── co2_batch_pipeline.py
│       ├── earthquake_pipeline.py
│       ├── load_co2_staging.py
│       ├── requirements.txt
│       └── co2_analytics/
│           ├── dbt_project.yml
│           ├── profiles.example.yml
│           └── models/
│               └── example/
│                   ├── marts/
│                   │   └── gold_annual_co2_summary.sql
│                   └── staging/
│                       └── silver_co2_emissions.sql
├── clickhouse/
│   └── init/
│       ├── 01_create_tables.sql
│       └── 02_create_co2_tables.sql
├── data/
│   └── owid-co2-data.csv
└── nifi/
    └── clickhouse-jdbc.jar
```

## Running the Project

I ran the project locally using Docker Compose from the project root.

### Start the services

```powershell
docker compose up -d
docker compose ps
```

### Local interfaces

* **Airflow:** http://localhost:8085
* **NiFi:** https://localhost:8443/nifi
* **ClickHouse HTTP:** http://localhost:8124

### Start the NiFi flow

In NiFi, I started the processors from the database destination backwards toward the HTTP source:

1. `PutDatabaseRecord`
2. `UpdateAttribute`
3. `JoltTransformJSON`
4. `SplitJson`
5. `InvokeHTTP`

### Check the Airflow DAGs

List available DAGs:

```powershell
docker exec data-pipelines-airflow airflow dags list
```

Trigger the earthquake refresh:

```powershell
docker exec data-pipelines-airflow airflow dags trigger earthquake_pipeline
```

Trigger the CO₂ batch workflow:

```powershell
docker exec data-pipelines-airflow airflow dags trigger co2_batch_pipeline
```

Check the earthquake pipeline runs:

```powershell
docker exec data-pipelines-airflow airflow dags list-runs -d earthquake_pipeline
```

Check the CO₂ pipeline runs:

```powershell
docker exec data-pipelines-airflow airflow dags list-runs -d co2_batch_pipeline
```

### Run the dbt models and tests

Run the models:

```powershell
docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt run --profiles-dir ."
```

Run the tests:

```powershell
docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt test --profiles-dir ."
```

## Validation Results

These are the results I observed during testing on October 26, 2026. Earthquake counts can change because the source feed is live.

### Earthquake Pipeline

| Check                                                 |                  Result |
| ----------------------------------------------------- | ----------------------: |
| Bronze records                                        |                      70 |
| Silver records                                        |                      70 |
| Gold summary rows                                     |                       6 |
| Unique earthquake IDs in the deduplicated Bronze read |                      70 |
| Latest ingestion timestamp observed                   | 2026-10-26 06:25:29 UTC |
| Recent Airflow runs                                   |              Successful |

Bronze and Silver had matching record counts in the check. Gold has fewer rows because it groups the events into date and magnitude summaries.

### CO₂ Batch Pipeline

| Check             |                                      Result |
| ----------------- | ------------------------------------------: |
| Bronze records    |                                      50,411 |
| Silver records    |                                      50,411 |
| Gold summary rows |                                         275 |
| dbt models        |             4 succeeded in the recorded run |
| dbt tests         |           4 passed in the recorded test run |
| Airflow           | A scheduled CO₂ task completed successfully |

## Queries I Used to Check the Tables

These commands use the `CLICKHOUSE_PASSWORD` environment variable in the current PowerShell session. Set it before running the queries if it is not already defined.

```powershell
$env:CLICKHOUSE_PASSWORD = Read-Host "Enter ClickHouse password"
```

### Earthquake counts

```powershell
docker exec data-pipelines-clickhouse clickhouse-client --user pipeline_user --password "$env:CLICKHOUSE_PASSWORD" --query "SELECT 'Bronze' AS layer, count() AS records FROM bronze.earthquakes_dedup FINAL UNION ALL SELECT 'Silver', count() FROM silver.earthquakes UNION ALL SELECT 'Gold', count() FROM gold.daily_earthquake_summary"
```

### CO₂ counts

```powershell
docker exec data-pipelines-clickhouse clickhouse-client --user pipeline_user --password "$env:CLICKHOUSE_PASSWORD" --query "SELECT 'Bronze CO2' AS layer, count() AS records FROM bronze.co2_emissions UNION ALL SELECT 'Silver CO2', count() FROM analytics_silver.co2_emissions UNION ALL SELECT 'Gold CO2', count() FROM analytics_gold.annual_co2_summary"
```

## Data Sources

* [USGS Earthquake GeoJSON Feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson)
* [Our World in Data CO₂ Dataset](https://github.com/owid/co2-data)
