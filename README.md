# Data Pipelines: Earthquakes, CO₂ Emissions and Baserow Sales

This project contains three data pipelines built to explore real-time ingestion, batch processing, and API-based data integration. I used Apache NiFi, Apache Airflow, ClickHouse, dbt, Python, and Docker Compose to build workflows that move data from different sources through Bronze, Silver, and Gold layers.

The three pipelines are:

1. A real-time earthquake pipeline using the USGS GeoJSON feed.
2. A batch CO₂ emissions pipeline using the Our World in Data dataset.
3. A Baserow Sales pipeline that extracts customer, product, and order data through the Baserow API.

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

### 3. Baserow Sales Data Pipeline

The Baserow pipeline integrates sales management data from Baserow using its REST API.

The source database contains three related tables:

* **Customers:** Customer IDs, names, email addresses, locations, and registration dates.
* **Products:** Product IDs, names, categories, unit prices, and stock quantities.
* **Orders:** Order IDs, dates, quantities, order statuses, and linked customer and product records.

Python and Requests handle API extraction and pagination. The ingestion script loads the extracted records into ClickHouse Bronze tables.

The pipeline uses an idempotent ingestion approach. It creates staging tables, inserts and validates the extracted records, and then exchanges the staging tables with the existing Bronze tables. This prevents repeated pipeline runs from simply appending duplicate source records.

Airflow orchestrates the workflow through the `baserow_pipeline` DAG.

The workflow is:

`Baserow API → Python Ingestion → ClickHouse Bronze → dbt Silver → dbt Gold → dbt Tests`

| Layer  | ClickHouse table                            | Purpose                                   |
| ------ | ------------------------------------------- | ----------------------------------------- |
| Bronze | `bronze.baserow_customers_raw`              | Stores extracted customer records         |
| Bronze | `bronze.baserow_products_raw`               | Stores extracted product records          |
| Bronze | `bronze.baserow_orders_raw`                 | Stores extracted order records            |
| Silver | `analytics_silver.stg_baserow_customers`    | Cleans and deduplicates customer records  |
| Silver | `analytics_silver.stg_baserow_products`     | Cleans and deduplicates product records   |
| Silver | `analytics_silver.stg_baserow_orders`       | Transforms and deduplicates order records |
| Gold   | `analytics_gold.gold_baserow_sales_summary` | Produces an aggregated sales summary      |

The Gold model combines order, customer, and product information to provide sales summaries by order date, status, and product category.

## Tools I Used

* Apache NiFi
* Apache Airflow
* ClickHouse
* dbt
* Docker Compose
* Python
* SQL
* Pandas
* Requests
* Baserow REST API

## Project Structure

```text
data-pipelines/
├── docker-compose.yml
├── README.md
├── airflow/
│   └── dags/
│       ├── earthquake_pipeline.py
│       ├── co2_batch_pipeline.py
│       ├── baserow_pipeline.py
│       ├── ingest_baserow.py
│       ├── inspect_baserow.py
│       ├── load_co2_staging.py
│       └── co2_analytics/
│           ├── dbt_project.yml
│           ├── profiles.example.yml
│           └── models/
│               ├── example/
│               └── baserow/
│                   ├── sources.yml
│                   ├── staging/
│                   │   ├── stg_baserow_customers.sql
│                   │   ├── stg_baserow_products.sql
│                   │   └── stg_baserow_orders.sql
│                   └── marts/
│                       └── gold_baserow_sales_summary.sql
├── baserow/
│   ├── ingest_baserow.py
│   └── inspect_baserow.py
├── clickhouse/
│   └── init/
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

Trigger the Baserow pipeline:

```powershell
docker exec data-pipelines-airflow airflow dags trigger baserow_pipeline
```

Check Baserow pipeline runs:

```powershell
docker exec data-pipelines-airflow airflow dags list-runs -d baserow_pipeline
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

Run the existing dbt models:

```powershell
docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt run --profiles-dir ."
```

Run the tests:

```powershell
docker exec data-pipelines-airflow bash -lc "cd /opt/airflow/dags/co2_analytics && dbt test --profiles-dir ."
```

To run only the Baserow models:

```powershell
docker exec data-pipelines-airflow bash -lc "dbt run --project-dir /opt/airflow/dags/co2_analytics --profiles-dir /opt/airflow/dags/co2_analytics --select path:models/baserow"
```

To test the Baserow models:

```powershell
docker exec data-pipelines-airflow bash -lc "dbt test --project-dir /opt/airflow/dags/co2_analytics --profiles-dir /opt/airflow/dags/co2_analytics --select path:models/baserow"
```

### Baserow configuration

The Baserow API token is supplied through an environment variable:

```text
BASEROW_API_TOKEN
```

The token should be configured in the local environment files and must not be committed to GitHub.

The Docker Compose configuration passes the token to Airflow through environment-variable substitution.

## Validation Results

These are results observed during testing on October 26–30, 2026. Earthquake counts can change because the source feed is live.

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

### Baserow Sales Pipeline

| Check                   |      Result |
| ----------------------- | ----------: |
| Customers extracted     |           5 |
| Products extracted      |           5 |
| Orders extracted        |           5 |
| Bronze customer records |           5 |
| Bronze product records  |           5 |
| Bronze order records    |           5 |
| dbt models              | 4 succeeded |
| dbt tests               |   14 passed |
| Airflow pipeline run    |  Successful |

The ingestion process was also tested more than once to verify that repeated runs did not append duplicate records to the Bronze tables.

The Gold summary contained five order records, with a gross order value of KES 128,900 across all order statuses, including cancelled orders. This is an order-value total, not realized revenue.

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

### Baserow counts

```powershell
docker exec data-pipelines-clickhouse clickhouse-client --user pipeline_user --password "$env:CLICKHOUSE_PASSWORD" --query "SELECT 'Customers' AS dataset, count() AS records FROM bronze.baserow_customers_raw UNION ALL SELECT 'Products', count() FROM bronze.baserow_products_raw UNION ALL SELECT 'Orders', count() FROM bronze.baserow_orders_raw"
```

## Data Sources

* [USGS Earthquake GeoJSON Feed](https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson)
* [Our World in Data CO₂ Dataset](https://github.com/owid/co2-data)
* [Baserow](https://baserow.io/)
