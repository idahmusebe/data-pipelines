"""
CO2 Batch Pipeline
------------------
Loads Our World in Data CO2 emissions into ClickHouse Bronze.

Pipeline:
1. Read the source CSV in chunks.
2. Load data into a staging table.
3. Validate row counts and unique country-year combinations.
4. Atomically exchange staging with Bronze.
5. Validate the active Bronze table.
6. Roll back the exchange if final validation fails.

The existing backup table is preserved.
"""

from datetime import datetime
import os

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DATA_FILE = "/opt/airflow/data/owid-co2-data.csv"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "pipeline_user"
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CLICKHOUSE_DATABASE = "bronze"

BRONZE_TABLE = "bronze.co2_emissions"
STAGING_TABLE = "bronze.co2_emissions_staging"
BACKUP_TABLE = "bronze.co2_emissions_backup"

BATCH_SIZE = 5000
EXPECTED_ROWS = 50411

COLUMNS = [
    "country",
    "year",
    "iso_code",
    "population",
    "gdp",
    "co2",
    "co2_per_capita",
    "co2_growth_abs",
    "co2_growth_prct",
    "coal_co2",
    "oil_co2",
    "gas_co2",
    "cement_co2",
    "land_use_change_co2",
    "total_ghg",
    "methane",
    "nitrous_oxide",
]


# --------------------------------------------------
# Ingestion and validation
# --------------------------------------------------

def load_co2_data():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    swapped = False

    try:
        # 1. Confirm the existing backup is available.
        backup_count = client.query(
            f"SELECT count() FROM {BACKUP_TABLE}"
        ).result_rows[0][0]

        if backup_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"Backup validation failed. "
                f"Expected {EXPECTED_ROWS} rows, found {backup_count}. "
                "Pipeline stopped without changing Bronze."
            )

        # 2. Ensure the staging table exists with the same structure.
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_TABLE}
            AS {BRONZE_TABLE}
            ENGINE = MergeTree
            ORDER BY (country, year)
            """
        )

        # 3. Clear staging only. The active Bronze table is untouched.
        client.command(
            f"TRUNCATE TABLE {STAGING_TABLE}"
        )

        # 4. Load the CSV into staging in manageable chunks.
        rows_loaded = 0

        for chunk in pd.read_csv(
            DATA_FILE,
            usecols=COLUMNS,
            chunksize=BATCH_SIZE,
        ):
            chunk = chunk.astype(object).where(
                pd.notna(chunk),
                None,
            )

            chunk["year"] = chunk["year"].astype(int)

            client.insert_df(
                STAGING_TABLE,
                chunk,
                database=CLICKHOUSE_DATABASE,
            )

            rows_loaded += len(chunk)

        print(f"Rows read from CSV: {rows_loaded}")

        # 5. Validate staging before replacing Bronze.
        staged_count = client.query(
            f"SELECT count() FROM {STAGING_TABLE}"
        ).result_rows[0][0]

        staged_unique_keys = client.query(
            f"""
            SELECT uniqExact(tuple(country, year))
            FROM {STAGING_TABLE}
            """
        ).result_rows[0][0]

        print(f"Rows in staging: {staged_count}")
        print(f"Unique country-year combinations: {staged_unique_keys}")

        if rows_loaded != EXPECTED_ROWS:
            raise RuntimeError(
                f"CSV row count mismatch: expected {EXPECTED_ROWS}, "
                f"read {rows_loaded}. Bronze remains unchanged."
            )

        if staged_count != EXPECTED_ROWS:
            raise RuntimeError(
                f"Staging row count mismatch: expected {EXPECTED_ROWS}, "
                f"found {staged_count}. Bronze remains unchanged."
            )

        if staged_unique_keys != staged_count:
            raise RuntimeError(
                "Duplicate country-year combinations detected in staging. "
                "Bronze remains unchanged."
            )

        print("Staging validation passed.")

        # 6. Atomically exchange the validated staging table with Bronze.
        # The previous Bronze table will take the staging table's name.
        client.command(
            f"EXCHANGE TABLES {BRONZE_TABLE} AND {STAGING_TABLE}"
        )

        swapped = True

        print("Atomic table exchange completed.")

        # 7. Validate the newly active Bronze table.
        final_count = client.query(
            f"SELECT count() FROM {BRONZE_TABLE}"
        ).result_rows[0][0]

        final_unique_keys = client.query(
            f"""
            SELECT uniqExact(tuple(country, year))
            FROM {BRONZE_TABLE}
            """
        ).result_rows[0][0]

        print(f"Final Bronze row count: {final_count}")
        print(f"Final Bronze unique keys: {final_unique_keys}")

        if (
            final_count != EXPECTED_ROWS
            or final_unique_keys != final_count
        ):
            raise RuntimeError(
                f"Final Bronze validation failed: "
                f"rows={final_count}, unique keys={final_unique_keys}."
            )

        # 8. Success: leave the previous Bronze table in staging.
        # The separate backup table remains untouched.
        swapped = False

        print(
            "CO2 ingestion completed successfully. "
            f"Bronze contains {final_count} rows and "
            f"{final_unique_keys} unique country-year combinations."
        )

    except Exception:
        # If the exchange occurred but final validation failed,
        # exchange the tables back to restore the previous Bronze data.
        if swapped:
            try:
                client.command(
                    f"EXCHANGE TABLES {BRONZE_TABLE} AND {STAGING_TABLE}"
                )
                print(
                    "Rollback successful. "
                    "The previous Bronze table is active again."
                )
            except Exception as rollback_error:
                print(
                    "CRITICAL: Automatic rollback failed. "
                    "Do not rerun the pipeline until the table state "
                    f"has been checked. Details: {rollback_error}"
                )

        raise

    finally:
        client.close()


# --------------------------------------------------
# Airflow DAG
# --------------------------------------------------

with DAG(
    dag_id="co2_batch_pipeline",
    description=(
        "Load and validate Our World in Data CO2 emissions "
        "using ClickHouse staging and atomic table exchange."
    ),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["batch", "clickhouse", "co2"],
) as dag:

    ingest_co2_data = PythonOperator(
        task_id="load_co2_data_to_bronze",
        python_callable=load_co2_data,
    )