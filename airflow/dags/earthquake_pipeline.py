"""
Earthquake pipeline: refresh Silver and Gold from deduplicated Bronze data.
Staging tables hold the previous active data after a successful exchange.
"""
from datetime import datetime
import os

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "pipeline_user"
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

SILVER = "silver.earthquakes"
SILVER_STAGE = "silver.earthquakes_refresh"
GOLD = "gold.daily_earthquake_summary"
GOLD_STAGE = "gold.daily_earthquake_summary_refresh"


def refresh_earthquake_layers():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    try:
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {SILVER_STAGE}
            AS {SILVER}
            ENGINE = MergeTree
            ORDER BY (event_time, earthquake_id)
        """)
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {GOLD_STAGE}
            AS {GOLD}
            ENGINE = MergeTree
            ORDER BY (event_date, magnitude_category)
        """)

        # Build and validate Silver staging from deduplicated Bronze.
        client.command(f"TRUNCATE TABLE {SILVER_STAGE}")
        client.command(f"""
            INSERT INTO {SILVER_STAGE}
            SELECT earthquake_id, magnitude, place, event_time,
                   longitude, latitude, depth_km, event_type, source_url,
                   ingested_at
            FROM bronze.earthquakes_dedup FINAL
        """)

        bronze_count = client.query(
            "SELECT count() FROM bronze.earthquakes_dedup FINAL"
        ).result_rows[0][0]
        silver_count = client.query(
            f"SELECT count() FROM {SILVER_STAGE}"
        ).result_rows[0][0]
        silver_unique = client.query(
            f"SELECT uniqExact(earthquake_id) FROM {SILVER_STAGE}"
        ).result_rows[0][0]

        if silver_count != bronze_count or silver_unique != silver_count:
            raise RuntimeError(
                f"Silver validation failed: Bronze={bronze_count}, "
                f"Silver={silver_count}, unique IDs={silver_unique}"
            )

        # Build Gold staging from validated Silver staging.
        client.command(f"TRUNCATE TABLE {GOLD_STAGE}")
        client.command(f"""
            INSERT INTO {GOLD_STAGE}
            SELECT
                toDate(event_time) AS event_date,
                multiIf(
                    isNull(magnitude), 'Unknown',
                    magnitude < 2, 'Minor',
                    magnitude < 4, 'Light',
                    magnitude < 5, 'Moderate',
                    magnitude < 6, 'Strong',
                    'Major'
                ) AS magnitude_category,
                count() AS earthquake_count,
                avg(magnitude) AS average_magnitude,
                max(magnitude) AS maximum_magnitude,
                now64(3) AS ingested_at
            FROM {SILVER_STAGE}
            GROUP BY event_date, magnitude_category
        """)

        gold_count = client.query(
            f"SELECT count() FROM {GOLD_STAGE}"
        ).result_rows[0][0]
        expected_gold_count = client.query(f"""
            SELECT count()
            FROM (
                SELECT event_date, magnitude_category
                FROM (
                    SELECT
                        toDate(event_time) AS event_date,
                        multiIf(
                            isNull(magnitude), 'Unknown',
                            magnitude < 2, 'Minor',
                            magnitude < 4, 'Light',
                            magnitude < 5, 'Moderate',
                            magnitude < 6, 'Strong',
                            'Major'
                        ) AS magnitude_category
                    FROM {SILVER_STAGE}
                )
                GROUP BY event_date, magnitude_category
            )
        """).result_rows[0][0]

        if gold_count != expected_gold_count:
            raise RuntimeError(
                f"Gold validation failed: expected {expected_gold_count}, "
                f"found {gold_count}"
            )

        # Publish validated tables. Exchanges preserve old tables as staging.
        client.command(f"EXCHANGE TABLES {SILVER} AND {SILVER_STAGE}")
        try:
            client.command(f"EXCHANGE TABLES {GOLD} AND {GOLD_STAGE}")
        except Exception:
            client.command(f"EXCHANGE TABLES {SILVER} AND {SILVER_STAGE}")
            raise

        active_silver = client.query(
            f"SELECT count() FROM {SILVER}"
        ).result_rows[0][0]
        active_gold = client.query(
            f"SELECT count() FROM {GOLD}"
        ).result_rows[0][0]

        if active_silver != bronze_count or active_gold != gold_count:
            client.command(f"EXCHANGE TABLES {GOLD} AND {GOLD_STAGE}")
            client.command(f"EXCHANGE TABLES {SILVER} AND {SILVER_STAGE}")
            raise RuntimeError(
                "Post-publish validation failed; previous tables were restored."
            )

        print(
            f"Earthquake refresh successful: Bronze={bronze_count}, "
            f"Silver={active_silver}, Gold summary rows={active_gold}. "
            "Previous tables remain under the refresh table names."
        )
    finally:
        client.close()


with DAG(
    dag_id="earthquake_pipeline",
    description="Refresh and validate earthquake Silver and Gold layers",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "earthquakes", "clickhouse"],
) as dag:
    refresh_layers = PythonOperator(
        task_id="refresh_silver_and_gold",
        python_callable=refresh_earthquake_layers,
    )
