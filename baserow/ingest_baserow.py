import os
import json
import logging
import uuid
from datetime import datetime, timezone

import requests
import clickhouse_connect


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

BASEROW_API_TOKEN = os.getenv("BASEROW_API_TOKEN")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

BASEROW_TABLES = {
    "customers": 1291093,
    "products": 1291171,
    "orders": 1291067,
}

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "pipeline_user"
CLICKHOUSE_DATABASE = "bronze"

STAGING_PREFIX = "baserow_staging"


# --------------------------------------------------
# LOGGING
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# VALIDATE CONFIGURATION
# --------------------------------------------------

if not BASEROW_API_TOKEN:
    raise ValueError("BASEROW_API_TOKEN is missing.")

if not CLICKHOUSE_PASSWORD:
    raise ValueError("CLICKHOUSE_PASSWORD is missing.")


# --------------------------------------------------
# BASEROW API EXTRACTION
# --------------------------------------------------

def fetch_baserow_records(table_id):
    """Fetch all records from Baserow, including pagination."""

    headers = {
        "Authorization": f"Token {BASEROW_API_TOKEN}"
    }

    url = (
        f"https://api.baserow.io/api/database/rows/table/"
        f"{table_id}/?user_field_names=true&size=200"
    )

    records = []

    while url:
        response = requests.get(
            url,
            headers=headers,
            timeout=60
        )

        response.raise_for_status()

        payload = response.json()

        records.extend(payload.get("results", []))

        url = payload.get("next")

    return records


# --------------------------------------------------
# CLICKHOUSE CONNECTION
# --------------------------------------------------

def get_clickhouse_client():

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )


# --------------------------------------------------
# CREATE BRONZE TABLES
# --------------------------------------------------

def create_bronze_tables(client):

    client.command(
        f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}"
    )

    for table_name in BASEROW_TABLES:

        bronze_table = f"baserow_{table_name}_raw"

        query = f"""
        CREATE TABLE IF NOT EXISTS bronze.{bronze_table}
        (
            baserow_id UInt64,
            source_table String,
            raw_data String,
            ingested_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY baserow_id
        """

        client.command(query)

        logger.info(
            "Verified Bronze table: %s",
            bronze_table
        )


# --------------------------------------------------
# CREATE STAGING TABLE
# --------------------------------------------------

def create_staging_table(client, staging_table):

    query = f"""
    CREATE TABLE bronze.{staging_table}
    (
        baserow_id UInt64,
        source_table String,
        raw_data String,
        ingested_at DateTime64(3)
    )
    ENGINE = MergeTree
    ORDER BY baserow_id
    """

    client.command(query)

    logger.info(
        "Created staging table: %s",
        staging_table
    )


# --------------------------------------------------
# LOAD RECORDS INTO STAGING
# --------------------------------------------------

def load_into_staging(
    client,
    table_name,
    records,
    staging_table
):

    ingestion_time = datetime.now(timezone.utc)

    rows = [
        [
            int(record["id"]),
            table_name,
            json.dumps(record, ensure_ascii=False),
            ingestion_time
        ]
        for record in records
    ]

    if not rows:
        raise ValueError(
            f"No records found for {table_name}. "
            "Existing Bronze data will not be replaced."
        )

    client.insert(
        f"bronze.{staging_table}",
        rows,
        column_names=[
            "baserow_id",
            "source_table",
            "raw_data",
            "ingested_at"
        ]
    )

    staged_count = client.query(
        f"SELECT count() FROM bronze.{staging_table}"
    ).result_rows[0][0]

    if staged_count != len(records):
        raise ValueError(
            f"Validation failed for {table_name}. "
            f"Expected {len(records)}, loaded {staged_count}."
        )

    logger.info(
        "Validated %s records in %s",
        staged_count,
        staging_table
    )


# --------------------------------------------------
# REPLACE BRONZE TABLE
# --------------------------------------------------

def replace_bronze_table(
    client,
    table_name,
    staging_table
):

    bronze_table = f"baserow_{table_name}_raw"

    client.command(
        f"""
        EXCHANGE TABLES
        bronze.{bronze_table}
        AND bronze.{staging_table}
        """
    )

    logger.info(
        "Successfully replaced Bronze table: %s",
        bronze_table
    )


# --------------------------------------------------
# CLEANUP STAGING TABLES
# --------------------------------------------------

def cleanup_staging_tables(client, staging_tables):

    for staging_table in staging_tables:

        try:

            client.command(
                f"DROP TABLE IF EXISTS bronze.{staging_table}"
            )

            logger.info(
                "Cleaned up staging table: %s",
                staging_table
            )

        except Exception:

            logger.exception(
                "Failed to clean staging table: %s",
                staging_table
            )


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def main():

    client = get_clickhouse_client()

    staging_tables = {}

    try:

        create_bronze_tables(client)

        # Extract all three tables first.
        extracted_data = {}

        for table_name, table_id in BASEROW_TABLES.items():

            logger.info(
                "Extracting Baserow table: %s",
                table_name
            )

            records = fetch_baserow_records(table_id)

            if not records:
                raise ValueError(
                    f"Extraction returned zero records "
                    f"for {table_name}. Aborting."
                )

            extracted_data[table_name] = records

            logger.info(
                "Extracted %s records from %s",
                len(records),
                table_name
            )

        # Create and populate staging tables.
        run_id = uuid.uuid4().hex[:12]

        for table_name, records in extracted_data.items():

            staging_table = (
                f"{STAGING_PREFIX}_{table_name}_{run_id}"
            )

            staging_tables[table_name] = staging_table

            create_staging_table(
                client,
                staging_table
            )

            load_into_staging(
                client,
                table_name,
                records,
                staging_table
            )

        # Replace existing Bronze tables only after
        # all three staging tables pass validation.
        for table_name, staging_table in staging_tables.items():

            replace_bronze_table(
                client,
                table_name,
                staging_table
            )

        logger.info(
            "Baserow Bronze ingestion completed successfully."
        )

    except Exception:

        logger.exception(
            "Baserow ingestion failed."
        )

        raise

    finally:

        cleanup_staging_tables(
            client,
            staging_tables.values()
        )

        client.close()


if __name__ == "__main__":
    main()