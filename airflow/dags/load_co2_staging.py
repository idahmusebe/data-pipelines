import pandas as pd
import os
import clickhouse_connect

DATA_FILE = "/opt/airflow/data/owid-co2-data.csv"
BATCH_SIZE = 5000

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


def main():
    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="pipeline_user",
       password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    try:
        # Start with an empty staging table.
        client.command(
            "TRUNCATE TABLE bronze.co2_emissions_staging"
        )

        rows_loaded = 0

        for chunk in pd.read_csv(
            DATA_FILE,
            usecols=COLUMNS,
            chunksize=BATCH_SIZE,
        ):
            chunk = chunk.astype(object).where(
                pd.notna(chunk), None
            )
            chunk["year"] = chunk["year"].astype(int)

            client.insert_df(
                "bronze.co2_emissions_staging",
                chunk,
                database="bronze",
            )

            rows_loaded += len(chunk)

        staged_count = client.query(
            "SELECT count() FROM bronze.co2_emissions_staging"
        ).result_rows[0][0]

        unique_keys = client.query(
            """
            SELECT uniqExact(tuple(country, year))
            FROM bronze.co2_emissions_staging
            """
        ).result_rows[0][0]

        print(f"Rows read: {rows_loaded}")
        print(f"Rows staged: {staged_count}")
        print(f"Unique country-year combinations: {unique_keys}")

        if staged_count != rows_loaded:
            raise RuntimeError("Staging row count mismatch.")

        if unique_keys != staged_count:
            raise RuntimeError("Duplicate country-year keys detected.")

        if staged_count != 50411:
            raise RuntimeError(
                f"Expected 50411 rows, found {staged_count}."
            )

        print("Staging validation passed.")

    finally:
        client.close()


if __name__ == "__main__":
    main()