"""
Baserow Sales Data Pipeline
---------------------------
Extracts Customers, Products and Orders from Baserow,
loads them into ClickHouse Bronze, transforms them through
Silver and Gold using dbt, and validates data quality.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dags/co2_analytics"
INGESTION_SCRIPT = "/opt/airflow/dags/ingest_baserow.py"


with DAG(
    dag_id="baserow_pipeline",
    description="Baserow to ClickHouse Bronze, Silver and Gold pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["baserow", "clickhouse", "bronze", "silver", "gold", "dbt"],
) as dag:

    ingest_baserow = BashOperator(
        task_id="ingest_baserow_to_bronze",
        bash_command=f"set -e && python {INGESTION_SCRIPT}",
    )

    run_dbt_transformations = BashOperator(
        task_id="run_baserow_dbt_transformations",
        bash_command=(
            "set -e && "
            f"dbt run --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            "--select path:models/baserow"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_baserow_dbt_tests",
        bash_command=(
            "set -e && "
            f"dbt test --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            "--select path:models/baserow"
        ),
    )

    ingest_baserow >> run_dbt_transformations >> run_dbt_tests