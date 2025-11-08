"""
Simplified Airflow DAG for running dbt models on BigQuery.

This is a simpler version that runs all dbt models in a single task.
Useful for smaller projects or as a starting point.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# DAG configuration
DAG_ID = "dbt_simple_pipeline"
DBT_PROJECT_DIR = Path("/home/user/dbt-airflow-2.9.1-python-3.11/dbt_project")
DBT_PROFILES_DIR = Path("/home/user/dbt-airflow-2.9.1-python-3.11/dbt_project")

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Simple dbt pipeline for BigQuery",
    schedule_interval="0 8 * * *",  # Run daily at 8 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bigquery", "simple"],
) as dag:

    # Run all dbt models
    dbt_run_all = BashOperator(
        task_id="dbt_run_all",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt deps --profiles-dir {DBT_PROFILES_DIR} && \
            dbt seed --profiles-dir {DBT_PROFILES_DIR} --target prod && \
            dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod && \
            dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod
        """,
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('US', true) }}",
        },
    )
