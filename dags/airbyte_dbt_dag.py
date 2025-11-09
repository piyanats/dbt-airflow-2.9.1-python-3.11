"""
Airflow DAG for Airbyte + dbt ELT Pipeline.

This DAG demonstrates the modern ELT pattern:
1. Airbyte extracts and loads data from sources to BigQuery (EL)
2. dbt transforms the data in BigQuery (T)

Flow: Airbyte Sync → dbt Run → dbt Test

Requirements:
- Airbyte instance running and accessible
- Airbyte connection configured
- dbt project ready
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# DAG configuration
DAG_ID = "airbyte_dbt_elt_pipeline"
DBT_PROJECT_DIR = Path("/opt/airflow/dbt_project")

# Airbyte configuration
# You can configure multiple connections for different data sources
AIRBYTE_CONNECTIONS = {
    "sales_data": {
        "connection_id": "your-sales-connection-id",  # From Airbyte UI
        "description": "Sync sales data from PostgreSQL to BigQuery",
    },
    "marketing_data": {
        "connection_id": "your-marketing-connection-id",  # From Airbyte UI
        "description": "Sync marketing data from Google Ads to BigQuery",
    },
}

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}


def check_airbyte_job_status(**context):
    """
    Check Airbyte job status and log results.
    This is optional - you can use AirbyteJobSensor instead.
    """
    job_id = context["task_instance"].xcom_pull(task_ids="sync_sales_data")
    print(f"Airbyte job ID: {job_id}")
    print("Airbyte sync completed successfully!")


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="ELT pipeline: Airbyte extracts/loads data, dbt transforms it",
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["elt", "airbyte", "dbt", "bigquery"],
    max_active_runs=1,
) as dag:

    # ========================================================================
    # Step 1: Extract and Load data using Airbyte
    # ========================================================================

    # Sync sales data from source to BigQuery
    sync_sales_data = AirbyteTriggerSyncOperator(
        task_id="sync_sales_data",
        airbyte_conn_id="airbyte_default",  # Airflow connection to Airbyte
        connection_id=AIRBYTE_CONNECTIONS["sales_data"]["connection_id"],
        asynchronous=False,  # Wait for sync to complete
        timeout=3600,  # 1 hour timeout
        wait_seconds=10,  # Check status every 10 seconds
    )

    # Sync marketing data from source to BigQuery
    sync_marketing_data = AirbyteTriggerSyncOperator(
        task_id="sync_marketing_data",
        airbyte_conn_id="airbyte_default",
        connection_id=AIRBYTE_CONNECTIONS["marketing_data"]["connection_id"],
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

    # Optional: Check sync status
    check_sync_status = PythonOperator(
        task_id="check_sync_status",
        python_callable=check_airbyte_job_status,
        provide_context=True,
    )

    # ========================================================================
    # Step 2: Transform data using dbt
    # ========================================================================

    # Install dbt dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Run dbt models (staging layer)
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --models staging --target prod",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
    )

    # Test dbt models (staging layer)
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR} --models staging --target prod",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
    )

    # Run dbt models (marts layer)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --models marts --target prod",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
    )

    # Test dbt models (marts layer)
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR} --models marts --target prod",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
    )

    # Generate dbt documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROJECT_DIR} --target prod",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
    )

    # ========================================================================
    # Define task dependencies
    # ========================================================================

    # Airbyte syncs run in parallel
    [sync_sales_data, sync_marketing_data] >> check_sync_status

    # After Airbyte completes, run dbt
    check_sync_status >> dbt_deps >> dbt_run_staging >> dbt_test_staging

    # After staging is validated, run marts
    dbt_test_staging >> dbt_run_marts >> dbt_test_marts

    # Generate docs after everything completes
    dbt_test_marts >> dbt_docs


# Task flow visualization:
#
#     ┌─────────────────┐     ┌──────────────────────┐
#     │ sync_sales_data │     │ sync_marketing_data  │
#     └────────┬────────┘     └──────────┬───────────┘
#              │                         │
#              └──────────┬──────────────┘
#                         ▼
#              ┌──────────────────────┐
#              │ check_sync_status    │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │      dbt_deps        │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │  dbt_run_staging     │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │  dbt_test_staging    │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │   dbt_run_marts      │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │  dbt_test_marts      │
#              └──────────┬───────────┘
#                         ▼
#              ┌──────────────────────┐
#              │  dbt_docs_generate   │
#              └──────────────────────┘
