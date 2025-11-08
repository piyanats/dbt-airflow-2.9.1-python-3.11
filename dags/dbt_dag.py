"""
Airflow DAG for running dbt models on BigQuery.

This DAG is designed to run on GKE with workload identities and uses
service account credentials from Airflow connections.

Requirements:
- Airflow 2.9.1
- Python 3.11
- dbt-core with dbt-bigquery adapter
- GKE with workload identity enabled
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago

# DAG configuration
DAG_ID = "dbt_bigquery_pipeline"
DBT_PROJECT_DIR = Path("/home/user/dbt-airflow-2.9.1-python-3.11/dbt_project")
DBT_PROFILES_DIR = Path("/home/user/dbt-airflow-2.9.1-python-3.11/dbt_project")

# BigQuery connection ID configured in Airflow
# This connection should have the service account credentials
BIGQUERY_CONN_ID = "bigquery_default"

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def setup_gcp_credentials(**context):
    """
    Set up GCP credentials from Airflow connection.

    This function extracts the service account credentials from the Airflow
    BigQuery connection and sets up the environment for dbt to use them.
    """
    hook = BigQueryHook(gcp_conn_id=BIGQUERY_CONN_ID, use_legacy_sql=False)

    # Get the credentials from the connection
    credentials = hook.get_credentials()
    project_id = hook.project_id

    # Set environment variables for dbt
    os.environ["GCP_PROJECT_ID"] = project_id
    os.environ["DBT_DATASET"] = "analytics"
    os.environ["GCP_LOCATION"] = "asia-southeast1"

    # If using service account key file
    if hasattr(credentials, 'service_account_email'):
        # For workload identity, set the service account to impersonate
        os.environ["DBT_SERVICE_ACCOUNT"] = credentials.service_account_email

    # Get the key file path if it exists
    extras = hook.get_connection(BIGQUERY_CONN_ID).extra_dejson
    if "key_path" in extras:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = extras["key_path"]

    print(f"GCP Project ID: {project_id}")
    print(f"BigQuery Dataset: {os.environ.get('DBT_DATASET')}")
    print("GCP credentials configured successfully")


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Run dbt models on BigQuery",
    schedule_interval="0 6 * * *",  # Run daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bigquery", "data-pipeline"],
    max_active_runs=1,
) as dag:

    # Task 1: Setup GCP credentials from Airflow connection
    setup_credentials = PythonOperator(
        task_id="setup_gcp_credentials",
        python_callable=setup_gcp_credentials,
        doc_md="""
        ## Setup GCP Credentials

        This task extracts service account credentials from the Airflow BigQuery
        connection and configures the environment for dbt to use them.
        """,
    )

    # Task 2: dbt debug - verify connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
        env={
            "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "DBT_DATASET": "{{ var.value.dbt_dataset | default('analytics', true) }}",
            "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
        },
        doc_md="""
        ## dbt Debug

        Verify the dbt configuration and BigQuery connection.
        """,
    )

    # Task 3: dbt deps - install dependencies
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
        doc_md="""
        ## dbt Dependencies

        Install dbt package dependencies defined in packages.yml.
        """,
    )

    # Task 4: dbt seed - load seed data
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROFILES_DIR} --target prod",
        doc_md="""
        ## dbt Seed

        Load CSV files from the seeds directory into BigQuery.
        """,
    )

    # Task 5: dbt run - build models (staging)
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod --models staging",
        doc_md="""
        ## dbt Run Staging

        Build staging models in BigQuery.
        """,
    )

    # Task 6: dbt test - test staging models
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod --models staging",
        doc_md="""
        ## dbt Test Staging

        Run tests on staging models.
        """,
    )

    # Task 7: dbt run - build models (marts)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target prod --models marts",
        doc_md="""
        ## dbt Run Marts

        Build marts models in BigQuery.
        """,
    )

    # Task 8: dbt test - test marts models
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target prod --models marts",
        doc_md="""
        ## dbt Test Marts

        Run tests on marts models.
        """,
    )

    # Task 9: dbt docs generate
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --target prod",
        doc_md="""
        ## dbt Docs Generate

        Generate dbt documentation.
        """,
    )

    # Define task dependencies
    setup_credentials >> dbt_debug >> dbt_deps >> dbt_seed
    dbt_seed >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_marts >> dbt_test_marts
    dbt_test_marts >> dbt_docs_generate
