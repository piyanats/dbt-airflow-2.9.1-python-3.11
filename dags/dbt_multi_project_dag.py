"""
Airflow DAG for running multiple dbt projects on BigQuery.

This DAG demonstrates how to orchestrate multiple dbt projects in a single pipeline:
- Sales Analytics
- Marketing Analytics
- Finance Analytics

Each project runs independently and can have different schedules or dependencies.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# DAG configuration
DAG_ID = "dbt_multi_project_pipeline"
DBT_PROJECTS_DIR = Path("/opt/airflow/dbt_projects")

# BigQuery connection ID
BIGQUERY_CONN_ID = "bigquery_default"

# dbt projects configuration
DBT_PROJECTS = {
    "sales_analytics": {
        "project_dir": DBT_PROJECTS_DIR / "sales_analytics",
        "dataset_env_var": "SALES_DATASET",
        "dataset_default": "sales_analytics",
        "description": "Sales data models",
    },
    "marketing_analytics": {
        "project_dir": DBT_PROJECTS_DIR / "marketing_analytics",
        "dataset_env_var": "MARKETING_DATASET",
        "dataset_default": "marketing_analytics",
        "description": "Marketing data models",
    },
    "finance_analytics": {
        "project_dir": DBT_PROJECTS_DIR / "finance_analytics",
        "dataset_env_var": "FINANCE_DATASET",
        "dataset_default": "finance_analytics",
        "description": "Finance data models",
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
    "execution_timeout": timedelta(hours=2),
}


def setup_gcp_credentials(**context):
    """Set up GCP credentials from Airflow connection."""
    hook = BigQueryHook(gcp_conn_id=BIGQUERY_CONN_ID, use_legacy_sql=False)
    credentials = hook.get_credentials()
    project_id = hook.project_id

    # Set environment variables for dbt
    os.environ["GCP_PROJECT_ID"] = project_id
    os.environ["GCP_LOCATION"] = "asia-southeast1"

    # Set dataset environment variables for each project
    for project_name, config in DBT_PROJECTS.items():
        dataset = config["dataset_default"]
        os.environ[config["dataset_env_var"]] = dataset

    # Get the key file path if it exists
    extras = hook.get_connection(BIGQUERY_CONN_ID).extra_dejson
    if "key_path" in extras:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = extras["key_path"]

    print(f"GCP Project ID: {project_id}")
    for project_name, config in DBT_PROJECTS.items():
        print(f"{project_name}: {os.environ.get(config['dataset_env_var'])}")


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Run multiple dbt projects on BigQuery",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bigquery", "multi-project"],
    max_active_runs=1,
) as dag:

    # Setup GCP credentials (runs once for all projects)
    setup_credentials = PythonOperator(
        task_id="setup_gcp_credentials",
        python_callable=setup_gcp_credentials,
    )

    # Create task groups for each dbt project
    project_task_groups = []

    for project_name, config in DBT_PROJECTS.items():
        project_dir = config["project_dir"]
        dataset_env_var = config["dataset_env_var"]
        dataset_default = config["dataset_default"]

        with TaskGroup(
            group_id=f"{project_name}_pipeline",
            tooltip=config["description"],
        ) as project_group:

            # dbt deps - install dependencies
            dbt_deps = BashOperator(
                task_id="dbt_deps",
                bash_command=f"cd {project_dir} && dbt deps --profiles-dir {project_dir}",
            )

            # dbt run - build models
            dbt_run = BashOperator(
                task_id="dbt_run",
                bash_command=f"cd {project_dir} && dbt run --profiles-dir {project_dir} --target prod",
                env={
                    "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
                    dataset_env_var: f"{{{{ var.value.get('{dataset_env_var.lower()}', '{dataset_default}') }}}}",
                    "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
                },
            )

            # dbt test - run tests
            dbt_test = BashOperator(
                task_id="dbt_test",
                bash_command=f"cd {project_dir} && dbt test --profiles-dir {project_dir} --target prod",
                env={
                    "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
                    dataset_env_var: f"{{{{ var.value.get('{dataset_env_var.lower()}', '{dataset_default}') }}}}",
                    "GCP_LOCATION": "{{ var.value.gcp_location | default('asia-southeast1', true) }}",
                },
            )

            # Define task dependencies within the project
            dbt_deps >> dbt_run >> dbt_test

        project_task_groups.append(project_group)

    # Set up dependencies between setup and all project groups
    setup_credentials >> project_task_groups

    # Projects run in parallel after credentials setup
    # If you want sequential execution, use:
    # setup_credentials >> project_task_groups[0] >> project_task_groups[1] >> project_task_groups[2]
