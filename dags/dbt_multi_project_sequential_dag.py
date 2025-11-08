"""
Airflow DAG for running multiple dbt projects sequentially.

This DAG runs dbt projects in order with dependencies:
1. Sales Analytics (runs first)
2. Marketing Analytics (depends on sales)
3. Finance Analytics (depends on marketing)

Use this pattern when your projects have dependencies on each other.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# DAG configuration
DAG_ID = "dbt_multi_project_sequential"
DBT_PROJECTS_DIR = Path("/opt/airflow/dbt_projects")

# Projects in order of execution
PROJECTS_ORDER = [
    {
        "name": "sales_analytics",
        "dataset_var": "SALES_DATASET",
        "dataset_default": "sales_analytics",
    },
    {
        "name": "marketing_analytics",
        "dataset_var": "MARKETING_DATASET",
        "dataset_default": "marketing_analytics",
    },
    {
        "name": "finance_analytics",
        "dataset_var": "FINANCE_DATASET",
        "dataset_default": "finance_analytics",
    },
]

# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Run dbt projects sequentially",
    schedule_interval="0 8 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bigquery", "sequential"],
) as dag:

    previous_group = None

    for project in PROJECTS_ORDER:
        project_name = project["name"]
        project_dir = DBT_PROJECTS_DIR / project_name
        dataset_var = project["dataset_var"]
        dataset_default = project["dataset_default"]

        with TaskGroup(
            group_id=f"{project_name}",
            tooltip=f"Run {project_name} dbt models",
        ) as project_group:

            run_project = BashOperator(
                task_id="run_all",
                bash_command=f"""
                    cd {project_dir} && \
                    dbt deps --profiles-dir {project_dir} && \
                    dbt run --profiles-dir {project_dir} --target prod && \
                    dbt test --profiles-dir {project_dir} --target prod
                """,
                env={
                    "GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
                    dataset_var: f"{{{{ var.value.get('{dataset_var.lower()}', '{dataset_default}') }}}}",
                    "GCP_LOCATION": "{{ var.value.gcp_location | default('US', true) }}",
                },
            )

        # Set up sequential dependencies
        if previous_group is not None:
            previous_group >> project_group

        previous_group = project_group
