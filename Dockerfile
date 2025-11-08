# Dockerfile for Airflow 2.9.1 with dbt-bigquery on Python 3.11
FROM apache/airflow:2.9.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --user -r /requirements.txt

# Copy dbt project
COPY --chown=airflow:root dbt_project /opt/airflow/dbt_project

# Copy DAGs
COPY --chown=airflow:root dags /opt/airflow/dags

# Set environment variables
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5m --retries=3 \
  CMD curl --fail http://localhost:8080/health || exit 1
