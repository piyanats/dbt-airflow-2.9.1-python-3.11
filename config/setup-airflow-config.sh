#!/bin/bash
# Script to set up Airflow connections and variables

set -e

echo "Setting up Airflow connections and variables..."

# Set Airflow variables
echo "Setting Airflow variables..."
airflow variables set gcp_project_id "${GCP_PROJECT_ID:-your-gcp-project-id}"
airflow variables set dbt_dataset "${DBT_DATASET:-analytics}"
airflow variables set gcp_location "${GCP_LOCATION:-US}"

# Create BigQuery connection
echo "Creating BigQuery connection..."

# For GKE with Workload Identity (no key file needed)
if [ -n "$USE_WORKLOAD_IDENTITY" ]; then
    airflow connections add 'bigquery_default' \
        --conn-type 'google_cloud_platform' \
        --conn-extra "{
            \"project\": \"${GCP_PROJECT_ID}\",
            \"location\": \"${GCP_LOCATION:-US}\",
            \"num_retries\": 5,
            \"use_legacy_sql\": false
        }"
else
    # For local development with service account key file
    airflow connections add 'bigquery_default' \
        --conn-type 'google_cloud_platform' \
        --conn-extra "{
            \"project\": \"${GCP_PROJECT_ID}\",
            \"location\": \"${GCP_LOCATION:-US}\",
            \"num_retries\": 5,
            \"use_legacy_sql\": false,
            \"key_path\": \"${GOOGLE_APPLICATION_CREDENTIALS:-/opt/airflow/config/gcp-key.json}\"
        }"
fi

echo "✓ Airflow configuration complete!"
echo ""
echo "You can verify the configuration with:"
echo "  airflow variables list"
echo "  airflow connections get bigquery_default"
