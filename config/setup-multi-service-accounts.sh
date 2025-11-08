#!/bin/bash
# Script to create separate service accounts for each dbt project
# This provides better security isolation and permission management

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
LOCATION="${GCP_LOCATION:-US}"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Creating Separate Service Accounts for dbt Projects         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Project ID: $PROJECT_ID"
echo "Location: $LOCATION"
echo ""

# Array of projects with their configurations
declare -A PROJECTS
PROJECTS["sales"]="sales_analytics:sales_raw:sales_staging:sales_marts"
PROJECTS["marketing"]="marketing_analytics:marketing_raw:marketing_staging:marketing_marts"
PROJECTS["finance"]="finance_analytics:finance_raw:finance_staging:finance_marts"

# Function to create service account and set permissions
create_service_account() {
    local name=$1
    local display_name=$2
    local raw_dataset=$3
    local staging_dataset=$4
    local marts_dataset=$5

    local sa_email="${name}-dbt@${PROJECT_ID}.iam.gserviceaccount.com"

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Creating Service Account: ${display_name}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # Create service account
    echo "→ Creating service account: ${sa_email}"
    gcloud iam service-accounts create "${name}-dbt" \
        --display-name="${display_name} dbt Service Account" \
        --project=$PROJECT_ID 2>/dev/null || echo "  Service account already exists"

    # Grant BigQuery permissions
    echo "→ Granting BigQuery permissions..."

    # BigQuery Job User (for running queries)
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:${sa_email}" \
        --role="roles/bigquery.jobUser" \
        --condition=None \
        > /dev/null

    # Create datasets if they don't exist
    echo "→ Creating BigQuery datasets..."
    for dataset in "$raw_dataset" "$staging_dataset" "$marts_dataset"; do
        bq mk --dataset \
            --location=$LOCATION \
            --project_id=$PROJECT_ID \
            "${PROJECT_ID}:${dataset}" 2>/dev/null || echo "  Dataset ${dataset} already exists"
    done

    # Grant dataset-specific permissions
    echo "→ Granting dataset permissions..."

    # Read permission on raw dataset
    bq show --format=prettyjson "${PROJECT_ID}:${raw_dataset}" | \
        jq ".access += [{\"role\": \"READER\", \"userByEmail\": \"${sa_email}\"}]" | \
        bq update --source /dev/stdin "${PROJECT_ID}:${raw_dataset}" > /dev/null 2>&1 || true

    # Read/Write permission on staging dataset
    bq show --format=prettyjson "${PROJECT_ID}:${staging_dataset}" | \
        jq ".access += [{\"role\": \"WRITER\", \"userByEmail\": \"${sa_email}\"}]" | \
        bq update --source /dev/stdin "${PROJECT_ID}:${staging_dataset}" > /dev/null 2>&1 || true

    # Read/Write permission on marts dataset
    bq show --format=prettyjson "${PROJECT_ID}:${marts_dataset}" | \
        jq ".access += [{\"role\": \"WRITER\", \"userByEmail\": \"${sa_email}\"}]" | \
        bq update --source /dev/stdin "${PROJECT_ID}:${marts_dataset}" > /dev/null 2>&1 || true

    # Download service account key
    local key_file="config/${name}-sa-key.json"
    echo "→ Downloading service account key to ${key_file}"
    mkdir -p config
    gcloud iam service-accounts keys create "${key_file}" \
        --iam-account="${sa_email}" \
        --project=$PROJECT_ID

    echo "✓ Service account ${sa_email} created successfully"
    echo "✓ Key file: ${key_file}"
    echo ""
}

# Create service accounts for each project
for project_key in "${!PROJECTS[@]}"; do
    IFS=':' read -r display_name raw_dataset staging_dataset marts_dataset <<< "${PROJECTS[$project_key]}"
    create_service_account "$project_key" "$display_name" "$raw_dataset" "$staging_dataset" "$marts_dataset"
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✓ All service accounts created successfully!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Next steps:"
echo ""
echo "1. Update your .env file with the key file paths:"
echo "   SALES_SERVICE_ACCOUNT_KEY=/opt/airflow/config/sales-sa-key.json"
echo "   MARKETING_SERVICE_ACCOUNT_KEY=/opt/airflow/config/marketing-sa-key.json"
echo "   FINANCE_SERVICE_ACCOUNT_KEY=/opt/airflow/config/finance-sa-key.json"
echo ""
echo "2. For local development, copy the key files:"
echo "   cp config/sales-sa-key.json config/"
echo "   cp config/marketing-sa-key.json config/"
echo "   cp config/finance-sa-key.json config/"
echo ""
echo "3. For GKE with Workload Identity, set up bindings:"
echo "   ./config/setup-multi-workload-identity.sh"
echo ""
echo "Service Account Summary:"
echo "┌────────────┬─────────────────────────────────────────────────────────────┐"
echo "│ Project    │ Service Account Email                                       │"
echo "├────────────┼─────────────────────────────────────────────────────────────┤"
echo "│ Sales      │ sales-dbt@${PROJECT_ID}.iam.gserviceaccount.com      │"
echo "│ Marketing  │ marketing-dbt@${PROJECT_ID}.iam.gserviceaccount.com  │"
echo "│ Finance    │ finance-dbt@${PROJECT_ID}.iam.gserviceaccount.com    │"
echo "└────────────┴─────────────────────────────────────────────────────────────┘"
echo ""
echo "⚠️  IMPORTANT: The key files contain sensitive credentials."
echo "   - Add them to .gitignore (already done)"
echo "   - Never commit them to version control"
echo "   - Rotate them regularly (every 90 days recommended)"
echo ""
