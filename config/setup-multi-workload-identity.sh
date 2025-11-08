#!/bin/bash
# Script to set up GKE Workload Identity for multiple dbt projects
# Each project uses its own service account for better isolation

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
CLUSTER_NAME="${GKE_CLUSTER_NAME:-airflow-cluster}"
CLUSTER_ZONE="${GKE_CLUSTER_ZONE:-us-central1-a}"
NAMESPACE="airflow"
KSA_NAME="airflow-sa"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Setting up Workload Identity for Multiple dbt Projects      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Project ID: $PROJECT_ID"
echo "Cluster: $CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo ""

# Array of service accounts
declare -a SA_NAMES=("sales-dbt" "marketing-dbt" "finance-dbt")
declare -a SA_EMAILS=(
    "sales-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
    "marketing-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
    "finance-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
)

# Enable Workload Identity on cluster (if not already enabled)
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Enabling Workload Identity on cluster..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
gcloud container clusters update $CLUSTER_NAME \
    --zone=$CLUSTER_ZONE \
    --workload-pool=${PROJECT_ID}.svc.id.goog || echo "Workload Identity already enabled"

# Create namespace if it doesn't exist
echo ""
echo "Creating Kubernetes namespace..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# Create Kubernetes Service Account
echo "Creating Kubernetes Service Account..."
kubectl create serviceaccount $KSA_NAME \
    --namespace=$NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# Bind each GCP service account to the Kubernetes service account
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Binding GCP Service Accounts to Kubernetes Service Account"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for sa_email in "${SA_EMAILS[@]}"; do
    echo ""
    echo "→ Binding ${sa_email}..."

    # Add IAM policy binding for Workload Identity
    gcloud iam service-accounts add-iam-policy-binding "${sa_email}" \
        --role=roles/iam.workloadIdentityUser \
        --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]" \
        --project=$PROJECT_ID

    echo "  ✓ Bound ${sa_email}"
done

# Create ConfigMap with service account emails for each project
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Creating ConfigMap with service account configurations..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

kubectl create configmap dbt-service-accounts \
    --from-literal=sales-sa-email="sales-dbt@${PROJECT_ID}.iam.gserviceaccount.com" \
    --from-literal=marketing-sa-email="marketing-dbt@${PROJECT_ID}.iam.gserviceaccount.com" \
    --from-literal=finance-sa-email="finance-dbt@${PROJECT_ID}.iam.gserviceaccount.com" \
    --namespace=$NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

echo "✓ ConfigMap created"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✓ Workload Identity setup complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Summary:"
echo "┌────────────┬───────────────────────────────────────────────────────────┐"
echo "│ Project    │ GCP Service Account                                       │"
echo "├────────────┼───────────────────────────────────────────────────────────┤"
for sa_email in "${SA_EMAILS[@]}"; do
    project_name=$(echo "$sa_email" | cut -d'-' -f1)
    printf "│ %-10s │ %-57s │\n" "$project_name" "$sa_email"
done
echo "└────────────┴───────────────────────────────────────────────────────────┘"
echo ""
echo "Next steps:"
echo ""
echo "1. Update your dbt profiles to use impersonate_service_account:"
echo "   Edit dbt_projects/*/profiles.yml and uncomment:"
echo "   # impersonate_service_account: \"{{ env_var('*_SERVICE_ACCOUNT_EMAIL', '') }}\""
echo ""
echo "2. Set environment variables in your deployment:"
echo "   SALES_SERVICE_ACCOUNT_EMAIL=sales-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
echo "   MARKETING_SERVICE_ACCOUNT_EMAIL=marketing-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
echo "   FINANCE_SERVICE_ACCOUNT_EMAIL=finance-dbt@${PROJECT_ID}.iam.gserviceaccount.com"
echo ""
echo "3. Deploy to GKE:"
echo "   kubectl apply -f config/airflow-gke-deployment.yaml"
echo ""
echo "4. Verify the setup:"
echo "   kubectl exec -it -n airflow deployment/airflow-webserver -- \\"
echo "     gcloud auth list"
echo ""
