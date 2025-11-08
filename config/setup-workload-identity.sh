#!/bin/bash
# Script to set up GKE Workload Identity for Airflow dbt pipeline
# This script configures the necessary IAM bindings and permissions

set -e

# Configuration - Update these values
PROJECT_ID="${GCP_PROJECT_ID:-your-gcp-project-id}"
CLUSTER_NAME="${GKE_CLUSTER_NAME:-airflow-cluster}"
CLUSTER_ZONE="${GKE_CLUSTER_ZONE:-us-central1-a}"
NAMESPACE="airflow"
KSA_NAME="airflow-sa"  # Kubernetes Service Account
GSA_NAME="dbt-airflow"  # GCP Service Account

echo "Setting up Workload Identity for Airflow on GKE"
echo "================================================"
echo "Project ID: $PROJECT_ID"
echo "Cluster: $CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo "K8s Service Account: $KSA_NAME"
echo "GCP Service Account: $GSA_NAME"
echo ""

# 1. Create GCP Service Account if it doesn't exist
echo "Creating GCP Service Account..."
gcloud iam service-accounts create $GSA_NAME \
    --display-name="Airflow dbt Service Account" \
    --project=$PROJECT_ID || echo "Service account already exists"

# 2. Grant necessary BigQuery permissions to the service account
echo "Granting BigQuery permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# 3. Grant Cloud Storage permissions (for dbt artifacts, if needed)
echo "Granting Cloud Storage permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# 4. Enable Workload Identity on the cluster (if not already enabled)
echo "Enabling Workload Identity on cluster..."
gcloud container clusters update $CLUSTER_NAME \
    --zone=$CLUSTER_ZONE \
    --workload-pool=${PROJECT_ID}.svc.id.goog || echo "Workload Identity already enabled"

# 5. Create namespace if it doesn't exist
echo "Creating Kubernetes namespace..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 6. Create Kubernetes Service Account
echo "Creating Kubernetes Service Account..."
kubectl create serviceaccount $KSA_NAME \
    --namespace=$NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# 7. Annotate Kubernetes Service Account with GCP Service Account
echo "Annotating Kubernetes Service Account..."
kubectl annotate serviceaccount $KSA_NAME \
    --namespace=$NAMESPACE \
    iam.gke.io/gcp-service-account=${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
    --overwrite

# 8. Bind GCP Service Account to Kubernetes Service Account
echo "Binding GCP SA to K8s SA..."
gcloud iam service-accounts add-iam-policy-binding \
    ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
    --role=roles/iam.workloadIdentityUser \
    --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]" \
    --project=$PROJECT_ID

echo ""
echo "✓ Workload Identity setup complete!"
echo ""
echo "Next steps:"
echo "1. Update the image in config/airflow-gke-deployment.yaml"
echo "2. Update PROJECT_ID in config/airflow-gke-deployment.yaml"
echo "3. Generate Fernet key: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
echo "4. Update the fernet-key in the airflow-secrets Secret"
echo "5. Deploy: kubectl apply -f config/airflow-gke-deployment.yaml"
