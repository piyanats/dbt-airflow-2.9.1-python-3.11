# Airflow dbt BigQuery Pipeline

**English** | [ภาษาไทย](README.th.md)

This project implements Apache Airflow DAGs that run dbt models on Google BigQuery, designed to run on Google Kubernetes Engine (GKE) with Workload Identity.

## Features

- **Airflow 2.9.1** with Python 3.11
- **dbt-core** with BigQuery adapter
- **GKE Workload Identity** support for secure authentication
- **Docker** support for local development
- **Kubernetes** manifests for GKE deployment
- Sample dbt models (staging and marts)
- Multiple DAG examples (comprehensive and simple)
- **Support for multiple dbt projects** (Sales, Marketing, Finance)

## Project Structure

```
.
├── dags/                           # Airflow DAGs
│   ├── dbt_dag.py                 # Main dbt pipeline DAG
│   └── dbt_simple_dag.py          # Simplified dbt DAG
├── dbt_project/                    # dbt project
│   ├── models/                    # dbt models
│   │   ├── staging/               # Staging models
│   │   └── marts/                 # Marts models
│   ├── dbt_project.yml            # dbt project configuration
│   └── profiles.yml               # dbt profiles configuration
├── config/                         # Configuration files
│   ├── airflow-gke-deployment.yaml # Kubernetes deployment
│   ├── setup-workload-identity.sh  # Workload identity setup script
│   ├── setup-airflow-config.sh     # Airflow config setup script
│   └── airflow-connections.json    # Airflow connections config
├── Dockerfile                      # Docker image definition
├── docker-compose.yml              # Local development setup
├── requirements.txt                # Python dependencies
├── requirements-dev.txt            # Development dependencies
└── README.md                       # This file
```

## Documentation

- 📘 **[Setup Guide](docs/SETUP-GUIDE.th.md)** - Step-by-step installation and usage instructions (Thai)
- 🔀 **[Multiple dbt Projects Guide](docs/MULTI-PROJECT-GUIDE.md)** - How to run multiple dbt projects concurrently
- ❓ **[FAQ](docs/FAQ.th.md)** - Frequently Asked Questions (Thai)
- 🇹🇭 **[Thai Documentation](README.th.md)** - Full documentation in Thai

## Prerequisites

### For Local Development
- Docker and Docker Compose
- Python 3.11+
- Google Cloud SDK (`gcloud`)
- Service account key file with BigQuery permissions

### For GKE Deployment
- GKE cluster with Workload Identity enabled
- `kubectl` configured to access your cluster
- Google Cloud SDK (`gcloud`)
- Appropriate IAM permissions

## Quick Start - Local Development

### 1. Clone the Repository

```bash
git clone <repository-url>
cd dbt-airflow-2.9.1-python-3.11
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
# Edit .env with your configuration
```

Update the following in `.env`:
- `GCP_PROJECT_ID`: Your GCP project ID
- `GCP_LOCATION`: BigQuery location (e.g., US, EU)
- `DBT_DATASET`: Target BigQuery dataset

### 3. Add GCP Service Account Key

Place your service account key file at `config/gcp-key.json`

```bash
# Download service account key
gcloud iam service-accounts keys create config/gcp-key.json \
  --iam-account=YOUR-SERVICE-ACCOUNT@YOUR-PROJECT.iam.gserviceaccount.com
```

### 4. Update dbt Source Configuration

Edit `dbt_project/models/staging/sources.yml` to point to your actual source tables in BigQuery.

### 5. Start Airflow with Docker Compose

```bash
# Initialize Airflow database
docker-compose up airflow-init

# Start Airflow services
docker-compose up -d
```

### 6. Access Airflow UI

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 7. Configure Airflow Connections and Variables

```bash
# Execute setup script in the running container
docker-compose exec airflow-webserver bash /opt/airflow/config/setup-airflow-config.sh
```

Or manually via Airflow UI:
- Navigate to Admin → Variables
- Add variables from `config/airflow-connections.json`
- Navigate to Admin → Connections
- Add BigQuery connection

### 8. Enable and Run DAGs

1. In the Airflow UI, enable the DAG `dbt_bigquery_pipeline` or `dbt_simple_pipeline`
2. Trigger the DAG manually or wait for the scheduled run

## Deployment to GKE

### 1. Set Up Workload Identity

```bash
# Set environment variables
export GCP_PROJECT_ID="your-gcp-project-id"
export GKE_CLUSTER_NAME="your-cluster-name"
export GKE_CLUSTER_ZONE="us-central1-a"

# Run the setup script
./config/setup-workload-identity.sh
```

This script will:
- Create a GCP service account
- Grant BigQuery and Cloud Storage permissions
- Enable Workload Identity on your GKE cluster
- Bind Kubernetes and GCP service accounts

### 2. Build and Push Docker Image

```bash
# Build the Docker image
docker build -t gcr.io/${GCP_PROJECT_ID}/airflow-dbt:latest .

# Push to Google Container Registry
docker push gcr.io/${GCP_PROJECT_ID}/airflow-dbt:latest
```

### 3. Update Kubernetes Manifests

Edit `config/airflow-gke-deployment.yaml`:
- Replace `YOUR-PROJECT-ID` with your actual GCP project ID
- Update the image reference to your pushed image
- Generate a Fernet key and update the `airflow-secrets` Secret

Generate Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Base64 encode the result
echo -n "your-fernet-key" | base64
```

### 4. Deploy to GKE

```bash
# Apply the Kubernetes manifests
kubectl apply -f config/airflow-gke-deployment.yaml

# Check deployment status
kubectl get pods -n airflow
kubectl get services -n airflow
```

### 5. Access Airflow UI

```bash
# Get the external IP
kubectl get service airflow-webserver-service -n airflow

# Access Airflow at http://<EXTERNAL-IP>
```

## DAG Overview

### Main DAG (`dbt_dag.py`)

A comprehensive DAG with separate tasks for each dbt command:

1. **setup_gcp_credentials**: Configure GCP credentials from Airflow connection
2. **dbt_debug**: Verify dbt configuration and BigQuery connection
3. **dbt_deps**: Install dbt package dependencies
4. **dbt_seed**: Load seed data from CSV files
5. **dbt_run_staging**: Build staging models
6. **dbt_test_staging**: Test staging models
7. **dbt_run_marts**: Build marts models
8. **dbt_test_marts**: Test marts models
9. **dbt_docs_generate**: Generate dbt documentation

### Simple DAG (`dbt_simple_dag.py`)

A simplified DAG that runs all dbt commands in a single task. Useful for:
- Smaller projects
- Simple data pipelines
- Getting started quickly

## dbt Models

### Staging Models

Located in `dbt_project/models/staging/`:
- `stg_orders.sql`: Staging model for orders data
- `stg_customers.sql`: Staging model for customers data

### Marts Models

Located in `dbt_project/models/marts/`:
- `customer_orders.sql`: Aggregated customer order data

### Customizing Models

1. Update the source configuration in `dbt_project/models/staging/sources.yml`
2. Modify existing models or create new ones
3. Update schema tests in the respective `schema.yml` files
4. Test locally: `cd dbt_project && dbt run --target dev`

## Configuration

### Airflow Variables

Set these in Airflow UI (Admin → Variables):
- `gcp_project_id`: Your GCP project ID
- `dbt_dataset`: Target BigQuery dataset (default: `analytics`)
- `gcp_location`: BigQuery location (default: `asia-southeast1`)

### Airflow Connections

Create a connection with ID `bigquery_default`:
- **Connection Type**: Google Cloud Platform
- **Project ID**: Your GCP project ID
- **Keyfile Path**: Path to service account key (for local dev)
- Or configure Workload Identity (for GKE)

### dbt Profiles

The `dbt_project/profiles.yml` is configured to use environment variables:
- `GCP_PROJECT_ID`: GCP project ID
- `DBT_DATASET`: Target BigQuery dataset
- `GCP_LOCATION`: BigQuery location
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key (optional)

## Workload Identity Configuration

When running on GKE with Workload Identity:

1. The Kubernetes service account `airflow-sa` is bound to a GCP service account
2. No service account keys are needed
3. Permissions are managed through IAM roles
4. The dbt profile can use `impersonate_service_account` instead of `keyfile`

Benefits:
- No credential files to manage
- Automatic credential rotation
- Improved security posture
- Simpler deployment process

## Monitoring and Troubleshooting

### View Airflow Logs

**Local Development:**
```bash
# View scheduler logs
docker-compose logs airflow-scheduler

# View webserver logs
docker-compose logs airflow-webserver
```

**GKE:**
```bash
# View scheduler logs
kubectl logs -n airflow -l component=scheduler

# View webserver logs
kubectl logs -n airflow -l component=webserver
```

### View dbt Logs

Airflow task logs contain dbt output. Access them via:
- Airflow UI → DAGs → Task Instance → Logs
- Or via CLI: `kubectl logs -n airflow <pod-name>`

### Common Issues

**Issue: dbt can't connect to BigQuery**
- Verify service account has necessary permissions
- Check that credentials are properly configured
- Run `dbt debug` to diagnose connection issues

**Issue: DAG import errors**
- Check Python dependencies are installed
- Verify DAG file syntax
- Check Airflow scheduler logs

**Issue: Workload Identity not working**
- Verify GKE cluster has Workload Identity enabled
- Check service account annotations
- Verify IAM bindings are correct

## Development

### Running dbt Locally

```bash
cd dbt_project

# Install dependencies
dbt deps

# Test connection
dbt debug

# Run models
dbt run --target dev

# Run tests
dbt test --target dev
```

### Testing DAGs

```bash
# Test DAG structure
docker-compose exec airflow-webserver airflow dags test dbt_bigquery_pipeline

# Test specific task
docker-compose exec airflow-webserver airflow tasks test dbt_bigquery_pipeline dbt_run_staging 2024-01-01
```

## Security Best Practices

1. **Never commit credentials**: Ensure `.env` and `*.json` files are in `.gitignore`
2. **Use Workload Identity**: Prefer Workload Identity over service account keys on GKE
3. **Principle of least privilege**: Grant only necessary BigQuery permissions
4. **Rotate credentials**: Regularly rotate service account keys if used
5. **Use Secrets**: Store sensitive data in Kubernetes Secrets or Secret Manager

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request

## License

[Your License Here]

## Support

For issues and questions:
- Open an issue in the repository
- Contact the data engineering team
