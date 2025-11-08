# Guide to Using Separate Service Accounts for Each dbt Project

**English** | [ภาษาไทย](SEPARATE-SERVICE-ACCOUNTS.th.md)

## Table of Contents

1. [Why Use Separate Service Accounts](#why-use-separate-service-accounts)
2. [Architecture Overview](#architecture-overview)
3. [Setup (Local Development)](#setup-local-development)
4. [Setup (GKE with Workload Identity)](#setup-gke-with-workload-identity)
5. [Managing Permissions](#managing-permissions)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Why Use Separate Service Accounts

### Benefits

✅ **Security Isolation**
- Each project accesses only its own data
- Reduces risk of unauthorized data access
- Easier to audit and track access

✅ **Principle of Least Privilege**
- Sales team accesses only sales data
- Marketing team accesses only marketing data
- Finance team accesses only finance data

✅ **Team Autonomy**
- Each team manages its own permissions
- Changes don't affect other projects
- Easier team member onboarding/offboarding

✅ **Compliance**
- Meets compliance requirements (SOC 2, GDPR, PDPA)
- Clear audit trail of who accessed what
- Data segregation per regulations

### Use Cases

**1. Multi-Team Organizations**
```
Sales Team       → sales-dbt@project.iam.gserviceaccount.com
Marketing Team   → marketing-dbt@project.iam.gserviceaccount.com
Finance Team     → finance-dbt@project.iam.gserviceaccount.com
```

**2. Different Permission Requirements**
```
Sales:     Read sales_raw, Write sales_staging + sales_marts
Marketing: Read marketing_raw + sales_marts (cross-reference)
Finance:   Read ALL, Write finance_staging + finance_marts
```

**3. Compliance & Security**
- Finance data requires higher security
- Clear service account separation
- Audit logs separated by service account

---

## Architecture Overview

### Single Service Account Pattern (Before)

```
┌─────────────────────────────────────────┐
│        Airflow Scheduler                │
│  ┌────────────────────────────────────┐ │
│  │  Single Service Account            │ │
│  │  airflow-dbt@project.iam...        │ │
│  │                                    │ │
│  │  Permissions:                      │ │
│  │  - All sales_* datasets            │ │
│  │  - All marketing_* datasets        │ │
│  │  - All finance_* datasets          │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
                    │
     ┌──────────────┼──────────────┐
     ▼              ▼              ▼
  [Sales]      [Marketing]     [Finance]
```

**Problems:**
- Sales project can access finance data
- Marketing project can access sales data
- High security risk

### Separate Service Accounts Pattern (New)

```
┌─────────────────────────────────────────┐
│        Airflow Scheduler                │
│  ┌────────────────────────────────────┐ │
│  │  Sales SA                          │ │
│  │  sales-dbt@project.iam...          │ │
│  │  → sales_* datasets only           │ │
│  └────────────────────────────────────┘ │
│  ┌────────────────────────────────────┐ │
│  │  Marketing SA                      │ │
│  │  marketing-dbt@project.iam...      │ │
│  │  → marketing_* datasets only       │ │
│  └────────────────────────────────────┘ │
│  ┌────────────────────────────────────┐ │
│  │  Finance SA                        │ │
│  │  finance-dbt@project.iam...        │ │
│  │  → finance_* datasets only         │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
      [Sales]      [Marketing]     [Finance]
```

**Benefits:**
- Each project accesses only its own datasets
- Clear security isolation
- Easy to audit

---

## Setup (Local Development)

### Step 1: Create Service Accounts

Run the automated script:

```bash
# Set environment variables
export GCP_PROJECT_ID="your-project-id"
export GCP_LOCATION="asia-southeast1"

# Run the setup script
./config/setup-multi-service-accounts.sh
```

**What the script does:**
1. Creates 3 service accounts:
   - `sales-dbt@PROJECT.iam.gserviceaccount.com`
   - `marketing-dbt@PROJECT.iam.gserviceaccount.com`
   - `finance-dbt@PROJECT.iam.gserviceaccount.com`

2. Creates BigQuery datasets:
   - Sales: `sales_raw`, `sales_staging`, `sales_marts`
   - Marketing: `marketing_raw`, `marketing_staging`, `marketing_marts`
   - Finance: `finance_raw`, `finance_staging`, `finance_marts`

3. Grants permissions:
   - `READER` on raw datasets
   - `WRITER` on staging and marts datasets
   - `bigquery.jobUser` for running queries

4. Downloads key files:
   - `config/sales-sa-key.json`
   - `config/marketing-sa-key.json`
   - `config/finance-sa-key.json`

### Step 2: Configure Environment Variables

Update `.env` file:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-actual-project-id
GCP_LOCATION=asia-southeast1

# Service Account Keys for Each Project
SALES_SERVICE_ACCOUNT_KEY=/opt/airflow/config/sales-sa-key.json
MARKETING_SERVICE_ACCOUNT_KEY=/opt/airflow/config/marketing-sa-key.json
FINANCE_SERVICE_ACCOUNT_KEY=/opt/airflow/config/finance-sa-key.json

# Datasets
SALES_DATASET=sales_analytics
MARKETING_DATASET=marketing_analytics
FINANCE_DATASET=finance_analytics
```

### Step 3: Copy Key Files

```bash
# Ensure key files are in the right location
cp config/sales-sa-key.json config/
cp config/marketing-sa-key.json config/
cp config/finance-sa-key.json config/

# Verify
ls -la config/*.json
```

### Step 4: Test

```bash
# Start Airflow
docker-compose up -d

# Test dbt connection for each project
docker-compose exec airflow-webserver bash

# Test Sales project
cd /opt/airflow/dbt_projects/sales_analytics
export SALES_SERVICE_ACCOUNT_KEY=/opt/airflow/config/sales-sa-key.json
export GCP_PROJECT_ID=your-project-id
dbt debug --profiles-dir .

# Test Marketing project
cd /opt/airflow/dbt_projects/marketing_analytics
export MARKETING_SERVICE_ACCOUNT_KEY=/opt/airflow/config/marketing-sa-key.json
dbt debug --profiles-dir .

# Test Finance project
cd /opt/airflow/dbt_projects/finance_analytics
export FINANCE_SERVICE_ACCOUNT_KEY=/opt/airflow/config/finance-sa-key.json
dbt debug --profiles-dir .
```

---

## Setup (GKE with Workload Identity)

### Step 1: Create Service Accounts (Same as Local)

```bash
export GCP_PROJECT_ID="your-project-id"
./config/setup-multi-service-accounts.sh
```

### Step 2: Setup Workload Identity

```bash
# Set cluster information
export GKE_CLUSTER_NAME="airflow-cluster"
export GKE_CLUSTER_ZONE="us-central1-a"

# Run Workload Identity setup script
./config/setup-multi-workload-identity.sh
```

**What the script does:**
1. Enables Workload Identity on GKE cluster
2. Creates Kubernetes namespace `airflow`
3. Creates Kubernetes Service Account `airflow-sa`
4. Binds each GCP SA to K8s SA:
   ```bash
   sales-dbt@PROJECT → airflow-sa
   marketing-dbt@PROJECT → airflow-sa
   finance-dbt@PROJECT → airflow-sa
   ```
5. Creates ConfigMap with SA emails

### Step 3: Update dbt Profiles

Edit `dbt_projects/*/profiles.yml` to use `impersonate_service_account`:

**Sales Analytics:**
```yaml
# dbt_projects/sales_analytics/profiles.yml
sales_analytics:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: "{{ env_var('SALES_DATASET') }}"
      # Comment out keyfile
      # keyfile: "..."
      # Use impersonate instead
      impersonate_service_account: "{{ env_var('SALES_SERVICE_ACCOUNT_EMAIL') }}"
```

Do the same for Marketing and Finance.

### Step 4: Update Kubernetes Deployment

Add environment variables in `config/airflow-gke-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: airflow
spec:
  template:
    spec:
      serviceAccountName: airflow-sa  # Important!
      containers:
      - name: scheduler
        env:
        # Service Account Emails
        - name: SALES_SERVICE_ACCOUNT_EMAIL
          value: "sales-dbt@YOUR-PROJECT.iam.gserviceaccount.com"
        - name: MARKETING_SERVICE_ACCOUNT_EMAIL
          value: "marketing-dbt@YOUR-PROJECT.iam.gserviceaccount.com"
        - name: FINANCE_SERVICE_ACCOUNT_EMAIL
          value: "finance-dbt@YOUR-PROJECT.iam.gserviceaccount.com"
        # Datasets
        - name: SALES_DATASET
          value: "sales_analytics"
        - name: MARKETING_DATASET
          value: "marketing_analytics"
        - name: FINANCE_DATASET
          value: "finance_analytics"
```

### Step 5: Deploy to GKE

```bash
# Deploy
kubectl apply -f config/airflow-gke-deployment.yaml

# Verify
kubectl get pods -n airflow
kubectl get sa -n airflow

# Test Workload Identity
kubectl exec -it -n airflow deployment/airflow-scheduler -- \
  gcloud auth list
```

---

## Managing Permissions

### Permission Matrix

| Project | Service Account | Datasets (READER) | Datasets (WRITER) |
|---------|----------------|-------------------|-------------------|
| Sales | sales-dbt@ | sales_raw | sales_staging, sales_marts |
| Marketing | marketing-dbt@ | marketing_raw | marketing_staging, marketing_marts |
| Finance | finance-dbt@ | finance_raw | finance_staging, finance_marts |

### Adding Cross-Project Access

If Marketing needs to read Sales data:

```bash
# Grant Marketing SA read access to sales_marts
bq show --format=prettyjson PROJECT:sales_marts | \
  jq '.access += [{"role": "READER", "userByEmail": "marketing-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_marts
```

### Checking Permissions

```bash
# List dataset permissions
bq show --format=prettyjson PROJECT:sales_raw

# List service account permissions
gcloud projects get-iam-policy PROJECT \
  --flatten="bindings[].members" \
  --filter="bindings.members:marketing-dbt@PROJECT.iam.gserviceaccount.com"
```

---

## Best Practices

### 1. Naming Convention

**Good:**
```
sales-dbt@project.iam.gserviceaccount.com
marketing-dbt@project.iam.gserviceaccount.com
finance-dbt@project.iam.gserviceaccount.com
```

**Bad:**
```
sa1@project.iam.gserviceaccount.com
service-account@project.iam.gserviceaccount.com
dbt@project.iam.gserviceaccount.com
```

### 2. Key Rotation

**Rotate keys every 90 days:**

```bash
# Generate new key
gcloud iam service-accounts keys create new-sales-sa-key.json \
  --iam-account=sales-dbt@PROJECT.iam.gserviceaccount.com

# Update configuration
# ... update .env or secrets ...

# Delete old key
gcloud iam service-accounts keys list \
  --iam-account=sales-dbt@PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts keys delete KEY_ID \
  --iam-account=sales-dbt@PROJECT.iam.gserviceaccount.com
```

### 3. Least Privilege

**Start with minimum permissions:**

```bash
# Start with READER only
bq show --format=prettyjson PROJECT:sales_raw | \
  jq '.access += [{"role": "READER", "userByEmail": "sales-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_raw

# Add WRITER only when needed
```

### 4. Monitoring

**Enable audit logs:**

```bash
# Enable Data Access audit logs
gcloud logging read \
  "protoPayload.authenticationInfo.principalEmail=sales-dbt@PROJECT.iam.gserviceaccount.com" \
  --limit 50 \
  --format json
```

---

## Troubleshooting

### Issue: Permission Denied when running dbt

**Error:**
```
google.api_core.exceptions.PermissionDenied: 403 Access Denied: Dataset project:sales_staging: User does not have permission to create table
```

**Solution:**

1. Check correct service account is being used:
```bash
# In DAG or container
echo $SALES_SERVICE_ACCOUNT_KEY
# or
echo $SALES_SERVICE_ACCOUNT_EMAIL
```

2. Check permissions:
```bash
bq show --format=prettyjson PROJECT:sales_staging | jq '.access'
```

3. Add permissions if needed:
```bash
bq show --format=prettyjson PROJECT:sales_staging | \
  jq '.access += [{"role": "WRITER", "userByEmail": "sales-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_staging
```

### Issue: Workload Identity Not Working

**Error:**
```
Could not automatically determine credentials
```

**Solution:**

1. Check service account annotation:
```bash
kubectl describe sa airflow-sa -n airflow | grep iam.gke.io
```

2. Check IAM binding:
```bash
gcloud iam service-accounts get-iam-policy \
  sales-dbt@PROJECT.iam.gserviceaccount.com
```

3. Verify pod uses correct service account:
```bash
kubectl get pod -n airflow -o yaml | grep serviceAccountName
```

---

## Summary

Using separate service accounts for each dbt project provides:

✅ **Better Security** - Clear data isolation
✅ **Compliance** - Meets regulatory requirements
✅ **Team Autonomy** - Each team manages own permissions
✅ **Audit Trail** - Track who accessed what data

**Quick Steps:**

```bash
# 1. Create service accounts
./config/setup-multi-service-accounts.sh

# 2. Update .env
# Add paths to key files

# 3. For GKE
./config/setup-multi-workload-identity.sh

# 4. Test
docker-compose up -d
# or
kubectl apply -f config/airflow-gke-deployment.yaml
```

**For more information:**
- [Multi-Project Guide](MULTI-PROJECT-GUIDE.md)
- [Setup Guide](SETUP-GUIDE.th.md) (Thai)
- [FAQ](FAQ.th.md) (Thai)
