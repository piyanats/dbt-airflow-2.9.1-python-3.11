# Airbyte + dbt Pipeline Integration Guide

**English** | [ภาษาไทย](AIRBYTE-INTEGRATION.th.md)

## Table of Contents

1. [Overview](#overview)
2. [ELT Architecture](#elt-architecture)
3. [Installing Airbyte](#installing-airbyte)
4. [Airflow Connection Setup](#airflow-connection-setup)
5. [Airbyte Connection Setup](#airbyte-connection-setup)
6. [Using the DAG](#using-the-dag)
7. [Usage Examples](#usage-examples)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Overview

Integrating Airbyte with dbt in Airflow allows you to build a complete **Modern ELT Pipeline**:

- **Airbyte**: Extract and Load data from various sources to BigQuery
- **dbt**: Transform data in BigQuery for analytics
- **Airflow**: Orchestrate the entire pipeline automatically

### Benefits

✅ **Automated Data Integration**
- Airbyte extracts data from 300+ sources automatically
- No need to write custom ETL code
- Automatic schema updates

✅ **Separation of Concerns**
- Airbyte: Responsible for extraction and loading
- dbt: Responsible for transformation
- Teams can work independently

✅ **Scalability**
- Airbyte syncs multiple sources concurrently
- dbt runs in BigQuery (fast and scalable)
- Airflow manages dependencies

✅ **Monitoring and Alerting**
- Airflow UI shows entire pipeline status
- Alerts on failures
- Centralized logs

---

## ELT Architecture

### Traditional ETL vs Modern ELT

**Traditional ETL (Old):**
```
Source → Extract → Transform (CPU intensive) → Load → Warehouse
         ↑__________________|
         Requires lots of code
```

**Modern ELT (New):**
```
Source → Extract & Load (Airbyte) → Warehouse → Transform (dbt)
                                     ↑______________|
                                   Uses warehouse power
```

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Scheduler                        │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    ELT Pipeline DAG                       │  │
│  │                                                           │  │
│  │  Step 1: Extract & Load (Airbyte)                        │  │
│  │  ┌─────────────────┐     ┌─────────────────┐            │  │
│  │  │  Airbyte Sync   │     │  Airbyte Sync   │            │  │
│  │  │  Sales Data     │     │  Marketing Data │            │  │
│  │  └────────┬────────┘     └────────┬────────┘            │  │
│  │           │                       │                      │  │
│  │           └───────────┬───────────┘                      │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │ Check Status    │                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       │                                  │  │
│  │  Step 2: Transform (dbt)                                │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │   dbt deps      │                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │ dbt run staging │                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │ dbt test staging│                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │  dbt run marts  │                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │ dbt test marts  │                         │  │
│  │              └────────┬────────┘                         │  │
│  │                       ▼                                  │  │
│  │              ┌─────────────────┐                         │  │
│  │              │   dbt docs      │                         │  │
│  │              └─────────────────┘                         │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
   ┌──────────┐                  ┌──────────────┐
   │ Airbyte  │                  │   BigQuery   │
   │ Instance │                  │   Warehouse  │
   └────┬─────┘                  └──────────────┘
        │
        ▼
  ┌─────────────┐
  │  Sources:   │
  │  - MySQL    │
  │  - Postgres │
  │  - APIs     │
  │  - etc.     │
  └─────────────┘
```

---

## Installing Airbyte

### Installation Options

#### Option 1: Docker Compose (For Development)

```bash
# Clone Airbyte
git clone https://github.com/airbytehq/airbyte.git
cd airbyte

# Start Airbyte
./run-ab-platform.sh

# Airbyte will be available at:
# Web UI: http://localhost:8000
# Username: airbyte
# Password: password
```

#### Option 2: Kubernetes (For Production)

```bash
# Add Helm repo
helm repo add airbyte https://airbytehq.github.io/helm-charts
helm repo update

# Install Airbyte
kubectl create namespace airbyte
helm install airbyte airbyte/airbyte --namespace airbyte

# Check pods
kubectl get pods -n airbyte
```

#### Option 3: Airbyte Cloud (Managed Service)

- Go to https://cloud.airbyte.com
- Sign up and start using immediately
- No infrastructure management required

---

## Airflow Connection Setup

### Step 1: Create Airbyte Connection in Airflow

In Airflow UI (Admin → Connections):

**Connection Details:**
- **Connection Id**: `airbyte_default`
- **Connection Type**: `Airbyte`
- **Host**: `localhost` (or Airbyte server IP)
- **Port**: `8001` (Airbyte API port)
- **Login**: (if required)
- **Password**: (if required)

**Configuration Example:**

```python
# Or configure via environment variable
AIRFLOW_CONN_AIRBYTE_DEFAULT='{"conn_type": "airbyte", "host": "localhost", "port": 8001}'
```

**For Airbyte Cloud:**
```python
AIRFLOW_CONN_AIRBYTE_DEFAULT='{"conn_type": "airbyte", "host": "api.airbyte.com", "port": 443, "login": "your-api-key"}'
```

### Step 2: Test Connection

```bash
# In Airflow container
docker-compose exec airflow-webserver bash

# Test connection
python -c "from airflow.providers.airbyte.hooks.airbyte import AirbyteHook; hook = AirbyteHook('airbyte_default'); print(hook.get_connection_status())"
```

---

## Airbyte Connection Setup

### Step 1: Open Airbyte UI

```bash
# Open browser to
http://localhost:8000

# Login:
# Username: airbyte
# Password: password
```

### Step 2: Create Source

**Example: PostgreSQL Source**

1. Go to **Sources** → **New Source**
2. Select **PostgreSQL**
3. Fill in details:
   ```
   Host: your-postgres-host
   Port: 5432
   Database: sales_db
   Username: readonly_user
   Password: ********
   ```
4. **Test Connection** → **Set up Source**

### Step 3: Create Destination (BigQuery)

1. Go to **Destinations** → **New Destination**
2. Select **BigQuery**
3. Fill in details:
   ```
   Project ID: your-gcp-project-id
   Dataset Location: asia-southeast1
   Default Dataset: raw_data
   Service Account Key JSON: { ... }
   ```
4. **Test** → **Set up Destination**

### Step 4: Create Connection

1. Go to **Connections** → **New Connection**
2. Select Source and Destination created above
3. Configure:
   - **Replication frequency**: Manual (Airflow will trigger)
   - **Destination Namespace**: Custom format
   - **Streams**: Select tables to sync

4. **Save Connection ID**:
   - Found in URL: `/connections/<connection-id>`
   - Example: `e3b0c442-98fc-1c14-b39f-92d1282a3b4e`

---

## Using the DAG

### DAG File: `dags/airbyte_dbt_dag.py`

**Configuration:**

1. Update Connection IDs:
```python
AIRBYTE_CONNECTIONS = {
    "sales_data": {
        "connection_id": "e3b0c442-98fc-1c14-b39f-92d1282a3b4e",  # From Airbyte UI
        "description": "Sync sales data from PostgreSQL to BigQuery",
    },
    "marketing_data": {
        "connection_id": "f4c1d553-a9ed-2d25-c4af-a3e2393c4c5f",  # From Airbyte UI
        "description": "Sync marketing data from Google Ads to BigQuery",
    },
}
```

2. Update dbt project path (if needed):
```python
DBT_PROJECT_DIR = Path("/opt/airflow/dbt_project")
```

### Running the DAG

1. **Enable DAG** in Airflow UI
2. **Trigger manually** or wait for schedule
3. **Monitor progress** in Graph View
4. **Check logs** if issues occur

---

## Usage Examples

### Use Case 1: E-commerce Analytics

**Scenario:**
- Extract orders from PostgreSQL
- Extract customers from MySQL
- Transform and create customer analytics

**Airbyte Connections:**
```python
AIRBYTE_CONNECTIONS = {
    "orders": {
        "connection_id": "orders-connection-id",
        "description": "Sync orders from PostgreSQL",
    },
    "customers": {
        "connection_id": "customers-connection-id",
        "description": "Sync customers from MySQL",
    },
}
```

**dbt Models:**
```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
FROM {{ source('raw', 'orders') }}

-- models/marts/customer_ltv.sql
SELECT
    customer_id,
    COUNT(*) as total_orders,
    SUM(total_amount) as lifetime_value
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
```

### Use Case 2: Marketing Analytics

**Scenario:**
- Extract ads data from Google Ads
- Extract conversions from Facebook Ads
- Analyze ROI and campaign performance

**Airbyte Connections:**
```python
AIRBYTE_CONNECTIONS = {
    "google_ads": {
        "connection_id": "google-ads-connection-id",
        "description": "Sync Google Ads campaigns",
    },
    "facebook_ads": {
        "connection_id": "facebook-ads-connection-id",
        "description": "Sync Facebook Ads insights",
    },
}
```

### Use Case 3: SaaS Metrics

**Scenario:**
- Extract subscriptions from Stripe
- Extract usage from application database
- Calculate MRR, Churn rate

---

## Best Practices

### 1. Incremental Syncs

**Configure Airbyte for incremental syncs:**

```
Sync Mode: Incremental - Append + Deduped
Cursor Field: updated_at
Primary Key: id
```

**Benefits:**
- Saves time and resources
- Syncs only changed data

### 2. Error Handling

**Add error handling to DAG:**

```python
from airflow.operators.python import BranchPythonOperator

def check_airbyte_success(**context):
    job_id = context["task_instance"].xcom_pull(task_ids="sync_data")
    if job_id:
        return "dbt_deps"
    else:
        return "send_alert"

check_sync = BranchPythonOperator(
    task_id="check_sync_result",
    python_callable=check_airbyte_success,
)

sync_data >> check_sync >> [dbt_deps, send_alert]
```

### 3. Data Quality Checks

**Add data quality tests before transformation:**

```python
# Check if data exists
check_data = BigQueryCheckOperator(
    task_id="check_data_exists",
    sql="SELECT COUNT(*) FROM `project.dataset.raw_orders`",
    use_legacy_sql=False,
)

sync_data >> check_data >> dbt_run
```

### 4. Monitoring

**Configure alerts:**

```python
default_args = {
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'sla': timedelta(hours=4),  # Alert if takes longer than 4 hours
}
```

### 5. Resource Management

**Limit parallel tasks:**

```python
# In DAG
max_active_runs=1,  # Run only 1 DAG instance at a time

# Airbyte connection config
timeout=7200,  # 2 hours timeout
wait_seconds=30,  # Check status every 30 seconds
```

---

## Troubleshooting

### Issue: Cannot connect to Airbyte

**Error:**
```
Failed to connect to Airbyte server at localhost:8001
```

**Solutions:**

1. Check if Airbyte is running:
```bash
curl http://localhost:8001/health
```

2. Check Airflow connection:
```bash
airflow connections get airbyte_default
```

3. If using Docker networks:
```yaml
# docker-compose.yml
services:
  airflow-webserver:
    networks:
      - airflow
      - airbyte
```

### Issue: Airbyte sync failed

**Error:**
```
Airbyte job failed with status: FAILED
```

**Solutions:**

1. Check logs in Airbyte UI
2. Verify source connection
3. Check destination permissions
4. Increase timeout:
```python
sync_data = AirbyteTriggerSyncOperator(
    timeout=7200,  # Increase to 2 hours
)
```

### Issue: dbt cannot find source tables

**Error:**
```
Compilation Error in model stg_orders
  Source 'raw.orders' not found
```

**Solutions:**

1. Verify Airbyte sync completed
2. Check dataset and table names:
```sql
-- In BigQuery
SELECT table_name
FROM `project.raw_data.INFORMATION_SCHEMA.TABLES`;
```

3. Update dbt sources.yml:
```yaml
sources:
  - name: raw
    database: project-id
    schema: raw_data  # Match what Airbyte creates
    tables:
      - name: orders  # Match name from Airbyte
```

### Issue: Schema mismatch

**Error:**
```
Column 'new_column' not found in source
```

**Solutions:**

Airbyte has automatic schema evolution, but dbt needs manual updates:

```bash
# Run dbt and check error
dbt run --models stg_orders

# Update model
# Add new column to SQL
```

---

## Summary

Using Airbyte + dbt + Airflow provides:

✅ **Modern ELT Pipeline** that's scalable
✅ **Automated Data Integration** from 300+ sources
✅ **Clean Separation of Concerns** (EL vs T)
✅ **Production-Ready** orchestration
✅ **Easy Monitoring** and debugging

**Quick Steps:**

1. Install Airbyte
2. Configure Sources and Destinations in Airbyte
3. Create Connections and save Connection IDs
4. Configure Airflow connection to Airbyte
5. Update DAG with Connection IDs
6. Run DAG and monitor

**Additional Documentation:**
- [Airbyte Documentation](https://docs.airbyte.com/)
- [Airflow Airbyte Provider](https://airflow.apache.org/docs/apache-airflow-providers-airbyte/)
- [dbt Documentation](https://docs.getdbt.com/)
