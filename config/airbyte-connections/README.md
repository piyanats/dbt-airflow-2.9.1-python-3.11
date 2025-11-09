# Airbyte Connection Examples

This directory contains example configurations for common Airbyte source-to-BigQuery connections.

## Available Examples

### 1. PostgreSQL to BigQuery
**File:** `postgres-to-bigquery-example.json`

**Use Case:** Syncing transactional data from PostgreSQL database
- Sales transactions
- Product catalog
- Store information

**Features:**
- Incremental sync with CDC (Change Data Capture)
- Append + dedup for transactional data
- Full refresh for lookup tables

### 2. MySQL to BigQuery
**File:** `mysql-to-bigquery-example.json`

**Use Case:** Syncing customer data from MySQL database
- Customer records
- Customer addresses

**Features:**
- Incremental sync using cursor field
- Append + dedup mode
- Standard replication method

### 3. Google Ads to BigQuery
**File:** `google-ads-to-bigquery-example.json`

**Use Case:** Marketing analytics from Google Ads
- Campaign data
- Ad groups
- Performance reports

**Features:**
- OAuth authentication
- Daily performance metrics
- Incremental sync by date

## How to Use These Examples

### Step 1: Review the Example

Choose the example that matches your use case and open the JSON file to review the configuration.

### Step 2: Create Source in Airbyte UI

1. Open Airbyte UI: http://localhost:8000
2. Go to **Sources** → **New Source**
3. Select the source type (PostgreSQL, MySQL, Google Ads, etc.)
4. Fill in the configuration using values from the example JSON
5. Replace placeholders with your actual credentials and settings
6. Test and save the source

### Step 3: Create Destination in Airbyte UI

1. Go to **Destinations** → **New Destination**
2. Select **BigQuery**
3. Fill in the configuration:
   ```
   Project ID: your-gcp-project-id
   Dataset Location: asia-southeast1
   Default Dataset: <your_dataset_name>
   Service Account Key JSON: <your_service_account_json>
   ```
4. Test and save the destination

### Step 4: Create Connection

1. Go to **Connections** → **New Connection**
2. Select your source and destination
3. Configure sync settings:
   - **Replication frequency:** Manual (Airflow will trigger)
   - **Namespace format:** Custom format
   - **Streams:** Select tables and configure sync modes
4. Save the connection

### Step 5: Get Connection ID

After creating the connection:
1. Note the connection ID from the URL: `/connections/<connection-id>`
2. Example: `e3b0c442-98fc-1c14-b39f-92d1282a3b4e`

### Step 6: Update Airflow DAG

Update `dags/airbyte_dbt_dag.py`:

```python
AIRBYTE_CONNECTIONS = {
    "sales_data": {
        "connection_id": "e3b0c442-98fc-1c14-b39f-92d1282a3b4e",  # Your connection ID
        "description": "Sync sales data from PostgreSQL to BigQuery",
    },
    "marketing_data": {
        "connection_id": "f4c1d553-a9ed-2d25-c4af-a3e2393c4c5f",  # Your connection ID
        "description": "Sync marketing data from Google Ads to BigQuery",
    },
}
```

## Sync Modes Explained

### Source Sync Modes

| Mode | Description | When to Use |
|------|-------------|-------------|
| **Full Refresh** | Syncs all records every time | Small tables, lookup data |
| **Incremental** | Syncs only new/changed records | Large tables, transactional data |

### Destination Sync Modes

| Mode | Description | When to Use |
|------|-------------|-------------|
| **Append** | Adds new records, keeps all history | Event logs, never update records |
| **Append + Dedup** | Adds new records, removes duplicates | Transactional data (recommended) |
| **Overwrite** | Replaces entire table | Small lookup tables |

## Best Practices

### 1. Use Incremental Sync for Large Tables

```json
{
  "config": {
    "syncMode": "incremental",
    "cursorField": ["updated_at"],
    "destinationSyncMode": "append_dedup",
    "primaryKey": [["id"]]
  }
}
```

**Requirements:**
- Table must have a cursor field (timestamp column)
- Table must have a primary key
- Cursor field should be indexed in source database

### 2. Choose Appropriate Replication Method

**PostgreSQL:**
- **CDC (Change Data Capture):** Real-time replication, low latency
- **Standard:** Cursor-based, simpler setup

**MySQL:**
- **CDC (Binlog):** Real-time replication, requires binlog enabled
- **Standard:** Cursor-based, works everywhere

### 3. Configure Proper Namespacing

```json
{
  "namespaceDefinition": "customformat",
  "namespaceFormat": "${SOURCE_NAMESPACE}"
}
```

This preserves schema names from source database.

### 4. Set Manual Schedule

```json
{
  "schedule": {
    "scheduleType": "manual"
  }
}
```

Let Airflow control the sync schedule for better orchestration.

### 5. Monitor Sync Performance

- Check sync duration in Airbyte UI
- Monitor BigQuery quota usage
- Review error logs regularly

## Common Issues

### Issue: Authentication Failed

**Solution:**
- Verify credentials are correct
- Check service account has proper BigQuery permissions
- For OAuth sources, refresh the token

### Issue: Schema Changes Not Detected

**Solution:**
- Airbyte auto-detects schema changes
- May need to refresh the source schema in UI
- Update dbt models if new columns added

### Issue: Sync Timeout

**Solution:**
- Increase timeout in DAG:
  ```python
  sync_task = AirbyteTriggerSyncOperator(
      timeout=7200,  # 2 hours
  )
  ```
- Consider splitting large tables
- Use incremental sync instead of full refresh

## Additional Resources

- [Airbyte Documentation](https://docs.airbyte.com/)
- [Airbyte Source Catalog](https://docs.airbyte.com/integrations/sources/)
- [BigQuery Destination Guide](https://docs.airbyte.com/integrations/destinations/bigquery/)
- [Airbyte Integration Guide](../docs/AIRBYTE-INTEGRATION.md)
