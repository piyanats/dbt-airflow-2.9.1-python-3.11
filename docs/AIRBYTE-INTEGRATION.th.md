# คู่มือการใช้งาน Airbyte + dbt Pipeline

[English](AIRBYTE-INTEGRATION.md) | **ภาษาไทย**

## สารบัญ

1. [ภาพรวม](#ภาพรวม)
2. [สถาปัตยกรรม ELT](#สถาปัตยกรรม-elt)
3. [การติดตั้ง Airbyte](#การติดตั้ง-airbyte)
4. [การตั้งค่า Airflow Connection](#การตั้งค่า-airflow-connection)
5. [การตั้งค่า Airbyte Connection](#การตั้งค่า-airbyte-connection)
6. [การใช้งาน DAG](#การใช้งาน-dag)
7. [ตัวอย่างการใช้งาน](#ตัวอย่างการใช้งาน)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## ภาพรวม

การรวม Airbyte กับ dbt ใน Airflow ช่วยให้คุณสร้าง **Modern ELT Pipeline** ที่สมบูรณ์:

- **Airbyte**: Extract และ Load ข้อมูลจาก sources ต่างๆ ไปยัง BigQuery
- **dbt**: Transform ข้อมูลใน BigQuery ให้พร้อมใช้งาน
- **Airflow**: Orchestrate ทั้ง pipeline ให้ทำงานอัตโนมัติ

### ข้อดี

✅ **Automated Data Integration**
- Airbyte ดึงข้อมูลจาก 300+ sources อัตโนมัติ
- ไม่ต้องเขียน ETL code เอง
- Update schema อัตโนมัติ

✅ **Separation of Concerns**
- Airbyte: รับผิดชอบ extraction และ loading
- dbt: รับผิดชอบ transformation
- ทีมทำงานแยกกันได้

✅ **Scalability**
- Airbyte sync หลาย sources พร้อมกัน
- dbt ทำงานใน BigQuery (fast และ scalable)
- Airflow จัดการ dependencies

✅ **Monitoring และ Alerting**
- Airflow UI แสดงสถานะทั้ง pipeline
- Alert เมื่อมีปัญหา
- Logs รวมในที่เดียว

---

## สถาปัตยกรรม ELT

### Traditional ETL vs Modern ELT

**Traditional ETL (เก่า):**
```
Source → Extract → Transform (CPU intensive) → Load → Warehouse
         ↑__________________|
         ต้องเขียน code เยอะ
```

**Modern ELT (ใหม่):**
```
Source → Extract & Load (Airbyte) → Warehouse → Transform (dbt)
                                     ↑______________|
                                   ใช้ power ของ warehouse
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

## การติดตั้ง Airbyte

### ตัวเลือกการติดตั้ง

#### Option 1: Docker Compose (สำหรับ Development)

```bash
# Clone Airbyte
git clone https://github.com/airbytehq/airbyte.git
cd airbyte

# เริ่ม Airbyte
./run-ab-platform.sh

# Airbyte จะทำงานที่:
# Web UI: http://localhost:8000
# Username: airbyte
# Password: password
```

#### Option 2: Kubernetes (สำหรับ Production)

```bash
# เพิ่ม Helm repo
helm repo add airbyte https://airbytehq.github.io/helm-charts
helm repo update

# Install Airbyte
kubectl create namespace airbyte
helm install airbyte airbyte/airbyte --namespace airbyte

# ตรวจสอบ pods
kubectl get pods -n airbyte
```

#### Option 3: Airbyte Cloud (Managed Service)

- ไปที่ https://cloud.airbyte.com
- Sign up และใช้งานได้เลย
- ไม่ต้องจัดการ infrastructure

---

## การตั้งค่า Airflow Connection

### ขั้นตอนที่ 1: สร้าง Airbyte Connection ใน Airflow

ใน Airflow UI (Admin → Connections):

**Connection Details:**
- **Connection Id**: `airbyte_default`
- **Connection Type**: `Airbyte`
- **Host**: `localhost` (หรือ Airbyte server IP)
- **Port**: `8001` (Airbyte API port)
- **Login**: (ถ้ามี)
- **Password**: (ถ้ามี)

**ตัวอย่างการตั้งค่า:**

```python
# หรือตั้งค่าผ่าน environment variable
AIRFLOW_CONN_AIRBYTE_DEFAULT='{"conn_type": "airbyte", "host": "localhost", "port": 8001}'
```

**สำหรับ Airbyte Cloud:**
```python
AIRFLOW_CONN_AIRBYTE_DEFAULT='{"conn_type": "airbyte", "host": "api.airbyte.com", "port": 443, "login": "your-api-key"}'
```

### ขั้นตอนที่ 2: ทดสอบ Connection

```bash
# ใน Airflow container
docker-compose exec airflow-webserver bash

# ทดสอบ connection
python -c "from airflow.providers.airbyte.hooks.airbyte import AirbyteHook; hook = AirbyteHook('airbyte_default'); print(hook.get_connection_status())"
```

---

## การตั้งค่า Airbyte Connection

### ขั้นตอนที่ 1: เปิด Airbyte UI

```bash
# เปิดเบราว์เซอร์ไปที่
http://localhost:8000

# Login:
# Username: airbyte
# Password: password
```

### ขั้นตอนที่ 2: สร้าง Source

**ตัวอย่าง: PostgreSQL Source**

1. ไปที่ **Sources** → **New Source**
2. เลือก **PostgreSQL**
3. กรอกข้อมูล:
   ```
   Host: your-postgres-host
   Port: 5432
   Database: sales_db
   Username: readonly_user
   Password: ********
   ```
4. **Test Connection** → **Set up Source**

### ขั้นตอนที่ 3: สร้าง Destination (BigQuery)

1. ไปที่ **Destinations** → **New Destination**
2. เลือก **BigQuery**
3. กรอกข้อมูล:
   ```
   Project ID: your-gcp-project-id
   Dataset Location: asia-southeast1
   Default Dataset: raw_data
   Service Account Key JSON: { ... }
   ```
4. **Test** → **Set up Destination**

### ขั้นตอนที่ 4: สร้าง Connection

1. ไปที่ **Connections** → **New Connection**
2. เลือก Source และ Destination ที่สร้างไว้
3. ตั้งค่า:
   - **Replication frequency**: Manual (จะให้ Airflow trigger)
   - **Destination Namespace**: Custom format
   - **Streams**: เลือก tables ที่ต้องการ sync

4. **บันทึก Connection ID**:
   - จะอยู่ใน URL: `/connections/<connection-id>`
   - ตัวอย่าง: `e3b0c442-98fc-1c14-b39f-92d1282a3b4e`

---

## การใช้งาน DAG

### ไฟล์ DAG: `dags/airbyte_dbt_dag.py`

**Configuration:**

1. อัปเดต Connection IDs:
```python
AIRBYTE_CONNECTIONS = {
    "sales_data": {
        "connection_id": "e3b0c442-98fc-1c14-b39f-92d1282a3b4e",  # จาก Airbyte UI
        "description": "Sync sales data from PostgreSQL to BigQuery",
    },
    "marketing_data": {
        "connection_id": "f4c1d553-a9ed-2d25-c4af-a3e2393c4c5f",  # จาก Airbyte UI
        "description": "Sync marketing data from Google Ads to BigQuery",
    },
}
```

2. อัปเดต dbt project path (ถ้าจำเป็น):
```python
DBT_PROJECT_DIR = Path("/opt/airflow/dbt_project")
```

### การรัน DAG

1. **Enable DAG** ใน Airflow UI
2. **Trigger manually** หรือรอ schedule
3. **Monitor progress** ใน Graph View
4. **ตรวจสอบ logs** ถ้ามีปัญหา

---

## ตัวอย่างการใช้งาน

### Use Case 1: E-commerce Analytics

**Scenario:**
- ดึงข้อมูล orders จาก PostgreSQL
- ดึงข้อมูล customers จาก MySQL
- Transform และสร้าง customer analytics

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
- ดึงข้อมูล ads จาก Google Ads
- ดึงข้อมูล conversions จาก Facebook Ads
- วิเคราะห์ ROI และ campaign performance

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
- ดึงข้อมูล subscriptions จาก Stripe
- ดึงข้อมูล usage จาก application database
- คำนวณ MRR, Churn rate

---

## Best Practices

### 1. Incremental Syncs

**ตั้งค่า Airbyte ให้ sync แบบ incremental:**

```
Sync Mode: Incremental - Append + Deduped
Cursor Field: updated_at
Primary Key: id
```

**ประโยชน์:**
- ประหยัดเวลาและ resources
- Sync เฉพาะข้อมูลที่เปลี่ยนแปลง

### 2. Error Handling

**เพิ่ม error handling ใน DAG:**

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

**เพิ่ม data quality tests ก่อน transform:**

```python
# ตรวจสอบว่ามีข้อมูลหรือไม่
check_data = BigQueryCheckOperator(
    task_id="check_data_exists",
    sql="SELECT COUNT(*) FROM `project.dataset.raw_orders`",
    use_legacy_sql=False,
)

sync_data >> check_data >> dbt_run
```

### 4. Monitoring

**ตั้งค่า alerts:**

```python
default_args = {
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'sla': timedelta(hours=4),  # Alert ถ้าทำงานนานเกิน 4 ชม.
}
```

### 5. Resource Management

**จำกัด parallel tasks:**

```python
# ใน DAG
max_active_runs=1,  # รัน DAG ครั้งละ 1 instance

# Airbyte connection config
timeout=7200,  # 2 hours timeout
wait_seconds=30,  # Check status ทุก 30 วินาที
```

---

## Troubleshooting

### ปัญหา: Airbyte connection ไม่ได้

**Error:**
```
Failed to connect to Airbyte server at localhost:8001
```

**แก้ไข:**

1. ตรวจสอบว่า Airbyte ทำงานอยู่:
```bash
curl http://localhost:8001/health
```

2. ตรวจสอบ Airflow connection:
```bash
airflow connections get airbyte_default
```

3. ถ้าใช้ Docker networks:
```yaml
# docker-compose.yml
services:
  airflow-webserver:
    networks:
      - airflow
      - airbyte
```

### ปัญหา: Airbyte sync ล้มเหลว

**Error:**
```
Airbyte job failed with status: FAILED
```

**แก้ไข:**

1. ดู logs ใน Airbyte UI
2. ตรวจสอบ source connection
3. ตรวจสอบ destination permissions
4. เพิ่ม timeout:
```python
sync_data = AirbyteTriggerSyncOperator(
    timeout=7200,  # เพิ่มเป็น 2 ชั่วโมง
)
```

### ปัญหา: dbt ไม่พบ source tables

**Error:**
```
Compilation Error in model stg_orders
  Source 'raw.orders' not found
```

**แก้ไข:**

1. ตรวจสอบว่า Airbyte sync เสร็จแล้ว
2. ตรวจสอบ dataset และ table names:
```sql
-- ใน BigQuery
SELECT table_name
FROM `project.raw_data.INFORMATION_SCHEMA.TABLES`;
```

3. อัปเดต dbt sources.yml:
```yaml
sources:
  - name: raw
    database: project-id
    schema: raw_data  # ตรงกับที่ Airbyte สร้าง
    tables:
      - name: orders  # ตรงกับชื่อจาก Airbyte
```

### ปัญหา: Schema mismatch

**Error:**
```
Column 'new_column' not found in source
```

**แก้ไข:**

Airbyte มี schema evolution อัตโนมัติ แต่ dbt ต้องอัปเดตเอง:

```bash
# รัน dbt และดู error
dbt run --models stg_orders

# อัปเดต model
# เพิ่ม column ใหม่ใน SQL
```

---

## สรุป

การใช้ Airbyte + dbt + Airflow ให้:

✅ **Modern ELT Pipeline** ที่ scalable
✅ **Automated Data Integration** จาก 300+ sources
✅ **Clean Separation of Concerns** (EL vs T)
✅ **Production-Ready** orchestration
✅ **Easy Monitoring** และ debugging

**ขั้นตอนสั้นๆ:**

1. ติดตั้ง Airbyte
2. ตั้งค่า Sources และ Destinations ใน Airbyte
3. สร้าง Connections และเก็บ Connection IDs
4. ตั้งค่า Airflow connection ไปยัง Airbyte
5. อัปเดต DAG ด้วย Connection IDs
6. รัน DAG และ monitor

**เอกสารเพิ่มเติม:**
- [Airbyte Documentation](https://docs.airbyte.com/)
- [Airflow Airbyte Provider](https://airflow.apache.org/docs/apache-airflow-providers-airbyte/)
- [dbt Documentation](https://docs.getdbt.com/)
