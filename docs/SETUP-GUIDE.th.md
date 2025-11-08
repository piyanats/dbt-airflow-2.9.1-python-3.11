# คู่มือการติดตั้งและใช้งาน Airflow dbt BigQuery Pipeline

## สารบัญ

1. [ความต้องการเบื้องต้น](#ความต้องการเบื้องต้น)
2. [การติดตั้งในเครื่อง (Local Development)](#การติดตั้งในเครื่อง-local-development)
3. [การติดตั้งบน GKE](#การติดตั้งบน-gke)
4. [การตั้งค่า dbt Models](#การตั้งค่า-dbt-models)
5. [การใช้งาน DAGs](#การใช้งาน-dags)
6. [การแก้ไขปัญหา](#การแก้ไขปัญหา)

---

## ความต้องการเบื้องต้น

### ซอฟต์แวร์ที่ต้องติดตั้ง

#### สำหรับการพัฒนาในเครื่อง
- **Docker Desktop** (เวอร์ชัน 20.10+)
  - [ดาวน์โหลดสำหรับ Mac](https://docs.docker.com/desktop/mac/install/)
  - [ดาวน์โหลดสำหรับ Windows](https://docs.docker.com/desktop/windows/install/)
  - [ดาวน์โหลดสำหรับ Linux](https://docs.docker.com/desktop/linux/install/)

- **Docker Compose** (เวอร์ชัน 2.0+)
  - มักจะมาพร้อมกับ Docker Desktop

- **Python 3.11+** (สำหรับทดสอบ dbt ในเครื่อง)
  ```bash
  # ตรวจสอบเวอร์ชัน Python
  python3 --version
  ```

- **Google Cloud SDK**
  ```bash
  # ติดตั้งบน Mac
  brew install google-cloud-sdk

  # ติดตั้งบน Linux
  curl https://sdk.cloud.google.com | bash

  # ตรวจสอบการติดตั้ง
  gcloud --version
  ```

#### สำหรับการ Deploy บน GKE
- **kubectl**
  ```bash
  # ติดตั้งบน Mac
  brew install kubectl

  # ติดตั้งบน Linux
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

  # ตรวจสอบการติดตั้ง
  kubectl version --client
  ```

### บัญชี Google Cloud Platform

1. **สร้าง GCP Project**
   - ไปที่ [Google Cloud Console](https://console.cloud.google.com)
   - สร้างโปรเจคใหม่หรือเลือกโปรเจคที่มีอยู่
   - จดบันทึก Project ID

2. **เปิดใช้งาน APIs**
   ```bash
   # ตั้งค่า project
   gcloud config set project YOUR-PROJECT-ID

   # เปิดใช้งาน BigQuery API
   gcloud services enable bigquery.googleapis.com

   # เปิดใช้งาน Cloud Storage API
   gcloud services enable storage-api.googleapis.com

   # เปิดใช้งาน Kubernetes Engine API (สำหรับ GKE)
   gcloud services enable container.googleapis.com
   ```

3. **สร้าง Service Account**
   ```bash
   # สร้าง service account
   gcloud iam service-accounts create airflow-dbt-sa \
     --display-name="Airflow dbt Service Account"

   # ให้สิทธิ์ BigQuery
   gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
     --member="serviceAccount:airflow-dbt-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"

   gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
     --member="serviceAccount:airflow-dbt-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"

   # ดาวน์โหลด key file
   gcloud iam service-accounts keys create ~/airflow-dbt-key.json \
     --iam-account=airflow-dbt-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com
   ```

---

## การติดตั้งในเครื่อง (Local Development)

### ขั้นตอนที่ 1: Clone Repository

```bash
# Clone repository
git clone https://github.com/YOUR-USERNAME/dbt-airflow-2.9.1-python-3.11.git
cd dbt-airflow-2.9.1-python-3.11

# หรือถ้าใช้ SSH
git clone git@github.com:YOUR-USERNAME/dbt-airflow-2.9.1-python-3.11.git
cd dbt-airflow-2.9.1-python-3.11
```

### ขั้นตอนที่ 2: ตั้งค่า Environment Variables

```bash
# คัดลอกไฟล์ตัวอย่าง
cp .env.example .env

# แก้ไขไฟล์ .env
nano .env  # หรือใช้ text editor ที่คุณชอบ
```

แก้ไขค่าต่อไปนี้ในไฟล์ `.env`:

```bash
# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow  # เปลี่ยนเป็นรหัสผ่านที่แข็งแรง

# GCP Configuration
GCP_PROJECT_ID=your-actual-project-id  # แทนที่ด้วย Project ID จริง
GCP_LOCATION=asia-southeast1  # หรือ asia-southeast1 สำหรับภูมิภาคเอเชีย
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json

# dbt Configuration
DBT_DATASET=analytics  # ชื่อ dataset ที่ต้องการสร้างใน BigQuery
DBT_PROFILES_DIR=/opt/airflow/dbt_project
DBT_PROJECT_DIR=/opt/airflow/dbt_project
```

### ขั้นตอนที่ 3: เพิ่ม Service Account Key

```bash
# สร้างโฟลเดอร์ config ถ้ายังไม่มี
mkdir -p config

# คัดลอก key file ที่ดาวน์โหลดมา
cp ~/airflow-dbt-key.json config/gcp-key.json

# ตรวจสอบว่าไฟล์อยู่ที่ถูกต้อง
ls -la config/gcp-key.json
```

**หมายเหตุ:** อย่า commit ไฟล์ `gcp-key.json` ไปยัง Git! ไฟล์นี้อยู่ใน `.gitignore` แล้ว

### ขั้นตอนที่ 4: เตรียมข้อมูลใน BigQuery

#### สร้าง Dataset และ Source Tables

```sql
-- เปิด BigQuery Console: https://console.cloud.google.com/bigquery

-- สร้าง dataset สำหรับข้อมูลต้นทาง
CREATE SCHEMA IF NOT EXISTS `your-project-id.raw_data`
OPTIONS(
  location="asia-southeast1"  -- หรือ "asia-southeast1"
);

-- สร้าง dataset สำหรับข้อมูล staging
CREATE SCHEMA IF NOT EXISTS `your-project-id.analytics_staging`
OPTIONS(
  location="asia-southeast1"
);

-- สร้าง dataset สำหรับข้อมูล marts
CREATE SCHEMA IF NOT EXISTS `your-project-id.analytics_marts`
OPTIONS(
  location="asia-southeast1"
);

-- ตัวอย่าง: สร้างตาราง orders (ปรับให้เหมาะกับข้อมูลจริงของคุณ)
CREATE TABLE IF NOT EXISTS `your-project-id.raw_data.orders` (
  order_id INT64,
  customer_id INT64,
  order_date DATE,
  order_amount NUMERIC(10,2),
  order_status STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- ตัวอย่าง: สร้างตาราง customers
CREATE TABLE IF NOT EXISTS `your-project-id.raw_data.customers` (
  customer_id INT64,
  customer_name STRING,
  customer_email STRING,
  customer_phone STRING,
  customer_address STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- เพิ่มข้อมูลตัวอย่าง (ไม่บังคับ)
INSERT INTO `your-project-id.raw_data.orders` VALUES
(1, 101, '2024-01-15', 150.00, 'completed', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
(2, 102, '2024-01-16', 200.00, 'completed', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
(3, 101, '2024-01-17', 75.50, 'pending', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

INSERT INTO `your-project-id.raw_data.customers` VALUES
(101, 'สมชาย ใจดี', 'somchai@example.com', '081-234-5678', 'กรุงเทพฯ', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()),
(102, 'สมหญิง รักษ์ดี', 'somying@example.com', '082-345-6789', 'เชียงใหม่', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

### ขั้นตอนที่ 5: อัปเดตการตั้งค่า dbt

แก้ไขไฟล์ `dbt_project/models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: raw
    description: "Raw data from source systems"
    database: "your-actual-project-id"  # แทนที่ด้วย Project ID จริง
    schema: raw_data
    tables:
      - name: orders
        description: "Raw orders table"
        # ... (เหมือนเดิม)

      - name: customers
        description: "Raw customers table"
        # ... (เหมือนเดิม)
```

### ขั้นตอนที่ 6: Build และ Start Airflow

```bash
# Build Docker images
docker-compose build

# เริ่มต้นฐานข้อมูล Airflow (รันครั้งแรกเท่านั้น)
docker-compose up airflow-init

# เริ่ม Airflow services
docker-compose up -d

# ตรวจสอบสถานะ containers
docker-compose ps

# ดู logs
docker-compose logs -f
```

**รอประมาณ 1-2 นาที** ให้ Airflow เริ่มต้นเสร็จ

### ขั้นตอนที่ 7: เข้าถึง Airflow UI

1. เปิดเว็บเบราว์เซอร์ไปที่: http://localhost:8080
2. Login ด้วย:
   - **Username**: `airflow`
   - **Password**: `airflow` (หรือที่ตั้งค่าใน .env)

### ขั้นตอนที่ 8: ตั้งค่า Airflow Variables และ Connections

#### วิธีที่ 1: ใช้ UI (แนะนำสำหรับมือใหม่)

**ตั้งค่า Variables:**
1. ใน Airflow UI ไปที่ **Admin** → **Variables**
2. คลิก **+** เพื่อเพิ่ม variable ใหม่
3. เพิ่ม variables ต่อไปนี้:

| Key | Value | Description |
|-----|-------|-------------|
| gcp_project_id | your-project-id | GCP Project ID |
| dbt_dataset | analytics | BigQuery dataset สำหรับ dbt |
| gcp_location | US | BigQuery location |

**ตั้งค่า Connection:**
1. ไปที่ **Admin** → **Connections**
2. คลิก **+** เพื่อเพิ่ม connection ใหม่
3. กรอกข้อมูล:
   - **Connection Id**: `bigquery_default`
   - **Connection Type**: `Google Cloud`
   - **Project Id**: `your-project-id`
   - **Keyfile Path**: `/opt/airflow/config/gcp-key.json`
   - **Scopes**: `https://www.googleapis.com/auth/bigquery`
4. คลิก **Test** เพื่อทดสอบการเชื่อมต่อ
5. คลิก **Save**

#### วิธีที่ 2: ใช้ Script

```bash
# รันสคริปต์ตั้งค่าใน container
docker-compose exec airflow-webserver bash -c "
  export GCP_PROJECT_ID=your-project-id
  export DBT_DATASET=analytics
  export GCP_LOCATION=asia-southeast1
  export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json
  /opt/airflow/config/setup-airflow-config.sh
"
```

### ขั้นตอนที่ 9: ทดสอบ dbt Connection

```bash
# เข้าไปใน container
docker-compose exec airflow-webserver bash

# ไปยังโฟลเดอร์ dbt
cd /opt/airflow/dbt_project

# ตั้งค่า environment variables
export GCP_PROJECT_ID=your-project-id
export DBT_DATASET=analytics
export GCP_LOCATION=asia-southeast1
export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json

# ทดสอบการเชื่อมต่อ
dbt debug

# ถ้าทุกอย่างถูกต้อง จะเห็นข้อความ "All checks passed!"

# ออกจาก container
exit
```

### ขั้นตอนที่ 10: รัน DAG

1. ใน Airflow UI ไปที่หน้า **DAGs**
2. คุณจะเห็น DAGs:
   - `dbt_bigquery_pipeline` - DAG แบบละเอียด
   - `dbt_simple_pipeline` - DAG แบบง่าย
3. เปิดใช้งาน DAG โดยคลิกที่ toggle switch
4. Trigger DAG ด้วยตนเองโดยคลิกปุ่ม ▶️ (Play)
5. คลิกที่ชื่อ DAG เพื่อดูรายละเอียด
6. ดูความคืบหน้าใน **Graph View** หรือ **Grid View**
7. คลิกที่ task เพื่อดู logs

### ขั้นตอนที่ 11: ตรวจสอบผลลัพธ์ใน BigQuery

```sql
-- ตรวจสอบ staging tables
SELECT * FROM `your-project-id.analytics_staging.stg_orders` LIMIT 10;
SELECT * FROM `your-project-id.analytics_staging.stg_customers` LIMIT 10;

-- ตรวจสอบ marts tables
SELECT * FROM `your-project-id.analytics_marts.customer_orders` LIMIT 10;
```

---

## การติดตั้งบน GKE

### ขั้นตอนที่ 1: สร้าง GKE Cluster

```bash
# ตั้งค่า variables
export PROJECT_ID=your-project-id
export CLUSTER_NAME=airflow-cluster
export REGION=us-central1
export ZONE=us-central1-a

# สร้าง GKE cluster พร้อม Workload Identity
gcloud container clusters create $CLUSTER_NAME \
  --zone=$ZONE \
  --num-nodes=3 \
  --machine-type=n1-standard-4 \
  --disk-size=50GB \
  --enable-autoscaling \
  --min-nodes=2 \
  --max-nodes=5 \
  --enable-stackdriver-kubernetes \
  --workload-pool=${PROJECT_ID}.svc.id.goog \
  --enable-ip-alias \
  --network=default \
  --subnetwork=default \
  --addons=HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver

# รับ credentials สำหรับ kubectl
gcloud container clusters get-credentials $CLUSTER_NAME --zone=$ZONE
```

### ขั้นตอนที่ 2: ตั้งค่า Workload Identity

```bash
# รันสคริปต์ตั้งค่า
export GCP_PROJECT_ID=$PROJECT_ID
export GKE_CLUSTER_NAME=$CLUSTER_NAME
export GKE_CLUSTER_ZONE=$ZONE

./config/setup-workload-identity.sh
```

สคริปต์นี้จะ:
- สร้าง GCP service account ชื่อ `dbt-airflow`
- ให้สิทธิ์ BigQuery และ Cloud Storage
- สร้าง Kubernetes service account ชื่อ `airflow-sa`
- เชื่อมโยง K8s SA กับ GCP SA

### ขั้นตอนที่ 3: Build และ Push Docker Image

```bash
# ตั้งค่า Docker สำหรับ GCR
gcloud auth configure-docker

# Build image
docker build -t gcr.io/${PROJECT_ID}/airflow-dbt:latest .

# Push ไปยัง Google Container Registry
docker push gcr.io/${PROJECT_ID}/airflow-dbt:latest

# หรือใช้ Make command
make push-image GCP_PROJECT_ID=$PROJECT_ID
```

### ขั้นตอนที่ 4: สร้าง Fernet Key

```bash
# สร้าง Fernet key สำหรับ Airflow
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# บันทึกค่าที่ได้ เช่น: xYz123ABC...

# แปลงเป็น base64
echo -n "xYz123ABC..." | base64

# บันทึกค่า base64 ที่ได้
```

### ขั้นตอนที่ 5: อัปเดต Kubernetes Manifests

แก้ไขไฟล์ `config/airflow-gke-deployment.yaml`:

```bash
# ใช้ sed เพื่อแทนที่ Project ID
sed -i "s/YOUR-PROJECT-ID/${PROJECT_ID}/g" config/airflow-gke-deployment.yaml
```

แก้ไขด้วยตนเอง:
1. เปิดไฟล์ `config/airflow-gke-deployment.yaml`
2. แทนที่ `YOUR-PROJECT-ID` ทั้งหมดด้วย project ID จริง
3. อัปเดต `fernet-key` ใน Secret `airflow-secrets` ด้วยค่า base64 ที่สร้างไว้
4. อัปเดต `postgres-password` ถ้าต้องการเปลี่ยน

### ขั้นตอนที่ 6: Deploy ไปยัง GKE

```bash
# Apply Kubernetes manifests
kubectl apply -f config/airflow-gke-deployment.yaml

# ตรวจสอบ namespace
kubectl get namespace airflow

# ตรวจสอบ pods
kubectl get pods -n airflow

# ตรวจสอบ services
kubectl get services -n airflow

# ดู logs
kubectl logs -n airflow -l component=webserver
kubectl logs -n airflow -l component=scheduler
```

### ขั้นตอนที่ 7: เข้าถึง Airflow UI

```bash
# รอให้ LoadBalancer ได้รับ External IP (อาจใช้เวลา 2-3 นาที)
kubectl get service airflow-webserver-service -n airflow -w

# เมื่อได้ IP แล้ว
export EXTERNAL_IP=$(kubectl get service airflow-webserver-service -n airflow -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "Airflow UI: http://${EXTERNAL_IP}"

# เปิดเว็บเบราว์เซอร์ไปที่ IP ที่ได้
```

### ขั้นตอนที่ 8: ตั้งค่า Airflow (บน GKE)

```bash
# เข้าไปใน webserver pod
kubectl exec -it -n airflow deployment/airflow-webserver -- bash

# ตั้งค่า variables และ connections
export GCP_PROJECT_ID=your-project-id
export DBT_DATASET=analytics
export GCP_LOCATION=asia-southeast1
export USE_WORKLOAD_IDENTITY=true

# รันสคริปต์ตั้งค่า
/opt/airflow/config/setup-airflow-config.sh

# ออกจาก pod
exit
```

---

## การตั้งค่า dbt Models

### โครงสร้างโปรเจค dbt

```
dbt_project/
├── dbt_project.yml          # การตั้งค่าโปรเจค
├── profiles.yml              # การตั้งค่า connection
├── packages.yml              # dbt packages
├── models/
│   ├── staging/
│   │   ├── sources.yml      # ตาราง source
│   │   ├── schema.yml       # schema และ tests
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   └── marts/
│       ├── schema.yml
│       └── customer_orders.sql
```

### การสร้าง Model ใหม่

#### 1. เพิ่ม Source Table

แก้ไข `dbt_project/models/staging/sources.yml`:

```yaml
sources:
  - name: raw
    database: "your-project-id"
    schema: raw_data
    tables:
      # เพิ่มตารางใหม่
      - name: products
        description: "ตารางสินค้า"
        columns:
          - name: product_id
            description: "รหัสสินค้า"
          - name: product_name
            description: "ชื่อสินค้า"
```

#### 2. สร้าง Staging Model

สร้างไฟล์ `dbt_project/models/staging/stg_products.sql`:

```sql
{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH source AS (
    SELECT
        product_id,
        product_name,
        product_category,
        product_price,
        created_at
    FROM {{ source('raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        LOWER(TRIM(product_category)) AS product_category,
        CAST(product_price AS NUMERIC) AS product_price,
        created_at
    FROM source
)

SELECT * FROM renamed
```

#### 3. เพิ่ม Tests

แก้ไข `dbt_project/models/staging/schema.yml`:

```yaml
models:
  - name: stg_products
    description: "Staging model สำหรับสินค้า"
    columns:
      - name: product_id
        description: "รหัสสินค้า"
        tests:
          - unique
          - not_null
      - name: product_name
        description: "ชื่อสินค้า"
        tests:
          - not_null
```

#### 4. สร้าง Marts Model

สร้างไฟล์ `dbt_project/models/marts/product_sales.sql`:

```sql
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.order_amount) AS total_revenue
    FROM products p
    LEFT JOIN orders o ON p.product_id = o.product_id
    GROUP BY 1, 2, 3
)

SELECT * FROM product_sales
```

### การทดสอบ Models

```bash
# ทดสอบใน local container
docker-compose exec airflow-webserver bash

cd /opt/airflow/dbt_project

# ติดตั้ง dependencies
dbt deps

# Compile models (ไม่รัน)
dbt compile

# รัน model เฉพาะ
dbt run --models stg_products

# รัน models ทั้งหมดใน staging
dbt run --models staging

# รัน tests
dbt test --models stg_products

# ดู lineage graph
dbt docs generate
dbt docs serve  # จะเปิดที่ port 8080 (อาจต้องเปลี่ยน port)
```

---

## การใช้งาน DAGs

### DAG แบบละเอียด (`dbt_dag.py`)

**Tasks:**
1. `setup_gcp_credentials` - ตั้งค่า GCP credentials
2. `dbt_debug` - ตรวจสอบ connection
3. `dbt_deps` - ติดตั้ง packages
4. `dbt_seed` - โหลด seed data
5. `dbt_run_staging` - รัน staging models
6. `dbt_test_staging` - ทดสอบ staging models
7. `dbt_run_marts` - รัน marts models
8. `dbt_test_marts` - ทดสอบ marts models
9. `dbt_docs_generate` - สร้างเอกสาร

**การปรับแต่ง Schedule:**

แก้ไขไฟล์ `dags/dbt_dag.py`:

```python
# รันทุกวันเวลา 6 โมงเช้า
schedule_interval="0 6 * * *"

# รันทุกชั่วโมง
schedule_interval="0 * * * *"

# รันทุกวันจันทร์เวลา 8 โมงเช้า
schedule_interval="0 8 * * 1"

# ไม่รันอัตโนมัติ (ต้อง trigger ด้วยตนเอง)
schedule_interval=None
```

### DAG แบบง่าย (`dbt_simple_dag.py`)

รันคำสั่ง dbt ทั้งหมดใน task เดียว เหมาะสำหรับ:
- โปรเจคเล็ก
- Pipeline ง่ายๆ
- การทดสอบ

---

## การแก้ไขปัญหา

### ปัญหา: dbt connection ไม่ได้

**อาการ:**
```
Database Error in model ...
Could not connect to BigQuery
```

**แก้ไข:**
1. ตรวจสอบ service account permissions
2. ตรวจสอบว่า key file อยู่ที่ถูกต้อง
3. รัน `dbt debug` เพื่อดูรายละเอียด

```bash
docker-compose exec airflow-webserver bash
cd /opt/airflow/dbt_project
export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json
export GCP_PROJECT_ID=your-project-id
dbt debug
```

### ปัญหา: DAG import errors

**อาการ:**
DAG ไม่แสดงใน UI หรือมี error

**แก้ไข:**
```bash
# ตรวจสอบ logs
docker-compose logs airflow-scheduler | grep ERROR

# ทดสอบ DAG
docker-compose exec airflow-webserver python /opt/airflow/dags/dbt_dag.py

# หรือใช้ Airflow CLI
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags test dbt_bigquery_pipeline
```

### ปัญหา: Workload Identity ใช้งานไม่ได้

**อาการ:**
Permission denied เมื่อรันบน GKE

**แก้ไข:**
```bash
# ตรวจสอบ service account annotation
kubectl describe sa airflow-sa -n airflow

# ตรวจสอบ IAM bindings
gcloud iam service-accounts get-iam-policy \
  dbt-airflow@${PROJECT_ID}.iam.gserviceaccount.com

# ทดสอบ Workload Identity
kubectl run -it --rm test-wi \
  --image=google/cloud-sdk:slim \
  --serviceaccount=airflow-sa \
  --namespace=airflow \
  -- gcloud auth list
```

### ปัญหา: Out of Memory

**อาการ:**
Pods ถูก killed หรือ restart บ่อย

**แก้ไข:**
แก้ไข `config/airflow-gke-deployment.yaml`:

```yaml
resources:
  requests:
    memory: "4Gi"  # เพิ่มจาก 2Gi
    cpu: "2000m"   # เพิ่มจาก 1000m
  limits:
    memory: "8Gi"  # เพิ่มจาก 4Gi
    cpu: "4000m"   # เพิ่มจาก 2000m
```

---

## เคล็ดลับและแนวทางปฏิบัติที่ดี

### 1. การจัดการ Secrets

```bash
# ใช้ Google Secret Manager
gcloud secrets create airflow-fernet-key --data-file=-
# paste your key and press Ctrl+D

# อ้างอิงใน Kubernetes
# ใช้ External Secrets Operator หรือ Workload Identity
```

### 2. การ Monitor และ Alert

```bash
# ตั้งค่า email alerts ใน Airflow
# แก้ไขใน DAG:
default_args = {
    'email': ['your-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

### 3. การ Backup

```bash
# Backup PostgreSQL (Airflow metadata)
kubectl exec -n airflow deployment/postgres -- \
  pg_dump -U airflow airflow > backup.sql

# Restore
kubectl exec -i -n airflow deployment/postgres -- \
  psql -U airflow airflow < backup.sql
```

### 4. การอัปเกรด

```bash
# อัปเดต Docker image
docker-compose pull
docker-compose up -d

# อัปเกรดบน GKE
docker build -t gcr.io/${PROJECT_ID}/airflow-dbt:v2 .
docker push gcr.io/${PROJECT_ID}/airflow-dbt:v2

kubectl set image deployment/airflow-webserver \
  webserver=gcr.io/${PROJECT_ID}/airflow-dbt:v2 -n airflow
```

---

## ทรัพยากรเพิ่มเติม

- [เอกสาร Airflow (EN)](https://airflow.apache.org/docs/)
- [เอกสาร dbt (EN)](https://docs.getdbt.com/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)

---

**หมายเหตุ:** คู่มือนี้สร้างขึ้นเพื่อช่วยให้การติดตั้งและใช้งานง่ายขึ้น หากพบปัญหาหรือมีคำถาม กรุณาเปิด issue ใน repository
