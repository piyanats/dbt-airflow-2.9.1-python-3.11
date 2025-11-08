# คำถามที่พบบ่อย (FAQ)

## สารบัญ

1. [คำถามทั่วไป](#คำถามทั่วไป)
2. [การติดตั้งและตั้งค่า](#การติดตั้งและตั้งค่า)
3. [Airflow](#airflow)
4. [dbt](#dbt)
5. [BigQuery](#bigquery)
6. [GKE และ Kubernetes](#gke-และ-kubernetes)
7. [ความปลอดภัย](#ความปลอดภัย)

---

## คำถามทั่วไป

### Q: โปรเจคนี้ใช้สำหรับอะไร?

**A:** โปรเจคนี้เป็น template สำหรับสร้าง data pipeline ที่ใช้ Airflow ในการ orchestrate dbt models บน BigQuery เหมาะสำหรับ:
- การสร้าง data warehouse
- การแปลงข้อมูล (ETL/ELT)
- การสร้าง data marts และ analytics tables
- การทำ data modeling แบบอัตโนมัติ

### Q: ต้องใช้ GCP เท่านั้นหรือไม่?

**A:** ไม่จำเป็น แต่โปรเจคนี้ออกแบบมาสำหรับ BigQuery (GCP) โดยเฉพาะ ถ้าต้องการใช้ data warehouse อื่น เช่น:
- **Snowflake**: เปลี่ยนเป็น `dbt-snowflake` adapter
- **Redshift**: ใช้ `dbt-redshift`
- **PostgreSQL**: ใช้ `dbt-postgres`

### Q: ใช้เงินเท่าไหร่?

**A:** ค่าใช้จ่ายประกอบด้วย:
- **GKE**: ~$100-500/เดือน (ขึ้นอยู่กับขนาด cluster)
- **BigQuery**: คิดตาม query และ storage ที่ใช้
- **Cloud Storage**: ~$0.02/GB/เดือน
- **สำหรับ Local Development**: ฟรี (ใช้แค่เครื่องของคุณ)

**เคล็ดลับประหยัด:**
- ใช้ local development ก่อน
- ใช้ GKE Autopilot (pay-per-pod)
- ตั้งค่า autoscaling
- ลบ resources ที่ไม่ใช้

### Q: ต้องมีความรู้อะไรบ้าง?

**A:** ความรู้พื้นฐานที่แนะนำ:
- **SQL**: สำคัญมาก (สำหรับเขียน dbt models)
- **Python**: พื้นฐาน (สำหรับแก้ไข DAGs)
- **Docker**: พื้นฐาน
- **Git**: พื้นฐาน
- **Cloud (GCP)**: พื้นฐาน

ถ้ามีประสบการณ์กับ data analytics หรือ data engineering จะเรียนรู้ได้ง่ายขึ้น

---

## การติดตั้งและตั้งค่า

### Q: ติดตั้ง Docker ยังไง?

**A:**
```bash
# Mac (ใช้ Homebrew)
brew install --cask docker

# หรือดาวน์โหลดจาก
# https://www.docker.com/products/docker-desktop

# Linux (Ubuntu/Debian)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# ตรวจสอบการติดตั้ง
docker --version
docker-compose --version
```

### Q: ใช้ Python เวอร์ชันอื่นได้ไหม?

**A:** โปรเจคนี้ใช้ Python 3.11 แต่สามารถใช้เวอร์ชัน 3.10-3.12 ได้ โดย:
1. แก้ไข `Dockerfile`: `FROM apache/airflow:2.9.1-python3.12`
2. ทดสอบว่า dependencies ทั้งหมดทำงานได้

### Q: Port 8080 ถูกใช้งานอยู่แล้ว ทำยังไง?

**A:** แก้ไขไฟล์ `docker-compose.yml`:
```yaml
services:
  airflow-webserver:
    ports:
      - "8081:8080"  # เปลี่ยนจาก 8080:8080
```
จากนั้นเข้า Airflow ที่ http://localhost:8081

### Q: ลืมรหัสผ่าน Airflow ทำยังไง?

**A:**
```bash
# Reset รหัสผ่าน
docker-compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password newpassword

# หรือ reset ทั้งหมด
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```

### Q: ดาวน์โหลด service account key ยังไง?

**A:**
```bash
# 1. สร้าง service account (ถ้ายังไม่มี)
gcloud iam service-accounts create airflow-dbt \
  --display-name="Airflow dbt SA"

# 2. ให้สิทธิ์
gcloud projects add-iam-policy-binding YOUR-PROJECT-ID \
  --member="serviceAccount:airflow-dbt@YOUR-PROJECT-ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# 3. ดาวน์โหลด key
gcloud iam service-accounts keys create key.json \
  --iam-account=airflow-dbt@YOUR-PROJECT-ID.iam.gserviceaccount.com

# 4. ย้ายไปที่ถูกต้อง
mv key.json config/gcp-key.json
```

---

## Airflow

### Q: จะเปลี่ยน schedule ของ DAG ได้ยังไง?

**A:** แก้ไขไฟล์ `dags/dbt_dag.py`:
```python
with DAG(
    dag_id="dbt_bigquery_pipeline",
    schedule_interval="0 6 * * *",  # เปลี่ยนตรงนี้
    ...
):
```

**ตัวอย่าง schedules:**
```python
"0 6 * * *"      # ทุกวัน 6:00 AM
"0 */6 * * *"    # ทุก 6 ชั่วโมง
"0 8 * * 1"      # ทุกวันจันทร์ 8:00 AM
"0 0 1 * *"      # วันแรกของเดือน 00:00
None             # ไม่รันอัตโนมัติ (manual only)
"@daily"         # ทุกวันเที่ยงคืน
"@hourly"        # ทุกชั่วโมง
```

### Q: จะส่ง email alert เมื่อ DAG fail ได้ไหม?

**A:** ได้ แก้ไขใน DAG:
```python
default_args = {
    'owner': 'data-engineering',
    'email': ['team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,  # ถ้าต้องการรับ email เมื่อสำเร็จ
}
```

จากนั้นตั้งค่า SMTP ใน `airflow.cfg` หรือ environment variables:
```bash
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=your-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your-app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM=airflow@example.com
```

### Q: DAG ไม่แสดงใน UI ทำไม?

**A:** สาเหตุที่เป็นไปได้:
1. **Syntax Error**: ตรวจสอบ logs
   ```bash
   docker-compose logs airflow-scheduler | grep ERROR
   ```

2. **DAG ถูก pause**: เปิดใช้งานใน UI

3. **Import Error**: ทดสอบ import
   ```bash
   docker-compose exec airflow-webserver python /opt/airflow/dags/dbt_dag.py
   ```

4. **ไฟล์อยู่ผิดที่**: ต้องอยู่ใน `dags/` folder

### Q: จะดู logs ของ task ที่ failed ได้ยังไง?

**A:**
1. ใน Airflow UI ไปที่ DAG
2. คลิกที่ task ที่ failed (สีแดง)
3. คลิก "Log"
4. อ่าน error message จากท้ายขึ้นไป

หรือใช้ command line:
```bash
# List task instances
docker-compose exec airflow-webserver \
  airflow tasks list dbt_bigquery_pipeline

# ดู log ของ task
docker-compose exec airflow-webserver \
  airflow tasks logs dbt_bigquery_pipeline dbt_run_staging 2024-01-01
```

### Q: จะ re-run task ที่ failed ได้ไหม?

**A:** ได้หลายวิธี:
1. **ใน UI**: คลิกที่ task → คลิก "Clear" → task จะรันใหม่
2. **Clear ทั้ง DAG**: คลิกที่ DAG → "Clear" → เลือก tasks
3. **CLI**:
   ```bash
   docker-compose exec airflow-webserver \
     airflow tasks clear dbt_bigquery_pipeline --task-regex "dbt_run.*"
   ```

---

## dbt

### Q: dbt คืออะไร?

**A:** dbt (data build tool) เป็นเครื่องมือสำหรับแปลงข้อมูลใน data warehouse โดย:
- เขียน SQL เพื่อแปลงข้อมูล
- สร้าง tables/views อัตโนมัติ
- ทำ data testing
- สร้าง documentation
- จัดการ dependencies ระหว่าง models

### Q: จะเพิ่ม dbt model ใหม่ได้ยังไง?

**A:**
1. สร้างไฟล์ `.sql` ใหม่ใน `dbt_project/models/`
   ```sql
   -- dbt_project/models/staging/stg_products.sql
   SELECT
       product_id,
       product_name,
       product_price
   FROM {{ source('raw', 'products') }}
   ```

2. เพิ่ม source ใน `sources.yml` (ถ้ายังไม่มี)
3. เพิ่ม tests ใน `schema.yml`
4. ทดสอบ:
   ```bash
   docker-compose exec airflow-webserver bash
   cd /opt/airflow/dbt_project
   dbt run --models stg_products
   ```

### Q: Staging vs Marts models แตกต่างกันยังไง?

**A:**
- **Staging Models**:
  - ทำความสะอาดข้อมูลจาก source
  - Rename columns, fix data types
  - มักเป็น `view`
  - 1:1 กับ source tables

- **Marts Models**:
  - รวมข้อมูลจากหลาย staging models
  - Business logic และ aggregations
  - มักเป็น `table`
  - สำหรับ end users/BI tools

### Q: จะทดสอบ dbt model ก่อนรันใน Airflow ได้ไหม?

**A:** ได้ รันใน container:
```bash
docker-compose exec airflow-webserver bash
cd /opt/airflow/dbt_project

# ตั้งค่า env vars
export GCP_PROJECT_ID=your-project-id
export DBT_DATASET=analytics
export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json

# ทดสอบ connection
dbt debug

# Compile (ไม่รัน)
dbt compile --models stg_orders

# รัน model เดียว
dbt run --models stg_orders

# รันทั้ง staging folder
dbt run --models staging

# รัน tests
dbt test --models stg_orders
```

### Q: จะดู data lineage ได้ยังไง?

**A:**
```bash
# สร้าง docs
docker-compose exec airflow-webserver bash
cd /opt/airflow/dbt_project
dbt docs generate

# Serve docs (ต้องเปิด port)
dbt docs serve --port 8001

# เปิดเบราว์เซอร์ไปที่ http://localhost:8001
```

### Q: dbt test คืออะไร?

**A:** dbt tests ใช้ตรวจสอบคุณภาพข้อมูล:
```yaml
# schema.yml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique           # ไม่ซ้ำ
          - not_null         # ไม่เป็น null
      - name: order_amount
        tests:
          - not_null
          - positive_value   # custom test
      - name: customer_id
        tests:
          - relationships:   # มีใน customers table
              to: ref('stg_customers')
              field: customer_id
```

---

## BigQuery

### Q: จะสร้าง dataset ใน BigQuery ได้ยังไง?

**A:**
```sql
-- ใน BigQuery Console
CREATE SCHEMA `your-project-id.analytics`
OPTIONS(
  location="asia-southeast1",  -- หรือ "asia-southeast1"
  description="Analytics dataset for dbt models"
);
```

หรือใช้ command line:
```bash
bq mk --dataset \
  --location=asia-southeast1 \
  --description="Analytics dataset" \
  your-project-id:analytics
```

### Q: ค่าใช้จ่าย BigQuery คำนวณยังไง?

**A:** BigQuery มี 2 ส่วน:
1. **Storage**: $0.02/GB/เดือน (active), $0.01/GB/เดือน (long-term)
2. **Query**: $5/TB ที่ scan

**เคล็ดลับประหยัด:**
- ใช้ `WHERE` clause เพื่อจำกัดข้อมูล
- ใช้ partitioned tables
- ใช้ clustered tables
- Preview ข้อมูลแทน `SELECT *`
- ตั้ง quota limits

### Q: จะเห็นตารางที่ dbt สร้างได้ที่ไหน?

**A:**
1. เปิด [BigQuery Console](https://console.cloud.google.com/bigquery)
2. ดูที่ datasets:
   - `analytics_staging` - staging models
   - `analytics_marts` - marts models
3. คลิกที่ table เพื่อดู schema และ preview

### Q: BigQuery location คืออะไร? ควรเลือกแบบไหน?

**A:** Location คือภูมิภาคที่เก็บข้อมูล:
- **US** (multi-region): ทั่วไป, ราคาประหยัด
- **EU** (multi-region): สำหรับยุโรป
- **asia-southeast1** (Singapore): เอเชียตะวันออกเฉียงใต้
- **asia-northeast1** (Tokyo): ญี่ปุ่น

**คำแนะนำ:** เลือกตาม:
1. ที่ตั้งของ users
2. ข้อกำหนดด้านกฎหมาย (GDPR, PDPA)
3. ต้นทางของข้อมูล

---

## GKE และ Kubernetes

### Q: GKE Autopilot vs Standard แตกต่างกันยังไง?

**A:**
- **Autopilot**: Google จัดการ nodes, pay-per-pod, ง่ายกว่า
- **Standard**: คุณจัดการ nodes เอง, ยืดหยุ่นกว่า

**แนะนำ Autopilot สำหรับ:**
- มือใหม่
- ไม่ต้องการจัดการ infrastructure
- ประหยัดเวลา

### Q: Workload Identity คืออะไร?

**A:** Workload Identity เป็นวิธีที่ปลอดภัยในการให้ GKE pods เข้าถึง GCP services โดย:
- **ไม่ต้องใช้ service account keys**
- เชื่อมโยง K8s service account กับ GCP service account
- Credentials หมุนเวียนอัตโนมัติ
- ปลอดภัยกว่าการใช้ key files

### Q: จะดู logs ของ pods ได้ยังไง?

**A:**
```bash
# List pods
kubectl get pods -n airflow

# ดู logs
kubectl logs -n airflow deployment/airflow-webserver
kubectl logs -n airflow deployment/airflow-scheduler

# Follow logs (real-time)
kubectl logs -f -n airflow -l component=scheduler

# ดู logs ของ pod ที่ crashed
kubectl logs -n airflow <pod-name> --previous
```

### Q: จะเข้าไปใน pod ได้ยังไง?

**A:**
```bash
# เข้าไปใน pod
kubectl exec -it -n airflow deployment/airflow-webserver -- bash

# รันคำสั่งเดียว
kubectl exec -n airflow deployment/airflow-webserver -- airflow version

# เข้าไปใน scheduler
kubectl exec -it -n airflow deployment/airflow-scheduler -- bash
```

### Q: Pod status CrashLoopBackOff แก้ยังไง?

**A:**
```bash
# ดู logs
kubectl logs -n airflow <pod-name>

# ดู events
kubectl describe pod -n airflow <pod-name>

# สาเหตุที่เป็นไปได้:
# 1. Out of memory - เพิ่ม resources
# 2. Configuration ผิด - ตรวจสอบ env vars
# 3. Image ผิด - ตรวจสอบ image tag
# 4. Health check ล้มเหลว - ปรับ liveness/readiness probe
```

---

## ความปลอดภัย

### Q: จะเก็บ secrets อย่างปลอดภัยได้ยังไง?

**A:** แนวทางที่แนะนำ:
1. **Local Dev**: ใช้ `.env` file (อย่า commit!)
2. **GKE**: ใช้ Kubernetes Secrets หรือ Google Secret Manager
3. **CI/CD**: ใช้ GitHub Secrets/GitLab Variables

```bash
# สร้าง secret ใน Kubernetes
kubectl create secret generic airflow-secrets \
  --from-literal=fernet-key="your-fernet-key" \
  --from-file=gcp-key=./key.json \
  -n airflow

# ใช้ Google Secret Manager
gcloud secrets create airflow-fernet-key --data-file=fernet-key.txt
```

### Q: Service account ควรมีสิทธิ์อะไรบ้าง?

**A:** ตามหลักการ **Least Privilege**:
```bash
# สิทธิ์ขั้นต่ำที่จำเป็น:
roles/bigquery.dataEditor      # อ่าน/เขียน tables
roles/bigquery.jobUser         # รัน queries
roles/storage.objectViewer     # อ่าน GCS (ถ้าใช้)

# ไม่แนะนำ:
roles/bigquery.admin          # สิทธิ์มากเกินไป
roles/owner                   # อันตราย!
```

### Q: จะ rotate service account key ได้ยังไง?

**A:**
```bash
# 1. สร้าง key ใหม่
gcloud iam service-accounts keys create new-key.json \
  --iam-account=airflow-dbt@PROJECT-ID.iam.gserviceaccount.com

# 2. อัปเดตใน Kubernetes
kubectl create secret generic airflow-gcp-key \
  --from-file=key.json=new-key.json \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Restart pods
kubectl rollout restart deployment/airflow-webserver -n airflow
kubectl rollout restart deployment/airflow-scheduler -n airflow

# 4. ลบ key เก่า
gcloud iam service-accounts keys list \
  --iam-account=airflow-dbt@PROJECT-ID.iam.gserviceaccount.com

gcloud iam service-accounts keys delete <OLD-KEY-ID> \
  --iam-account=airflow-dbt@PROJECT-ID.iam.gserviceaccount.com
```

### Q: จะตรวจสอบว่ามี vulnerabilities ใน Docker image ไหม?

**A:**
```bash
# Scan ด้วย Google Container Analysis
gcloud container images scan gcr.io/PROJECT-ID/airflow-dbt:latest

# ดูผลลัพธ์
gcloud container images list-tags gcr.io/PROJECT-ID/airflow-dbt --show-occurrences

# Scan ด้วย Trivy (local)
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
  aquasec/trivy image gcr.io/PROJECT-ID/airflow-dbt:latest
```

---

## อื่นๆ

### Q: จะทำ backup Airflow metadata ได้ยังไง?

**A:**
```bash
# Local (PostgreSQL in Docker)
docker-compose exec postgres pg_dump -U airflow airflow > backup-$(date +%Y%m%d).sql

# Restore
docker-compose exec -T postgres psql -U airflow airflow < backup-20240115.sql

# GKE
kubectl exec -n airflow deployment/postgres -- \
  pg_dump -U airflow airflow > backup.sql
```

### Q: จะ migrate จาก local ไป GKE ได้ยังไง?

**A:**
1. Export DAGs และ dbt models (มีอยู่ใน repo แล้ว)
2. Export Airflow connections และ variables:
   ```bash
   airflow connections export connections.json
   airflow variables export variables.json
   ```
3. Build และ push Docker image
4. Deploy ไปยัง GKE ตามคู่มือ
5. Import connections และ variables:
   ```bash
   kubectl exec -n airflow deployment/airflow-webserver -- \
     airflow connections import connections.json
   ```

### Q: จะอัปเดต Airflow version ได้ยังไง?

**A:**
1. แก้ไข `Dockerfile`:
   ```dockerfile
   FROM apache/airflow:2.10.0-python3.11  # เปลี่ยนเวอร์ชัน
   ```
2. ตรวจสอบ [breaking changes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
3. อัปเดต dependencies ถ้าจำเป็น
4. Build และทดสอบ local
5. Deploy ไป GKE

---

**มีคำถามเพิ่มเติม?**
- เปิด issue ใน GitHub repository
- ตรวจสอบ [SETUP-GUIDE.th.md](SETUP-GUIDE.th.md) สำหรับคู่มือการติดตั้ง
- อ่าน [README.th.md](../README.th.md) สำหรับข้อมูลทั่วไป
