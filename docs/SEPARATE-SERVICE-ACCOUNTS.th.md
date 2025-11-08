# คู่มือการใช้ Service Accounts แยกกันสำหรับแต่ละ dbt Project

[English](SEPARATE-SERVICE-ACCOUNTS.md) | **ภาษาไทย**

## สารบัญ

1. [ทำไมต้องใช้ Service Account แยกกัน](#ทำไมต้องใช้-service-account-แยกกัน)
2. [ภาพรวมสถาปัตยกรรม](#ภาพรวมสถาปัตยกรรม)
3. [การตั้งค่า (Local Development)](#การตั้งค่า-local-development)
4. [การตั้งค่า (GKE with Workload Identity)](#การตั้งค่า-gke-with-workload-identity)
5. [การจัดการ Permissions](#การจัดการ-permissions)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

---

## ทำไมต้องใช้ Service Account แยกกัน

### ข้อดี

✅ **Security Isolation**
- แต่ละ project เข้าถึงได้เฉพาะข้อมูลของตัวเอง
- ลดความเสี่ยงจากการเข้าถึงข้อมูลโดยไม่ได้รับอนุญาต
- ง่ายต่อการ audit และ track การเข้าถึง

✅ **Principle of Least Privilege**
- Sales team เข้าถึงเฉพาะ sales data
- Marketing team เข้าถึงเฉพาะ marketing data
- Finance team เข้าถึงเฉพาะ finance data

✅ **Team Autonomy**
- แต่ละทีมจัดการ permissions ของตัวเอง
- ไม่กระทบกับ projects อื่น
- ง่ายต่อการ onboard/offboard team members

✅ **Compliance**
- ตอบสนอง compliance requirements (SOC 2, GDPR, PDPA)
- Audit trail ชัดเจนว่าใครเข้าถึงข้อมูลอะไร
- Data segregation ตาม regulations

### กรณีการใช้งาน

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
- ข้อมูล Finance ต้องการ security สูง
- แยก service accounts ชัดเจน
- Audit logs แยกตาม service account

---

## ภาพรวมสถาปัตยกรรม

### แบบ Service Account เดียว (ก่อนหน้า)

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

**ปัญหา:**
- Sales project เข้าถึง finance data ได้
- Marketing project เข้าถึง sales data ได้
- Security risk สูง

### แบบ Service Account แยกกัน (ใหม่)

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

**ข้อดี:**
- แต่ละ project เข้าถึงได้เฉพาะ datasets ของตัวเอง
- Security isolation ชัดเจน
- ง่ายต่อการ audit

---

## การตั้งค่า (Local Development)

### ขั้นตอนที่ 1: สร้าง Service Accounts

รันสคริปต์อัตโนมัติ:

```bash
# Set environment variables
export GCP_PROJECT_ID="your-project-id"
export GCP_LOCATION="US"

# Run the setup script
./config/setup-multi-service-accounts.sh
```

**สคริปต์จะทำอะไร:**
1. สร้าง 3 service accounts:
   - `sales-dbt@PROJECT.iam.gserviceaccount.com`
   - `marketing-dbt@PROJECT.iam.gserviceaccount.com`
   - `finance-dbt@PROJECT.iam.gserviceaccount.com`

2. สร้าง BigQuery datasets:
   - Sales: `sales_raw`, `sales_staging`, `sales_marts`
   - Marketing: `marketing_raw`, `marketing_staging`, `marketing_marts`
   - Finance: `finance_raw`, `finance_staging`, `finance_marts`

3. ให้ permissions:
   - `READER` บน raw datasets
   - `WRITER` บน staging และ marts datasets
   - `bigquery.jobUser` สำหรับรัน queries

4. ดาวน์โหลด key files:
   - `config/sales-sa-key.json`
   - `config/marketing-sa-key.json`
   - `config/finance-sa-key.json`

### ขั้นตอนที่ 2: ตั้งค่า Environment Variables

อัปเดตไฟล์ `.env`:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-actual-project-id
GCP_LOCATION=US

# Service Account Keys for Each Project
SALES_SERVICE_ACCOUNT_KEY=/opt/airflow/config/sales-sa-key.json
MARKETING_SERVICE_ACCOUNT_KEY=/opt/airflow/config/marketing-sa-key.json
FINANCE_SERVICE_ACCOUNT_KEY=/opt/airflow/config/finance-sa-key.json

# Datasets
SALES_DATASET=sales_analytics
MARKETING_DATASET=marketing_analytics
FINANCE_DATASET=finance_analytics
```

### ขั้นตอนที่ 3: คัดลอก Key Files

```bash
# Ensure key files are in the right location
cp config/sales-sa-key.json config/
cp config/marketing-sa-key.json config/
cp config/finance-sa-key.json config/

# Verify
ls -la config/*.json
```

### ขั้นตอนที่ 4: ทดสอบ

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

## การตั้งค่า (GKE with Workload Identity)

### ขั้นตอนที่ 1: สร้าง Service Accounts (เหมือน Local)

```bash
export GCP_PROJECT_ID="your-project-id"
./config/setup-multi-service-accounts.sh
```

### ขั้นตอนที่ 2: ตั้งค่า Workload Identity

```bash
# Set cluster information
export GKE_CLUSTER_NAME="airflow-cluster"
export GKE_CLUSTER_ZONE="us-central1-a"

# Run Workload Identity setup script
./config/setup-multi-workload-identity.sh
```

**สคริปต์จะทำอะไร:**
1. Enable Workload Identity on GKE cluster
2. สร้าง Kubernetes namespace `airflow`
3. สร้าง Kubernetes Service Account `airflow-sa`
4. Bind แต่ละ GCP SA กับ K8s SA:
   ```bash
   sales-dbt@PROJECT → airflow-sa
   marketing-dbt@PROJECT → airflow-sa
   finance-dbt@PROJECT → airflow-sa
   ```
5. สร้าง ConfigMap พร้อม SA emails

### ขั้นตอนที่ 3: อัปเดต dbt Profiles

แก้ไข `dbt_projects/*/profiles.yml` เพื่อใช้ `impersonate_service_account`:

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

ทำเหมือนกันสำหรับ Marketing และ Finance.

### ขั้นตอนที่ 4: อัปเดต Kubernetes Deployment

เพิ่ม environment variables ใน `config/airflow-gke-deployment.yaml`:

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

### ขั้นตอนที่ 5: Deploy ไปยัง GKE

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

## การจัดการ Permissions

### ตาราง Permissions

| Project | Service Account | Datasets (READER) | Datasets (WRITER) |
|---------|----------------|-------------------|-------------------|
| Sales | sales-dbt@ | sales_raw | sales_staging, sales_marts |
| Marketing | marketing-dbt@ | marketing_raw | marketing_staging, marketing_marts |
| Finance | finance-dbt@ | finance_raw | finance_staging, finance_marts |

### เพิ่ม Cross-Project Access

ถ้า Marketing ต้องการอ่านข้อมูลจาก Sales:

```bash
# Grant Marketing SA read access to sales_marts
bq show --format=prettyjson PROJECT:sales_marts | \
  jq '.access += [{"role": "READER", "userByEmail": "marketing-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_marts
```

### ตรวจสอบ Permissions

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

**ดี:**
```
sales-dbt@project.iam.gserviceaccount.com
marketing-dbt@project.iam.gserviceaccount.com
finance-dbt@project.iam.gserviceaccount.com
```

**ไม่ดี:**
```
sa1@project.iam.gserviceaccount.com
service-account@project.iam.gserviceaccount.com
dbt@project.iam.gserviceaccount.com
```

### 2. Key Rotation

**หมุนเวียน keys ทุก 90 วัน:**

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

**เริ่มจาก minimum permissions:**

```bash
# Start with READER only
bq show --format=prettyjson PROJECT:sales_raw | \
  jq '.access += [{"role": "READER", "userByEmail": "sales-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_raw

# Add WRITER only when needed
```

### 4. Monitoring

**ตั้งค่า audit logs:**

```bash
# Enable Data Access audit logs
gcloud logging read \
  "protoPayload.authenticationInfo.principalEmail=sales-dbt@PROJECT.iam.gserviceaccount.com" \
  --limit 50 \
  --format json
```

### 5. Documentation

**บันทึกว่า service account ไหนใช้งานอะไร:**

```markdown
# Service Accounts

## sales-dbt@project.iam
- **Purpose**: Run Sales analytics dbt models
- **Owner**: Sales Data Team (sales-data@company.com)
- **Datasets**: sales_raw (R), sales_staging (RW), sales_marts (RW)
- **Last rotated**: 2024-01-15
- **Next rotation**: 2024-04-15
```

---

## Troubleshooting

### ปัญหา: Permission Denied เมื่อรัน dbt

**Error:**
```
google.api_core.exceptions.PermissionDenied: 403 Access Denied: Dataset project:sales_staging: User does not have permission to create table
```

**แก้ไข:**

1. ตรวจสอบว่าใช้ service account ถูกต้อง:
```bash
# ใน DAG หรือ container
echo $SALES_SERVICE_ACCOUNT_KEY
# หรือ
echo $SALES_SERVICE_ACCOUNT_EMAIL
```

2. ตรวจสอบ permissions:
```bash
bq show --format=prettyjson PROJECT:sales_staging | jq '.access'
```

3. เพิ่ม permissions ถ้าจำเป็น:
```bash
bq show --format=prettyjson PROJECT:sales_staging | \
  jq '.access += [{"role": "WRITER", "userByEmail": "sales-dbt@PROJECT.iam.gserviceaccount.com"}]' | \
  bq update --source /dev/stdin PROJECT:sales_staging
```

### ปัญหา: Workload Identity ไม่ทำงาน

**Error:**
```
Could not automatically determine credentials
```

**แก้ไข:**

1. ตรวจสอบ service account annotation:
```bash
kubectl describe sa airflow-sa -n airflow | grep iam.gke.io
```

2. ตรวจสอบ IAM binding:
```bash
gcloud iam service-accounts get-iam-policy \
  sales-dbt@PROJECT.iam.gserviceaccount.com
```

3. ตรวจสอบว่า pod ใช้ service account ถูกต้อง:
```bash
kubectl get pod -n airflow -o yaml | grep serviceAccountName
```

### ปัญหา: Key file ไม่พบ

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/config/sales-sa-key.json'
```

**แก้ไข:**

1. ตรวจสอบว่า key file อยู่ใน volume:
```bash
docker-compose exec airflow-webserver ls -la /opt/airflow/config/
```

2. ตรวจสอบ docker-compose.yml volumes:
```yaml
volumes:
  - ./config:/opt/airflow/config
```

3. คัดลอก key files ใหม่:
```bash
cp config/*-sa-key.json config/
docker-compose restart
```

---

## สรุป

การใช้ service accounts แยกกันสำหรับแต่ละ dbt project ช่วยให้:

✅ **ความปลอดภัยดีขึ้น** - Data isolation ชัดเจน
✅ **Compliance** - ตอบสนอง regulatory requirements
✅ **Team Autonomy** - แต่ละทีมจัดการ permissions ของตัวเอง
✅ **Audit Trail** - ตรวจสอบได้ว่าใครเข้าถึงข้อมูลอะไร

**ขั้นตอนสั้นๆ:**

```bash
# 1. สร้าง service accounts
./config/setup-multi-service-accounts.sh

# 2. อัปเดต .env
# ใส่ paths ของ key files

# 3. สำหรับ GKE
./config/setup-multi-workload-identity.sh

# 4. ทดสอบ
docker-compose up -d
# หรือ
kubectl apply -f config/airflow-gke-deployment.yaml
```

**สำหรับข้อมูลเพิ่มเติม:**
- [Multi-Project Guide](MULTI-PROJECT-GUIDE.th.md)
- [Setup Guide](SETUP-GUIDE.th.md)
- [FAQ](FAQ.th.md)
