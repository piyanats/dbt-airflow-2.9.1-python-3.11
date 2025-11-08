# คู่มือการจัดการ dbt หลาย Projects

## สารบัญ

1. [ภาพรวม](#ภาพรวม)
2. [โครงสร้างโปรเจค](#โครงสร้างโปรเจค)
3. [กรณีการใช้งาน](#กรณีการใช้งาน)
4. [การตั้งค่า](#การตั้งค่า)
5. [DAGs สำหรับ Multiple Projects](#dags-สำหรับ-multiple-projects)
6. [ตัวอย่างโปรเจค](#ตัวอย่างโปรเจค)
7. [Best Practices](#best-practices)

---

## ภาพรวม

โปรเจคนี้รองรับการรันหลาย dbt projects ใน Airflow DAG เดียวกัน ซึ่งมีประโยชน์เมื่อ:

- แยก data domains ต่างกัน (Sales, Marketing, Finance)
- แต่ละทีมจัดการ models ของตัวเอง
- ต้องการ isolation ระหว่าง projects
- แต่ละ project มี schedules หรือ dependencies ต่างกัน

### ข้อดี

✅ **Separation of Concerns**: แต่ละ project จัดการ domain ของตัวเอง
✅ **Team Autonomy**: ทีมต่างๆ ทำงานแยกกันได้
✅ **Parallel Execution**: รัน projects พร้อมกันเพื่อประหยัดเวลา
✅ **Easier Debugging**: แยก logs และ errors ของแต่ละ project
✅ **Flexible Dependencies**: กำหนด dependencies ระหว่าง projects ได้

### ข้อควรพิจารณา

⚠️ **Complexity**: จัดการหลาย projects ซับซ้อนกว่า single project
⚠️ **Resources**: ใช้ resources มากขึ้นถ้ารันพร้อมกัน
⚠️ **Coordination**: ต้องประสานงานเมื่อมี dependencies ข้าม projects

---

## โครงสร้างโปรเจค

```
dbt_projects/
├── sales_analytics/              # Project 1: Sales data
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml       # Source: sales_raw
│       │   └── stg_transactions.sql
│       └── marts/
│           └── daily_sales.sql
│
├── marketing_analytics/          # Project 2: Marketing data
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml       # Source: marketing_raw
│       │   └── stg_campaigns.sql
│       └── marts/
│           └── campaign_performance.sql
│
└── finance_analytics/            # Project 3: Finance data
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── staging/
        │   ├── sources.yml       # Source: finance_raw
        │   └── stg_invoices.sql
        └── marts/
            └── monthly_revenue.sql
```

---

## กรณีการใช้งาน

### Use Case 1: Domain-Driven Design

แยก projects ตาม business domains:

```
sales_analytics/     → ข้อมูลการขาย, transactions, products
marketing_analytics/ → ข้อมูล campaigns, leads, conversions
finance_analytics/   → ข้อมูลการเงิน, invoices, payments
```

**เหมาะกับ:**
- องค์กรขนาดกลาง-ใหญ่
- แต่ละ department มีทีม data เป็นของตัวเอง
- ข้อมูลมาจาก sources ต่างกัน

### Use Case 2: Environment Separation

แยก projects ตาม environments:

```
production_analytics/    → Production data models
staging_analytics/       → Staging/testing models
development_analytics/   → Development experiments
```

**เหมาะกับ:**
- ต้องการ test models ก่อน production
- Experimentation และ prototyping
- Multiple versions ของ models

### Use Case 3: Client/Tenant Separation

แยก projects ตาม clients (multi-tenant):

```
client_a_analytics/      → Client A data
client_b_analytics/      → Client B data
client_c_analytics/      → Client C data
```

**เหมาะกับ:**
- SaaS companies ที่มีหลาย clients
- Data isolation requirements
- Client-specific transformations

---

## การตั้งค่า

### ขั้นตอนที่ 1: ตั้งค่า Environment Variables

อัปเดตไฟล์ `.env`:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-gcp-project-id
GCP_LOCATION=asia-southeast1
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json

# dbt Configuration (Multiple Projects)
SALES_DATASET=sales_analytics
MARKETING_DATASET=marketing_analytics
FINANCE_DATASET=finance_analytics
```

### ขั้นตอนที่ 2: สร้าง BigQuery Datasets

สร้าง datasets สำหรับแต่ละ project:

```sql
-- Sales Analytics datasets
CREATE SCHEMA IF NOT EXISTS `your-project-id.sales_raw`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.sales_staging`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.sales_marts`;

-- Marketing Analytics datasets
CREATE SCHEMA IF NOT EXISTS `your-project-id.marketing_raw`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.marketing_staging`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.marketing_marts`;

-- Finance Analytics datasets
CREATE SCHEMA IF NOT EXISTS `your-project-id.finance_raw`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.finance_staging`;
CREATE SCHEMA IF NOT EXISTS `your-project-id.finance_marts`;
```

### ขั้นตอนที่ 3: ตั้งค่า Airflow Variables

ใน Airflow UI (Admin → Variables):

| Key | Value | Description |
|-----|-------|-------------|
| gcp_project_id | your-project-id | GCP Project ID |
| gcp_location | asia-southeast1 | BigQuery location |
| sales_dataset | sales_analytics | Sales dataset |
| marketing_dataset | marketing_analytics | Marketing dataset |
| finance_dataset | finance_analytics | Finance dataset |

---

## DAGs สำหรับ Multiple Projects

### DAG 1: Parallel Execution (แนะนำ)

**ไฟล์:** `dags/dbt_multi_project_dag.py`

**คุณสมบัติ:**
- รัน projects พร้อมกันเพื่อประหยัดเวลา
- แต่ละ project มี task group แยกกัน
- เหมาะกับ projects ที่ไม่ depend กัน

**โครงสร้าง DAG:**

```
setup_gcp_credentials
        |
    ┌───┴────┬────────────┐
    ▼        ▼            ▼
[Sales]  [Marketing]  [Finance]
    |        |            |
dbt_deps dbt_deps     dbt_deps
    |        |            |
dbt_run  dbt_run      dbt_run
    |        |            |
dbt_test dbt_test     dbt_test
```

**ตัวอย่างการใช้:**

```python
# Projects รันพร้อมกัน (parallel)
setup_credentials >> [
    sales_pipeline,
    marketing_pipeline,
    finance_pipeline
]
```

**เวลาที่ใช้:** ~30 นาที (ถ้าแต่ละ project ใช้เวลา 30 นาที)

### DAG 2: Sequential Execution

**ไฟล์:** `dags/dbt_multi_project_sequential_dag.py`

**คุณสมบัติ:**
- รัน projects ตามลำดับ
- เหมาะกับ projects ที่ depend กัน
- ถ้า project แรก fail จะไม่รัน project ถัดไป

**โครงสร้าง DAG:**

```
[Sales]
   |
   ▼
[Marketing]  (รอ Sales เสร็จ)
   |
   ▼
[Finance]    (รอ Marketing เสร็จ)
```

**ตัวอย่างการใช้:**

```python
# Projects รันตามลำดับ (sequential)
sales_pipeline >> marketing_pipeline >> finance_pipeline
```

**เวลาที่ใช้:** ~90 นาที (30 + 30 + 30 นาที)

### DAG 3: Custom Dependencies

สร้าง dependencies แบบ custom:

```python
# Sales และ Marketing รันพร้อมกัน
# Finance รอทั้งสองเสร็จ
setup_credentials >> [sales_pipeline, marketing_pipeline] >> finance_pipeline
```

**โครงสร้าง:**

```
setup_credentials
        |
    ┌───┴────┐
    ▼        ▼
[Sales]  [Marketing]
    └────┬───┘
         ▼
     [Finance]
```

---

## ตัวอย่างโปรเจค

### Sales Analytics Project

**วัตถุประสงค์:** วิเคราะห์ข้อมูลการขาย

**Source Tables:**
- `sales_raw.transactions` - ข้อมูลการขาย
- `sales_raw.products` - ข้อมูลสินค้า
- `sales_raw.stores` - ข้อมูลร้านค้า

**Models:**
- `stg_transactions.sql` - Staging transactions
- `daily_sales.sql` - ยอดขายรายวัน

**Output Datasets:**
- `sales_staging` - Staging models
- `sales_marts` - Marts models

### Marketing Analytics Project

**วัตถุประสงค์:** วิเคราะห์ประสิทธิภาพ marketing campaigns

**Source Tables:**
- `marketing_raw.campaigns` - ข้อมูล campaigns
- `marketing_raw.leads` - ข้อมูล leads
- `marketing_raw.conversions` - ข้อมูล conversions

**Models:**
- `stg_campaigns.sql` - Staging campaigns
- `campaign_performance.sql` - ผลลัพธ์ campaigns

**Output Datasets:**
- `marketing_staging` - Staging models
- `marketing_marts` - Marts models

### Finance Analytics Project

**วัตถุประสงค์:** วิเคราะห์ข้อมูลการเงิน

**Source Tables:**
- `finance_raw.invoices` - ข้อมูล invoices
- `finance_raw.payments` - ข้อมูลการจ่ายเงิน
- `finance_raw.expenses` - ข้อมูลค่าใช้จ่าย

**Models:**
- `stg_invoices.sql` - Staging invoices
- `monthly_revenue.sql` - รายได้รายเดือน

**Output Datasets:**
- `finance_staging` - Staging models
- `finance_marts` - Marts models

---

## Best Practices

### 1. การตั้งชื่อ Projects

**ดี:**
```
sales_analytics/
marketing_analytics/
finance_analytics/
```

**ไม่ดี:**
```
project1/
proj2/
data/
```

### 2. การจัดการ Dependencies

**ถ้า projects ไม่ depend กัน:**
```python
# รันพร้อมกัน
setup >> [sales, marketing, finance]
```

**ถ้ามี dependencies:**
```python
# Marketing ต้องการข้อมูลจาก Sales
setup >> sales >> marketing >> finance
```

**ถ้ามี dependencies บางส่วน:**
```python
# Finance ต้องการทั้ง Sales และ Marketing
setup >> [sales, marketing] >> finance
```

### 3. การจัดการ Resources

**Parallel execution:**
- ตั้งค่า BigQuery quota limits
- Monitor memory usage
- ใช้ different time slots ถ้า resources จำกัด

```python
# ตัวอย่าง: รัน Sales ตอนเช้า, Marketing บ่าย, Finance เย็น
sales_dag = DAG(schedule_interval="0 6 * * *")    # 6 AM
marketing_dag = DAG(schedule_interval="0 12 * * *") # 12 PM
finance_dag = DAG(schedule_interval="0 18 * * *")  # 6 PM
```

### 4. Shared Models และ Macros

**ถ้ามี models ที่ใช้ร่วมกัน:**

สร้าง shared project:

```
dbt_projects/
├── shared_utils/              # Shared utilities
│   ├── macros/
│   │   └── common_macros.sql
│   └── models/
│       └── dim_date.sql
│
├── sales_analytics/           # Reference shared project
├── marketing_analytics/       # Reference shared project
└── finance_analytics/         # Reference shared project
```

**ใน packages.yml:**
```yaml
packages:
  - local: ../shared_utils
```

### 5. Testing Strategies

**แต่ละ project ควรมี tests:**

```yaml
# schema.yml
models:
  - name: daily_sales
    tests:
      - dbt_utils.recency:
          datepart: day
          field: transaction_date
          interval: 1
```

**Cross-project tests:**
```sql
-- tests/assert_sales_marketing_consistent.sql
SELECT
    s.transaction_id
FROM {{ ref('sales_analytics', 'stg_transactions') }} s
LEFT JOIN {{ ref('marketing_analytics', 'stg_conversions') }} m
    ON s.transaction_id = m.transaction_id
WHERE m.transaction_id IS NULL
```

### 6. Monitoring และ Alerting

**ตั้งค่า alerts แยกตาม project:**

```python
default_args = {
    'email': ['sales-team@example.com'],
    'email_on_failure': True,
}

sales_dag = DAG(
    'sales_analytics',
    default_args=default_args,
)

# Marketing team gets different alerts
marketing_args = {
    'email': ['marketing-team@example.com'],
    'email_on_failure': True,
}
```

### 7. Version Control

**แยก branches ตาม projects:**

```bash
# Git workflow
git checkout -b sales/new-model
git checkout -b marketing/campaign-analysis
git checkout -b finance/revenue-report
```

**หรือใช้ monorepo:**
```
.
├── dbt_projects/
│   ├── sales_analytics/
│   │   └── CHANGELOG.md
│   ├── marketing_analytics/
│   │   └── CHANGELOG.md
│   └── finance_analytics/
│       └── CHANGELOG.md
```

### 8. การจัดการ Credentials

**แยก service accounts ตาม project:**

```yaml
# sales_analytics/profiles.yml
sales_analytics:
  outputs:
    prod:
      type: bigquery
      keyfile: /opt/airflow/config/sales-sa-key.json

# marketing_analytics/profiles.yml
marketing_analytics:
  outputs:
    prod:
      type: bigquery
      keyfile: /opt/airflow/config/marketing-sa-key.json
```

---

## การ Troubleshooting

### ปัญหา: Project ไม่พบ models จาก project อื่น

**สาเหตุ:** dbt projects แยกกัน ไม่สามารถ ref() ข้ามได้โดยตรง

**แก้ไข:**

1. **ใช้ sources แทน refs:**
```sql
-- ใน marketing_analytics
-- อ้างอิง output ของ sales_analytics
SELECT *
FROM {{ source('sales_marts', 'daily_sales') }}
```

2. **สร้าง cross-project dependencies:**
```yaml
# marketing_analytics/models/sources.yml
sources:
  - name: sales_marts
    database: "{{ env_var('GCP_PROJECT_ID') }}"
    schema: sales_marts
    tables:
      - name: daily_sales
```

### ปัญหา: Projects รันช้าเกินไป

**แก้ไข:**

1. **ตรวจสอบว่ารัน parallel ได้หรือไม่**
2. **ใช้ incremental models:**
```sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}
```

3. **ลด scope ของ models:**
```sql
-- ใช้ WHERE clause จำกัดข้อมูล
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
```

### ปัญหา: Memory หรือ quota exceeded

**แก้ไข:**

1. **กระจาย schedule:**
```python
sales_dag.schedule_interval = "0 6 * * *"      # 6 AM
marketing_dag.schedule_interval = "0 12 * * *" # 12 PM
finance_dag.schedule_interval = "0 18 * * *"   # 6 PM
```

2. **เพิ่ม resources ใน GKE:**
```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
```

---

## สรุป

การจัดการหลาย dbt projects ช่วยให้:

✅ แยก domains ต่างกันได้ชัดเจน
✅ ทีมต่างๆ ทำงานแยกกันได้
✅ รัน parallel เพื่อประหยัดเวลา
✅ ง่ายต่อการ maintain และ debug

**แต่ต้องวางแผน:**
- Project structure
- Dependencies ระหว่าง projects
- Resource allocation
- Monitoring และ alerting

**เลือก pattern ที่เหมาะกับองค์กร:**
- Parallel สำหรับ independent projects
- Sequential สำหรับ dependent projects
- Custom dependencies สำหรับ complex scenarios

---

**คำแนะนำ:** เริ่มจาก single project ก่อน แล้วค่อยแยกออกเป็นหลาย projects เมื่อจำเป็น
