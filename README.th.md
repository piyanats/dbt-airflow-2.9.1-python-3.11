# Airflow dbt BigQuery Pipeline

[English](README.md) | **ภาษาไทย**

โปรเจคนี้ใช้สำหรับสร้าง Apache Airflow DAGs ที่รัน dbt models บน Google BigQuery โดยออกแบบมาเพื่อให้ทำงานบน Google Kubernetes Engine (GKE) พร้อม Workload Identity

## คุณสมบัติ

- **Airflow 2.9.1** พร้อม Python 3.11
- **dbt-core** พร้อม BigQuery adapter
- รองรับ **GKE Workload Identity** สำหรับการยืนยันตัวตนที่ปลอดภัย
- รองรับ **Docker** สำหรับการพัฒนาในเครื่อง
- **Kubernetes manifests** สำหรับการ deploy บน GKE
- ตัวอย่าง dbt models (staging และ marts)
- ตัวอย่าง DAGs หลายแบบ (แบบละเอียดและแบบง่าย)
- **รองรับการรันหลาย dbt projects** (Sales, Marketing, Finance)

## เอกสารประกอบ

- 📘 **[คู่มือการติดตั้งและใช้งาน](docs/SETUP-GUIDE.th.md)** - คำแนะนำทีละขั้นตอนสำหรับการติดตั้งและใช้งาน
- 🔀 **[คู่มือการจัดการหลาย dbt Projects](docs/MULTI-PROJECT-GUIDE.th.md)** - วิธีการรัน dbt หลาย projects พร้อมกัน
- ❓ **[คำถามที่พบบ่อย (FAQ)](docs/FAQ.th.md)** - คำตอบสำหรับคำถามที่พบบ่อย
- 📖 **[เอกสารภาษาอังกฤษ](README.md)** - English documentation

## โครงสร้างโปรเจค

```
.
├── dags/                           # Airflow DAGs
│   ├── dbt_dag.py                 # DAG หลักสำหรับ dbt pipeline
│   └── dbt_simple_dag.py          # DAG แบบง่าย
├── dbt_project/                    # โปรเจค dbt
│   ├── models/                    # dbt models
│   │   ├── staging/               # Staging models
│   │   └── marts/                 # Marts models
│   ├── dbt_project.yml            # การตั้งค่าโปรเจค dbt
│   └── profiles.yml               # การตั้งค่า profiles สำหรับ dbt
├── config/                         # ไฟล์ configuration
│   ├── airflow-gke-deployment.yaml # Kubernetes deployment
│   ├── setup-workload-identity.sh  # สคริปต์ตั้งค่า workload identity
│   ├── setup-airflow-config.sh     # สคริปต์ตั้งค่า Airflow
│   └── airflow-connections.json    # ตัวอย่างการตั้งค่า connection
├── Dockerfile                      # Docker image definition
├── docker-compose.yml              # ตั้งค่าสำหรับการพัฒนาในเครื่อง
├── requirements.txt                # Python dependencies
├── requirements-dev.txt            # Development dependencies
└── README.md                       # เอกสารภาษาอังกฤษ
```

## ความต้องการเบื้องต้น

### สำหรับการพัฒนาในเครื่อง (Local Development)
- Docker และ Docker Compose
- Python 3.11+
- Google Cloud SDK (`gcloud`)
- Service account key file ที่มีสิทธิ์เข้าถึง BigQuery

### สำหรับการ Deploy บน GKE
- GKE cluster ที่เปิดใช้งาน Workload Identity
- `kubectl` ที่ตั้งค่าเพื่อเข้าถึง cluster ของคุณ
- Google Cloud SDK (`gcloud`)
- สิทธิ์ IAM ที่เหมาะสม

## เริ่มต้นอย่างรวดเร็ว - การพัฒนาในเครื่อง

### 1. Clone Repository

```bash
git clone <repository-url>
cd dbt-airflow-2.9.1-python-3.11
```

### 2. ตั้งค่า Environment Variables

```bash
cp .env.example .env
# แก้ไขไฟล์ .env ด้วยการตั้งค่าของคุณ
```

อัปเดตค่าต่อไปนี้ในไฟล์ `.env`:
- `GCP_PROJECT_ID`: GCP project ID ของคุณ
- `GCP_LOCATION`: ตำแหน่ง BigQuery (เช่น US, EU)
- `DBT_DATASET`: BigQuery dataset ปลายทาง

### 3. เพิ่ม GCP Service Account Key

วาง service account key file ไว้ที่ `config/gcp-key.json`

```bash
# ดาวน์โหลด service account key
gcloud iam service-accounts keys create config/gcp-key.json \
  --iam-account=YOUR-SERVICE-ACCOUNT@YOUR-PROJECT.iam.gserviceaccount.com
```

### 4. อัปเดตการตั้งค่า dbt Source

แก้ไขไฟล์ `dbt_project/models/staging/sources.yml` เพื่อชี้ไปยังตารางต้นทางจริงใน BigQuery ของคุณ

### 5. เริ่ม Airflow ด้วย Docker Compose

```bash
# เริ่มต้นฐานข้อมูล Airflow
docker-compose up airflow-init

# เริ่ม Airflow services
docker-compose up -d
```

### 6. เข้าถึง Airflow UI

- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 7. ตั้งค่า Airflow Connections และ Variables

```bash
# รันสคริปต์ตั้งค่าใน container ที่กำลังทำงาน
docker-compose exec airflow-webserver bash /opt/airflow/config/setup-airflow-config.sh
```

หรือตั้งค่าด้วยตนเองผ่าน Airflow UI:
- ไปที่ Admin → Variables
- เพิ่ม variables จาก `config/airflow-connections.json`
- ไปที่ Admin → Connections
- เพิ่ม BigQuery connection

### 8. เปิดใช้งานและรัน DAGs

1. ใน Airflow UI ให้เปิดใช้งาน DAG `dbt_bigquery_pipeline` หรือ `dbt_simple_pipeline`
2. Trigger DAG ด้วยตนเองหรือรอให้รันตามกำหนดเวลา

## การ Deploy บน GKE

### 1. ตั้งค่า Workload Identity

```bash
# ตั้งค่า environment variables
export GCP_PROJECT_ID="your-gcp-project-id"
export GKE_CLUSTER_NAME="your-cluster-name"
export GKE_CLUSTER_ZONE="us-central1-a"

# รันสคริปต์ตั้งค่า
./config/setup-workload-identity.sh
```

สคริปต์นี้จะ:
- สร้าง GCP service account
- ให้สิทธิ์เข้าถึง BigQuery และ Cloud Storage
- เปิดใช้งาน Workload Identity บน GKE cluster
- เชื่อมโยง Kubernetes service account กับ GCP service account

### 2. Build และ Push Docker Image

```bash
# Build Docker image
docker build -t gcr.io/${GCP_PROJECT_ID}/airflow-dbt:latest .

# Push ไปยัง Google Container Registry
docker push gcr.io/${GCP_PROJECT_ID}/airflow-dbt:latest
```

### 3. อัปเดต Kubernetes Manifests

แก้ไขไฟล์ `config/airflow-gke-deployment.yaml`:
- แทนที่ `YOUR-PROJECT-ID` ด้วย GCP project ID จริงของคุณ
- อัปเดต image reference ให้ตรงกับ image ที่ push ไปแล้ว
- สร้าง Fernet key และอัปเดตใน `airflow-secrets` Secret

สร้าง Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# แปลงผลลัพธ์เป็น base64
echo -n "your-fernet-key" | base64
```

### 4. Deploy ไปยัง GKE

```bash
# Apply Kubernetes manifests
kubectl apply -f config/airflow-gke-deployment.yaml

# ตรวจสอบสถานะการ deploy
kubectl get pods -n airflow
kubectl get services -n airflow
```

### 5. เข้าถึง Airflow UI

```bash
# รับ external IP
kubectl get service airflow-webserver-service -n airflow

# เข้าถึง Airflow ที่ http://<EXTERNAL-IP>
```

## ภาพรวม DAG

### DAG หลัก (`dbt_dag.py`)

DAG แบบละเอียดที่มี task แยกสำหรับแต่ละคำสั่ง dbt:

1. **setup_gcp_credentials**: ตั้งค่า GCP credentials จาก Airflow connection
2. **dbt_debug**: ตรวจสอบการตั้งค่า dbt และการเชื่อมต่อ BigQuery
3. **dbt_deps**: ติดตั้ง dbt package dependencies
4. **dbt_seed**: โหลดข้อมูล seed จากไฟล์ CSV
5. **dbt_run_staging**: Build staging models
6. **dbt_test_staging**: ทดสอบ staging models
7. **dbt_run_marts**: Build marts models
8. **dbt_test_marts**: ทดสอบ marts models
9. **dbt_docs_generate**: สร้างเอกสาร dbt

### DAG แบบง่าย (`dbt_simple_dag.py`)

DAG แบบง่ายที่รันคำสั่ง dbt ทั้งหมดใน task เดียว เหมาะสำหรับ:
- โปรเจคขนาดเล็ก
- Data pipeline แบบง่าย
- การเริ่มต้นใช้งานอย่างรวดเร็ว

## dbt Models

### Staging Models

อยู่ใน `dbt_project/models/staging/`:
- `stg_orders.sql`: Staging model สำหรับข้อมูลคำสั่งซื้อ
- `stg_customers.sql`: Staging model สำหรับข้อมูลลูกค้า

### Marts Models

อยู่ใน `dbt_project/models/marts/`:
- `customer_orders.sql`: ข้อมูลคำสั่งซื้อของลูกค้าแบบรวม

### การปรับแต่ง Models

1. อัปเดตการตั้งค่า source ใน `dbt_project/models/staging/sources.yml`
2. แก้ไข models ที่มีอยู่หรือสร้าง models ใหม่
3. อัปเดต schema tests ในไฟล์ `schema.yml` ที่เกี่ยวข้อง
4. ทดสอบในเครื่อง: `cd dbt_project && dbt run --target dev`

## การตั้งค่า

### Airflow Variables

ตั้งค่าใน Airflow UI (Admin → Variables):
- `gcp_project_id`: GCP project ID ของคุณ
- `dbt_dataset`: BigQuery dataset ปลายทาง (ค่าเริ่มต้น: `analytics`)
- `gcp_location`: ตำแหน่ง BigQuery (ค่าเริ่มต้น: `asia-southeast1`)

### Airflow Connections

สร้าง connection ด้วย ID `bigquery_default`:
- **Connection Type**: Google Cloud Platform
- **Project ID**: GCP project ID ของคุณ
- **Keyfile Path**: path ไปยัง service account key (สำหรับ local dev)
- หรือตั้งค่า Workload Identity (สำหรับ GKE)

### dbt Profiles

ไฟล์ `dbt_project/profiles.yml` ถูกตั้งค่าให้ใช้ environment variables:
- `GCP_PROJECT_ID`: GCP project ID
- `DBT_DATASET`: BigQuery dataset ปลายทาง
- `GCP_LOCATION`: ตำแหน่ง BigQuery
- `GOOGLE_APPLICATION_CREDENTIALS`: path ไปยัง service account key (ไม่บังคับ)

## การตั้งค่า Workload Identity

เมื่อรันบน GKE ด้วย Workload Identity:

1. Kubernetes service account `airflow-sa` ถูกเชื่อมโยงกับ GCP service account
2. ไม่จำเป็นต้องใช้ service account keys
3. จัดการสิทธิ์ผ่าน IAM roles
4. dbt profile สามารถใช้ `impersonate_service_account` แทน `keyfile`

ข้อดี:
- ไม่ต้องจัดการไฟล์ credentials
- หมุนเวียน credentials อัตโนมัติ
- ความปลอดภัยดีขึ้น
- กระบวนการ deploy ง่ายขึ้น

## การติดตามและแก้ไขปัญหา

### ดู Airflow Logs

**การพัฒนาในเครื่อง:**
```bash
# ดู scheduler logs
docker-compose logs airflow-scheduler

# ดู webserver logs
docker-compose logs airflow-webserver
```

**GKE:**
```bash
# ดู scheduler logs
kubectl logs -n airflow -l component=scheduler

# ดู webserver logs
kubectl logs -n airflow -l component=webserver
```

### ดู dbt Logs

Airflow task logs มี dbt output เข้าถึงได้ผ่าน:
- Airflow UI → DAGs → Task Instance → Logs
- หรือผ่าน CLI: `kubectl logs -n airflow <pod-name>`

### ปัญหาที่พบบ่อย

**ปัญหา: dbt เชื่อมต่อ BigQuery ไม่ได้**
- ตรวจสอบว่า service account มีสิทธิ์ที่จำเป็น
- ตรวจสอบว่า credentials ถูกตั้งค่าอย่างถูกต้อง
- รัน `dbt debug` เพื่อวินิจฉัยปัญหาการเชื่อมต่อ

**ปัญหา: DAG import errors**
- ตรวจสอบว่า Python dependencies ถูกติดตั้งแล้ว
- ตรวจสอบ syntax ของไฟล์ DAG
- ตรวจสอบ Airflow scheduler logs

**ปัญหา: Workload Identity ใช้งานไม่ได้**
- ตรวจสอบว่า GKE cluster เปิดใช้งาน Workload Identity แล้ว
- ตรวจสอบ service account annotations
- ตรวจสอบว่า IAM bindings ถูกต้อง

## การพัฒนา

### รัน dbt ในเครื่อง

```bash
cd dbt_project

# ติดตั้ง dependencies
dbt deps

# ทดสอบการเชื่อมต่อ
dbt debug

# รัน models
dbt run --target dev

# รัน tests
dbt test --target dev
```

### ทดสอบ DAGs

```bash
# ทดสอบโครงสร้าง DAG
docker-compose exec airflow-webserver airflow dags test dbt_bigquery_pipeline

# ทดสอบ task เฉพาะ
docker-compose exec airflow-webserver airflow tasks test dbt_bigquery_pipeline dbt_run_staging 2024-01-01
```

## แนวทางปฏิบัติด้านความปลอดภัย

1. **ห้าม commit credentials**: ตรวจสอบว่าไฟล์ `.env` และ `*.json` อยู่ใน `.gitignore`
2. **ใช้ Workload Identity**: แนะนำให้ใช้ Workload Identity แทน service account keys บน GKE
3. **หลักการสิทธิ์น้อยที่สุด**: ให้เฉพาะสิทธิ์ BigQuery ที่จำเป็นเท่านั้น
4. **หมุนเวียน credentials**: หมุนเวียน service account keys เป็นประจำหากใช้
5. **ใช้ Secrets**: เก็บข้อมูลที่ละเอียดอ่อนใน Kubernetes Secrets หรือ Secret Manager

## คำสั่ง Make ที่มีประโยชน์

```bash
# ดูคำสั่งที่มีทั้งหมด
make help

# เริ่มต้นโปรเจค
make init

# Build Docker images
make build

# เริ่ม Airflow
make up

# หยุด Airflow
make down

# ดู logs
make logs

# ทดสอบ dbt
make test-dbt

# ตั้งค่า Workload Identity
make setup-wi

# Build และ push image ไปยัง GCR
make push-image GCP_PROJECT_ID=your-project-id

# Deploy ไปยัง GKE
make deploy-gke
```

## CI/CD Pipeline

โปรเจคมี GitHub Actions workflow (`.github/workflows/ci.yml`) ที่:
- ทดสอบ dbt models (compile)
- ตรวจสอบคุณภาพโค้ดด้วย flake8 และ black
- Build Docker image
- (ไม่บังคับ) Deploy ไปยัง GKE อัตโนมัติ

### การเปิดใช้งาน Auto-deployment

1. เพิ่ม secrets ใน GitHub repository:
   - `GCP_PROJECT_ID`: GCP project ID
   - `GCP_SA_KEY`: Service account key JSON
   - `GKE_CLUSTER_NAME`: ชื่อ GKE cluster
   - `GKE_ZONE`: GKE cluster zone

2. Uncomment ส่วน `deploy-gke` job ใน `.github/workflows/ci.yml`

## การแก้ไขปัญหาที่พบบ่อย

### 1. Port 8080 ถูกใช้งานอยู่

```bash
# หยุดบริการที่ใช้ port 8080 หรือเปลี่ยน port ใน docker-compose.yml
# แก้ไข:
# ports:
#   - "8081:8080"  # ใช้ port 8081 แทน
```

### 2. Permission denied เมื่อรัน Docker

```bash
# เพิ่ม user เข้า docker group
sudo usermod -aG docker $USER
# Logout และ login ใหม่
```

### 3. dbt models ไม่พบ source tables

- ตรวจสอบว่า source tables มีอยู่จริงใน BigQuery
- อัปเดต `dbt_project/models/staging/sources.yml` ให้ตรงกับชื่อ dataset และ table จริง
- ตรวจสอบสิทธิ์การเข้าถึงตาราง

### 4. Airflow scheduler ไม่ detect DAGs ใหม่

```bash
# Restart scheduler
docker-compose restart airflow-scheduler

# หรือตรวจสอบ DAG errors ใน Airflow UI
```

## คำถามที่พบบ่อย (FAQ)

**Q: จะเปลี่ยน schedule ของ DAG ได้อย่างไร?**
A: แก้ไข `schedule_interval` ในไฟล์ DAG (เช่น `"0 6 * * *"` สำหรับรันทุกวันเวลา 6 โมงเช้า)

**Q: จะเพิ่ม dbt models ใหม่ได้อย่างไร?**
A: สร้างไฟล์ `.sql` ใหม่ใน `dbt_project/models/` และอัปเดต `schema.yml` ตามต้องการ

**Q: จะใช้ dataset อื่นใน BigQuery ได้ไหม?**
A: เปลี่ยนค่า `DBT_DATASET` environment variable หรือ Airflow variable `dbt_dataset`

**Q: จะ backup Airflow metadata ได้อย่างไร?**
A: Backup PostgreSQL database โดยใช้ `pg_dump` หรือ snapshot persistent volume

**Q: จะเพิ่ม dbt packages ได้อย่างไร?**
A: เพิ่มใน `dbt_project/packages.yml` แล้วรัน `dbt deps`

## การสนับสนุนและการติดต่อ

สำหรับปัญหาและคำถาม:
- เปิด issue ใน repository
- ติดต่อทีม data engineering

## ทรัพยากรเพิ่มเติม

- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/2.9.1/)
- [dbt Documentation](https://docs.getdbt.com/)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [GKE Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
- [dbt BigQuery Adapter](https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup)

## License

[Your License Here]

---

สร้างด้วย ❤️ โดยทีม Data Engineering
