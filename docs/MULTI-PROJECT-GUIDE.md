# Multiple dbt Projects Guide

[ภาษาไทย](MULTI-PROJECT-GUIDE.th.md) | **English**

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Use Cases](#use-cases)
4. [Setup](#setup)
5. [DAGs for Multiple Projects](#dags-for-multiple-projects)
6. [Example Projects](#example-projects)
7. [Best Practices](#best-practices)

---

## Overview

This project supports running multiple dbt projects in a single Airflow DAG, which is useful when:

- Separating different data domains (Sales, Marketing, Finance)
- Each team manages their own models
- Requiring isolation between projects
- Each project has different schedules or dependencies

### Benefits

✅ **Separation of Concerns**: Each project manages its own domain
✅ **Team Autonomy**: Different teams can work independently
✅ **Parallel Execution**: Run projects concurrently to save time
✅ **Easier Debugging**: Separate logs and errors for each project
✅ **Flexible Dependencies**: Define dependencies between projects

### Considerations

⚠️ **Complexity**: Managing multiple projects is more complex than a single project
⚠️ **Resources**: Uses more resources when running in parallel
⚠️ **Coordination**: Requires coordination when there are cross-project dependencies

---

## Project Structure

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

## Use Cases

### Use Case 1: Domain-Driven Design

Separate projects by business domains:

```
sales_analytics/     → Sales data, transactions, products
marketing_analytics/ → Campaigns, leads, conversions
finance_analytics/   → Financial data, invoices, payments
```

**Best for:**
- Medium to large organizations
- Each department has its own data team
- Data comes from different sources

### Use Case 2: Environment Separation

Separate projects by environments:

```
production_analytics/    → Production data models
staging_analytics/       → Staging/testing models
development_analytics/   → Development experiments
```

**Best for:**
- Testing models before production
- Experimentation and prototyping
- Multiple versions of models

### Use Case 3: Client/Tenant Separation

Separate projects by clients (multi-tenant):

```
client_a_analytics/      → Client A data
client_b_analytics/      → Client B data
client_c_analytics/      → Client C data
```

**Best for:**
- SaaS companies with multiple clients
- Data isolation requirements
- Client-specific transformations

---

## Setup

### Step 1: Configure Environment Variables

Update `.env` file:

```bash
# GCP Configuration
GCP_PROJECT_ID=your-gcp-project-id
GCP_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/config/gcp-key.json

# dbt Configuration (Multiple Projects)
SALES_DATASET=sales_analytics
MARKETING_DATASET=marketing_analytics
FINANCE_DATASET=finance_analytics
```

### Step 2: Create BigQuery Datasets

Create datasets for each project:

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

### Step 3: Configure Airflow Variables

In Airflow UI (Admin → Variables):

| Key | Value | Description |
|-----|-------|-------------|
| gcp_project_id | your-project-id | GCP Project ID |
| gcp_location | US | BigQuery location |
| sales_dataset | sales_analytics | Sales dataset |
| marketing_dataset | marketing_analytics | Marketing dataset |
| finance_dataset | finance_analytics | Finance dataset |

---

## DAGs for Multiple Projects

### DAG 1: Parallel Execution (Recommended)

**File:** `dags/dbt_multi_project_dag.py`

**Features:**
- Run projects concurrently to save time
- Each project has its own task group
- Best for independent projects

**DAG Structure:**

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

**Usage Example:**

```python
# Projects run in parallel
setup_credentials >> [
    sales_pipeline,
    marketing_pipeline,
    finance_pipeline
]
```

**Runtime:** ~30 minutes (if each project takes 30 minutes)

### DAG 2: Sequential Execution

**File:** `dags/dbt_multi_project_sequential_dag.py`

**Features:**
- Run projects in sequence
- Best for dependent projects
- If first project fails, subsequent projects won't run

**DAG Structure:**

```
[Sales]
   |
   ▼
[Marketing]  (waits for Sales)
   |
   ▼
[Finance]    (waits for Marketing)
```

**Usage Example:**

```python
# Projects run sequentially
sales_pipeline >> marketing_pipeline >> finance_pipeline
```

**Runtime:** ~90 minutes (30 + 30 + 30 minutes)

### DAG 3: Custom Dependencies

Create custom dependencies:

```python
# Sales and Marketing run in parallel
# Finance waits for both
setup_credentials >> [sales_pipeline, marketing_pipeline] >> finance_pipeline
```

---

## Example Projects

### Sales Analytics Project

**Purpose:** Analyze sales data

**Source Tables:**
- `sales_raw.transactions` - Sales transactions
- `sales_raw.products` - Product catalog
- `sales_raw.stores` - Store information

**Models:**
- `stg_transactions.sql` - Staging transactions
- `daily_sales.sql` - Daily sales summary

**Output Datasets:**
- `sales_staging` - Staging models
- `sales_marts` - Marts models

### Marketing Analytics Project

**Purpose:** Analyze marketing campaign performance

**Source Tables:**
- `marketing_raw.campaigns` - Campaign data
- `marketing_raw.leads` - Lead information
- `marketing_raw.conversions` - Conversion events

**Models:**
- `stg_campaigns.sql` - Staging campaigns
- `campaign_performance.sql` - Campaign results

**Output Datasets:**
- `marketing_staging` - Staging models
- `marketing_marts` - Marts models

### Finance Analytics Project

**Purpose:** Analyze financial data

**Source Tables:**
- `finance_raw.invoices` - Invoice records
- `finance_raw.payments` - Payment transactions
- `finance_raw.expenses` - Expense records

**Models:**
- `stg_invoices.sql` - Staging invoices
- `monthly_revenue.sql` - Monthly revenue summary

**Output Datasets:**
- `finance_staging` - Staging models
- `finance_marts` - Marts models

---

## Best Practices

### 1. Project Naming

**Good:**
```
sales_analytics/
marketing_analytics/
finance_analytics/
```

**Bad:**
```
project1/
proj2/
data/
```

### 2. Managing Dependencies

**If projects are independent:**
```python
# Run in parallel
setup >> [sales, marketing, finance]
```

**If there are dependencies:**
```python
# Marketing depends on Sales
setup >> sales >> marketing >> finance
```

**If there are partial dependencies:**
```python
# Finance depends on both Sales and Marketing
setup >> [sales, marketing] >> finance
```

### 3. Resource Management

**Parallel execution:**
- Set BigQuery quota limits
- Monitor memory usage
- Use different time slots if resources are limited

```python
# Example: Run Sales in morning, Marketing afternoon, Finance evening
sales_dag = DAG(schedule_interval="0 6 * * *")    # 6 AM
marketing_dag = DAG(schedule_interval="0 12 * * *") # 12 PM
finance_dag = DAG(schedule_interval="0 18 * * *")  # 6 PM
```

### 4. Shared Models and Macros

**If you have shared models:**

Create a shared project:

```
dbt_projects/
├── shared_utils/              # Shared utilities
│   ├── macros/
│   │   └── common_macros.sql
│   └── models/
│       └── dim_date.sql
│
├── sales_analytics/           # References shared project
├── marketing_analytics/       # References shared project
└── finance_analytics/         # References shared project
```

**In packages.yml:**
```yaml
packages:
  - local: ../shared_utils
```

### 5. Testing Strategies

**Each project should have tests:**

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

### 6. Monitoring and Alerting

**Set up alerts by project:**

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

---

## Troubleshooting

### Issue: Can't find models from other projects

**Cause:** dbt projects are separate, can't use ref() across projects directly

**Solution:**

1. **Use sources instead of refs:**
```sql
-- In marketing_analytics
-- Reference sales_analytics output
SELECT *
FROM {{ source('sales_marts', 'daily_sales') }}
```

2. **Create cross-project dependencies:**
```yaml
# marketing_analytics/models/sources.yml
sources:
  - name: sales_marts
    database: "{{ env_var('GCP_PROJECT_ID') }}"
    schema: sales_marts
    tables:
      - name: daily_sales
```

### Issue: Projects running too slow

**Solution:**

1. **Check if can run in parallel**
2. **Use incremental models:**
```sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}
```

3. **Reduce model scope:**
```sql
-- Use WHERE clause to limit data
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
```

---

## Summary

Managing multiple dbt projects allows you to:

✅ Clearly separate different domains
✅ Different teams work independently
✅ Run in parallel to save time
✅ Easier to maintain and debug

**But requires planning:**
- Project structure
- Dependencies between projects
- Resource allocation
- Monitoring and alerting

**Choose the right pattern:**
- Parallel for independent projects
- Sequential for dependent projects
- Custom dependencies for complex scenarios

---

**Recommendation:** Start with a single project, then split into multiple projects when necessary
