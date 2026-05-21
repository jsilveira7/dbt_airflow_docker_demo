# Sales Demo — Data Pipeline

An end-to-end data pipeline for ingesting, cleaning, and modelling customer transaction data using **Apache Airflow**, **dbt**, and **PostgreSQL**, fully containerised with Docker.

## Overview

| Component | Version | Role |
|-----------|---------|------|
| Apache Airflow | 2.7.3 | Workflow orchestration |
| dbt-core | 1.7.2 | SQL-based transformation layer |
| PostgreSQL | 15 | Data storage |
| Docker Compose | 3.8 | Local development environment |

## Architecture

```
                  ┌──────────────────────────────────────────────┐
                  │               Docker Compose                 │
                  │                                              │
  CSV File ──────▶│  Airflow ──▶ PostgreSQL (raw schema)        │
                  │     │                                        │
                  │     ▼                                        │
                  │   dbt ──────▶ staging schema (clean + audit) │
                  │     │                                        │
                  │     ▼                                        │
                  │   dbt ──────▶ marts schema (facts + dims)    │
                  │     │                                        │
                  │     ▼                                        │
                  │   dbt test ─▶ Data quality validation        │
                  └──────────────────────────────────────────────┘
```

### Data Layers

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Raw** | `raw` | Unmodified CSV data, all columns stored as VARCHAR |
| **Staging** | `staging` | Cleaned, typed, and validated data; rejected rows tracked in `audit_transactions` |
| **Marts** | `marts` | Business-ready dimension and fact tables |

## Project Structure

```
airflow/
├── dags/
│   └── sales_demo_pipeline_dag.py   # Orchestration DAG
└── plugins/
    └── custom_operators.py           # Reusable data-quality operators

data/
└── customer_transactions.csv         # Source dataset

dbt/
├── models/
│   ├── staging/                      # Cleaning & validation
│   │   ├── _stg_transactions_base.sql  (ephemeral — shared logic)
│   │   ├── stg_transactions.sql        (clean rows)
│   │   └── audit_transactions.sql      (rejected rows)
│   └── marts/                        # Business analytics
│       ├── fact_transactions.sql
│       ├── dim_customer.sql
│       └── dim_product.sql
├── tests/                            # Custom singular tests
├── macros/utils.sql                  # Schema naming helpers
├── dbt_project.yml
└── profiles.yml

postgres/
├── init_scripts/
│   └── 01_init_databases.sql         # Schema & table bootstrap
└── validation_queries.sql            # Ad-hoc validation queries

scripts/
└── entrypoint.sh                     # Airflow container bootstrap

docker-compose.yml
Dockerfile
Makefile
requirements.txt
```

## Quick Start

### Prerequisites

- Docker & Docker Compose v2.0+
- Git
- 4 GB RAM minimum

### 1. Configure Environment

```bash
cp .env.example .env        # create local env file
# edit .env if you need custom credentials
```

### 2. Build & Start

```bash
docker-compose up -d --build
```

### 3. Run the Pipeline

Open the Airflow UI at **http://localhost:8080** (user: `airflow` / pass: `airflow`), find the `sales_demo_pipeline` DAG, and trigger it.

Alternatively:

```bash
docker-compose exec airflow-webserver airflow dags trigger sales_demo_pipeline
```

### 4. Verify Results

```bash
docker-compose exec postgres psql -U airflow -d sales_demo_analytics \
  -c "SELECT COUNT(*) FROM marts.fact_transactions;"
```

## Data Quality Approach

The source CSV contains **intentional data quality issues** including:

| Issue | Example | Handling |
|-------|---------|----------|
| Non-numeric IDs | `T1010` | Parsed; rows with unparseable IDs are dropped |
| Mixed date formats | `18-07-2023` vs `2023-07-18` | Regex-matched and normalised |
| Text in numeric fields | `"Two Hundred"` | Flagged, set to NULL, row moved to audit |
| Missing customer IDs | empty | Row excluded from staging |
| Float customer IDs | `501.0` | Truncated to integer |
| Invalid product IDs | `P100` | Enriched from `product_name` lookup |

### Data Enrichment Rules

The staging layer applies intelligent enrichment for missing product information:

| Scenario | Rule | Mapping |
|----------|------|---------|
| Missing `product_id`, has `product_name` | Fill `product_id` from product name | Product A → 101, Product B → 102, etc. |
| Missing `product_name`, has `product_id` | Fill `product_name` from product ID | 101 → Product A, 102 → Product B, etc. |
| Both present | Use as-is | No change |
| Both missing | Reject row | Row moved to `audit_transactions` |

This ensures that **every valid transaction in the staging layer has both `product_id` and `product_name` populated**, enabling reliable joins with the `dim_product` table downstream.

Clean rows flow into `stg_transactions`; rejected rows are captured in `audit_transactions` with a `rejection_reason` column for traceability.

## Stopping

```bash
docker-compose down          # preserve data
docker-compose down -v       # remove all data
```

## Further Reading

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed deployment steps, configuration reference, monitoring commands, and troubleshooting.
