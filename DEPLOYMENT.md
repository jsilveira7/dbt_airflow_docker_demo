# Deployment & Operations Guide

Complete step-by-step instructions for deploying, configuring, and running the data pipeline.

## Prerequisites

- Docker (v20.10+)
- Docker Compose (v2.0+)
- Git
- 4GB RAM (minimum recommended)
- PostgreSQL client tools (optional, for direct database queries)

## Configuration Variables

### Environment File (.env)

Create or edit `.env` file in the project root (see `.env.example` for a template):

```bash
# Database Configuration
POSTGRES_USER=airflow                          # PostgreSQL user (default: airflow)
POSTGRES_PASSWORD=airflow                      # PostgreSQL password (default: airflow)
POSTGRES_DB=airflow                            # Main database name (default: airflow)
POSTGRES_PORT=5432                             # PostgreSQL port (default: 5432)

# Airflow Configuration
AIRFLOW_SECRET_KEY=Nx8_A9Lr3gH8xK0dFvWpQjZm4nB7cE2rT5sU6vX9yZ  # Airflow secret key

# Optional Settings
# DBT_THREADS=4                                 # Number of dbt threads
# AIRFLOW__CORE__PARALLELISM=32                # Number of parallel tasks
```

### Variable Locations & Usage

| Variable | File | Used By | Impact |
|----------|------|---------|--------|
| `POSTGRES_USER` | `.env` | Docker, Airflow | Database authentication |
| `POSTGRES_PASSWORD` | `.env` | Docker, Airflow | Database authentication |
| `POSTGRES_DB` | `.env` | Docker, PostgreSQL | Initial database name |
| `POSTGRES_PORT` | `.env` | Docker, Airflow | Database connection port |
| `AIRFLOW_SECRET_KEY` | `.env` | Airflow | Security & encryption |

## Deployment Steps

### Step 1: Prepare Environment

```bash
# Navigate to project directory
cd ebury_case_study

# Verify Docker is running
docker --version
docker-compose --version

# Verify .env file exists with correct values
cat .env
```

### Step 2: Build Docker Image

```bash
# Build the custom Airflow image with dbt and dependencies
docker-compose build
```

**What happens:**
- Builds custom Airflow image from `Dockerfile`
- Installs dbt-core, dbt-postgres, pandas, psycopg2
- Tags as `ebury-airflow:latest`

### Step 3: Start Services

```bash
# Start all services (PostgreSQL, Airflow webserver, Airflow scheduler)
docker-compose up -d

# Optional: Watch startup logs
docker-compose logs -f
```

**Services starting:**
1. **PostgreSQL** - Data storage (listens on port 5432)
2. **Airflow Webserver** - UI & API (available at http://localhost:8080)
3. **Airflow Scheduler** - Task orchestration (runs in background)

### Step 4: Verify Installation

```bash
# Check container status
docker-compose ps

# Should show all 3 containers as 'healthy' or 'running'
```

Expected output:
```
NAME                        STATUS
ebury_postgres              Up (healthy)
ebury_airflow_webserver     Up (running)
ebury_airflow_scheduler     Up (running)
```

## Triggering the Pipeline

### Option 1: Using Airflow UI (Easiest)

1. **Open Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

2. **Find the DAG**
   - Look for: `ebury_data_pipeline`
   - Appears in the DAG list within 30-60 seconds

3. **Trigger Execution**
   - Click the DAG name
   - Click the ▶️ (play) button in top-right
   - Select "Trigger DAG"
   - Observe execution in real-time

4. **Monitor Progress**
   - Click on the DAG run
   - View task status (pending → running → success)
   - Check logs for each task

### Option 2: Using Command Line

```bash
# Trigger DAG from command line
docker-compose exec airflow-webserver \
  airflow dags trigger ebury_data_pipeline

# Monitor scheduler logs
docker-compose logs -f airflow-scheduler
```

## Expected Pipeline Execution

### Timeline
```
Ingestion        (2-3 min)  → Load CSV to PostgreSQL
Transformation   (5-8 min)  → Run dbt models
Validation       (2-3 min)  → Quality checks
Summary          (<1 min)   → Generate report
─────────────────────────────────────────────
Total Runtime    ~10-15 min
```

### Pipeline Stages

**Stage 1: Data Ingestion**
- Loads CSV to PostgreSQL raw schema
- Task: `load_csv_to_postgres`
- Database: `ebury_raw.staging.transactions`

**Stage 2: Data Transformation**
- Cleans and validates data with dbt
- Task: `ebury_data_pipeline_transform`
- Models created:
  - `ebury_analytics.staging.stg_transactions` - Cleaned data
  - `ebury_analytics.marts.dim_customer` - Customer dimension
  - `ebury_analytics.marts.fact_transactions` - Transaction facts

**Stage 3: Data Quality Validation**
- Runs dbt tests
- Task: `validate_data_quality`
- Checks: Uniqueness, nullability, business rules

**Stage 4: Summary Generation**
- Creates execution summary
- Task: `generate_summary`
- Logs data quality metrics

## Monitoring & Debugging

### View Logs

```bash
# Airflow scheduler logs
docker-compose logs airflow-scheduler

# Airflow webserver logs
docker-compose logs airflow-webserver

# PostgreSQL logs
docker-compose logs postgres

# Follow logs in real-time
docker-compose logs -f
```

### Connect to Database

```bash
# Open PostgreSQL prompt
docker-compose exec postgres \
  psql -U airflow -d ebury_analytics

# Useful queries
\dt ebury_analytics.staging.*;           # List staging tables
\dt ebury_analytics.marts.*;             # List mart tables
SELECT COUNT(*) FROM ebury_analytics.marts.fact_transactions;
```

### Check Pipeline Status

```bash
# List all DAG runs
docker-compose exec airflow-webserver \
  airflow dags list

# Check specific DAG
docker-compose exec airflow-webserver \
  airflow dags list-runs --dag-id ebury_data_pipeline
```

## Customization

### Changing Database Credentials

1. Edit `.env`:
   ```bash
   POSTGRES_USER=my_user
   POSTGRES_PASSWORD=my_password
   ```

2. Restart services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Changing Pipeline Schedule

Edit `airflow/dags/ebury_data_pipeline_dag.py`:

```python
# Change schedule_interval (line with schedule)
# Options: 
#   '@daily' - every day at midnight
#   '@hourly' - every hour
#   '0 2 * * *' - cron format (2 AM daily)
#   None - manual trigger only
```

### Loading Different CSV File

1. Place CSV in project root
2. Edit `airflow/dags/ebury_data_pipeline_dag.py`
3. Change `csv_file_path` variable
4. Retrigger DAG

### Modifying dbt Models

1. Edit SQL files in `dbt/models/`
2. Run: `docker-compose exec airflow-webserver dbt run`
3. Retrigger Airflow DAG

## Stopping & Cleanup

### Stop Services (Keep Data)
```bash
docker-compose down
# Data persists in postgres_data volume
```

### Full Cleanup (Remove Data)
```bash
docker-compose down -v
# WARNING: This deletes all data!
```

### Remove Containers & Images
```bash
docker-compose down --rmi all -v
# Also removes the built Airflow image
```

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose logs

# Common issues:
# - Port 5432 in use: Change POSTGRES_PORT in .env
# - Port 8080 in use: Change port in docker-compose.yml
# - Insufficient memory: Free up RAM
```

### DAG Not Appearing

```bash
# Check DAG syntax
docker-compose exec airflow-webserver \
  python -m py_compile airflow/dags/ebury_data_pipeline_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler

# Wait 30-60 seconds for DAG to appear
```

### Database Connection Failed

```bash
# Verify PostgreSQL is healthy
docker-compose ps

# Check .env credentials match
cat .env

# Test connection
docker-compose exec postgres \
  psql -U airflow -d airflow -c "SELECT version();"
```

### Out of Memory

```bash
# Check current usage
docker stats

# If needed, increase Docker Desktop memory limit
```

## ✅ Quick Reference

| Task | Command |
|------|---------|
| Start pipeline | `docker-compose up -d` |
| Stop pipeline | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Connect to DB | `docker-compose exec postgres psql -U airflow -d ebury_analytics` |
| Rebuild image | `docker-compose build` |
| Full reset | `docker-compose down -v && docker-compose up -d` |

---

For more information, see [README.md](./README.md)
