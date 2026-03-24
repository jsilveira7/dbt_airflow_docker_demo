#!/bin/bash
set -e

# Set default values if not provided
export POSTGRES_HOST=${POSTGRES_HOST:-postgres}
export POSTGRES_USER=${POSTGRES_USER:-airflow}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-airflow}
export POSTGRES_DB=${POSTGRES_DB:-airflow}
export AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready at $POSTGRES_HOST..."
for i in {1..30}; do
  if PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1" > /dev/null 2>&1; then
    echo "✅ PostgreSQL is ready!"
    break
  fi
  echo "PostgreSQL not ready, retrying... ($i/30)"
  sleep 1
done

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db migrate

# Create admin user
echo "Creating admin user..."
airflow users create \
  --username airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password airflow \
  2>/dev/null || echo "User already exists"

# Create PostgreSQL connection for DAGs
echo "Creating PostgreSQL connection..."
airflow connections delete postgres_default 2>/dev/null || true
airflow connections add postgres_default \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST:-postgres}" \
  --conn-login "${POSTGRES_USER:-airflow}" \
  --conn-password "${POSTGRES_PASSWORD:-airflow}" \
  --conn-port 5432 \
  --conn-schema "ebury_analytics"

echo "✅ Airflow initialization complete"

# Start Airflow service based on command passed
exec "$@"
