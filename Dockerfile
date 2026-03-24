FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies from requirements.txt (single source of truth)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set environment variables (connection string is injected at runtime via docker-compose)
ENV AIRFLOW_HOME=/opt/airflow

# Create a startup script
USER root
RUN mkdir -p /opt/airflow/scripts
COPY --chown=airflow:airflow scripts/entrypoint.sh /opt/airflow/scripts/entrypoint.sh
RUN chmod +x /opt/airflow/scripts/entrypoint.sh

USER airflow
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/scripts/entrypoint.sh"]
