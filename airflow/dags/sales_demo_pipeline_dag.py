"""
Sales Demo Pipeline DAG
Orchestrates data ingestion, transformation, and quality checks.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import logging
import os

# Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Initialize logger
logger = logging.getLogger(__name__)


def load_csv_to_postgres(**context):
    """Load CSV file into PostgreSQL raw table using COPY for performance."""
    try:
        csv_path = '/opt/airflow/data/customer_transactions.csv'
        df = pd.read_csv(csv_path)

        logger.info(f"Loaded {len(df)} rows from CSV file")

        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Truncate table before loading to prevent duplicates on retries
        cursor.execute("TRUNCATE TABLE raw.customer_transactions_raw")

        # Use StringIO + COPY for bulk loading instead of row-by-row inserts
        from io import StringIO

        columns = [
            'transaction_id', 'customer_id', 'transaction_date',
            'product_id', 'product_name', 'quantity', 'price', 'tax',
        ]
        buffer = StringIO()
        df[columns].to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        cursor.copy_expert(
            """COPY raw.customer_transactions_raw
               (transaction_id, customer_id, transaction_date,
                product_id, product_name, quantity, price, tax)
               FROM STDIN WITH (FORMAT text, NULL '\\N')""",
            buffer,
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"Successfully loaded {len(df)} rows into raw.customer_transactions_raw")
        context['ti'].xcom_push(key='rows_loaded', value=len(df))

    except Exception as e:
        logger.error(f"Error loading CSV to PostgreSQL: {str(e)}")
        raise


def run_dbt_command(command: str, **context):
    """Execute dbt command."""
    try:
        import subprocess

        cmd_env = os.environ.copy()
        cmd_env.update({
            'POSTGRES_HOST': os.environ.get('POSTGRES_HOST', 'postgres'),
            'POSTGRES_USER': os.environ.get('POSTGRES_USER', 'airflow'),
            'POSTGRES_PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'airflow'),
            'POSTGRES_PORT': os.environ.get('POSTGRES_PORT', '5432'),
        })
        
        full_command = f'cd /opt/dbt && {command}'
        logger.info(f"Executing: {full_command}")
        
        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            env=cmd_env
        )
        
        logger.info(f"DBT Command Output:\n{result.stdout}")
        
        if result.returncode != 0:
            logger.error(f"DBT Command Error:\n{result.stderr}")
            raise Exception(f"DBT command failed with return code {result.returncode}")
        
        return result.stdout
        
    except Exception as e:
        logger.error(f"Error running dbt command: {str(e)}")
        raise


def validate_data_quality(**context):
    """Validate data quality after transformation"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Query to check data quality flags
        quality_check_sql = """
            SELECT 
                data_quality_flag,
                COUNT(*) as record_count
            FROM marts.fact_transactions
            GROUP BY data_quality_flag
            ORDER BY data_quality_flag
        """
        
        result = postgres_hook.get_records(quality_check_sql)
        
        logger.info("Data Quality Summary:")
        for row in result:
            logger.info(f"  {row[0]}: {row[1]} records")
        
        # Check for HIGH quality issues
        high_quality_check = """
            SELECT COUNT(*) FROM marts.fact_transactions
            WHERE data_quality_flag = 'HIGH'
        """
        high_issue_count = postgres_hook.get_first(high_quality_check)[0]
        
        if high_issue_count > 0:
            logger.warning(f"Found {high_issue_count} records with HIGH quality flags")
        
        context['ti'].xcom_push(key='data_quality_check', value={
            'status': 'completed',
            'high_issues': high_issue_count
        })
        
    except Exception as e:
        logger.error(f"Error validating data quality: {str(e)}")
        raise


def generate_summary(**context):
    """Generate pipeline summary"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        summary_sql = """
            SELECT
                COUNT(DISTINCT customer_id) as total_customers,
                COUNT(DISTINCT product_id) as total_products,
                COUNT(*) as total_transactions,
                ROUND(SUM(total_amount), 2) as total_revenue,
                MIN(transaction_date) as earliest_transaction,
                MAX(transaction_date) as latest_transaction
            FROM marts.fact_transactions
        """
        
        result = postgres_hook.get_first(summary_sql)
        
        summary = {
            'total_customers': result[0],
            'total_products': result[1],
            'total_transactions': result[2],
            'total_revenue': result[3],
            'earliest_transaction': str(result[4]),
            'latest_transaction': str(result[5])
        }
        
        logger.info(f"Pipeline Summary: {summary}")
        context['ti'].xcom_push(key='pipeline_summary', value=summary)
        
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
        raise


# Define DAG
dag = DAG(
    'sales_demo_pipeline',
    default_args=DEFAULT_ARGS,
    description='Data pipeline for customer transactions: ingest, transform, and aggregate',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales_demo', 'production'],
)

# Create tasks
with dag:
    
    # Data Ingestion Tasks
    with TaskGroup('data_ingestion', tooltip='Data ingestion tasks') as data_ingestion:
        
        create_raw_table = PostgresOperator(
            task_id='create_raw_table',
            postgres_conn_id='postgres_default',
            sql="""
                CREATE TABLE IF NOT EXISTS raw.customer_transactions_raw (
                    transaction_id VARCHAR(50),
                    customer_id VARCHAR(50),
                    transaction_date VARCHAR(50),
                    product_id VARCHAR(50),
                    product_name VARCHAR(255),
                    quantity VARCHAR(50),
                    price VARCHAR(50),
                    tax VARCHAR(50),
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        )
        
        load_data = PythonOperator(
            task_id='load_csv_to_postgres',
            python_callable=load_csv_to_postgres,
        )
        
        create_raw_table >> load_data
    
    # Data Transformation Tasks
    with TaskGroup('data_transformation', tooltip='dbt transformation tasks') as data_transformation:
        
        dbt_run = BashOperator(
            task_id='dbt_run',
            bash_command='cd /opt/dbt && DBT_PROFILES_DIR=/opt/dbt dbt run --target dev --full-refresh',
        )
        
        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command='cd /opt/dbt && DBT_PROFILES_DIR=/opt/dbt dbt test --target dev',
        )
        
        dbt_run >> dbt_test
    
    # Data Validation Tasks
    with TaskGroup('data_validation', tooltip='Data quality validation tasks') as data_validation:
        
        validate_quality = PythonOperator(
            task_id='validate_data_quality',
            python_callable=validate_data_quality,
        )
        
        summary = PythonOperator(
            task_id='generate_summary',
            python_callable=generate_summary,
        )
        
        validate_quality >> summary
    
    # Define dependencies
    data_ingestion >> data_transformation >> data_validation
