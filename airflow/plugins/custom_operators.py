"""
Sales Demo Pipeline - Custom Airflow Operators
"""

from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Executes data quality checks against PostgreSQL tables.

    :param postgres_conn_id: Airflow connection ID for PostgreSQL
    :param database: Database name to connect to
    :param table: Table to check
    :param checks: List of (check_name, check_sql) tuples to perform
    """

    ui_color = '#89DA59'

    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        database: str | None = None,
        table: str | None = None,
        checks: list[tuple[str, str]] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.table = table
        self.checks = checks or []

    def execute(self, context: dict[str, Any]) -> None:
        """Execute data quality checks."""
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        logger.info(f"Running data quality checks on {self.database}.{self.table}")
        
        for check_name, check_sql in self.checks:
            try:
                result = postgres_hook.get_first(check_sql)
                
                if result and result[0] > 0:
                    logger.error(f"Check '{check_name}' failed: {result[0]} issues found")
                    raise ValueError(f"Data quality check failed: {check_name}")
                
                logger.info(f"Check '{check_name}' passed")
                
            except Exception as e:
                logger.error(f"Error executing check '{check_name}': {str(e)}")
                raise


class DataQualitySummaryOperator(BaseOperator):
    """
    Generates a summary of data quality metrics.

    :param postgres_conn_id: Airflow connection ID for PostgreSQL
    :param database: Database name
    :param schema: Schema name
    :param table: Table to summarize
    """

    ui_color = '#F0EFF0'

    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.schema = schema
        self.table = table

    def execute(self, context: dict[str, Any]) -> None:
        """Generate data quality summary."""
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Count records by quality flag
        summary_sql = f"""
            SELECT 
                CASE WHEN data_quality_flag IS NOT NULL THEN data_quality_flag ELSE 'UNKNOWN' END as quality_flag,
                COUNT(*) as record_count,
                ROUND(COUNT(*)::NUMERIC / 
                    (SELECT COUNT(*) FROM {self.schema}.{self.table})::NUMERIC * 100, 2) as percentage
            FROM {self.schema}.{self.table}
            GROUP BY CASE WHEN data_quality_flag IS NOT NULL THEN data_quality_flag ELSE 'UNKNOWN' END
            ORDER BY quality_flag
        """
        
        try:
            results = postgres_hook.get_records(summary_sql)
            
            logger.info("=" * 50)
            logger.info(f"Data Quality Summary: {self.schema}.{self.table}")
            logger.info("=" * 50)
            
            for flag, count, percentage in results:
                logger.info(f"{flag:10s} | Count: {count:6d} | Percentage: {percentage:6.2f}%")
            
            logger.info("=" * 50)
            
            # Push to XCom for downstream use
            context['ti'].xcom_push(key='dq_summary', value={
                'results': results,
                'timestamp': context['execution_date']
            })
            
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            raise
