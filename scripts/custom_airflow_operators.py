"""
Custom Airflow Operators for NYC TLC Data Platform

This module provides custom operators for common ETL tasks
in the NYC TLC Data Platform.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import logging
import os


class S3ToPostgresOperator(BaseOperator):
    """
    Custom operator to load data from S3 to PostgreSQL with optional transformation
    """
    
    @apply_defaults
    def __init__(
        self,
        s3_bucket: str,
        s3_key: str,
        postgres_conn_id: str = 'postgres_default',
        table_name: str,
        file_format: str = 'parquet',
        transform_function: Optional[callable] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.file_format = file_format
        self.transform_function = transform_function

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the operator to load data from S3 to PostgreSQL
        """
        # Initialize S3 hook
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        # Download file from S3
        temp_file_path = f"/tmp/{self.s3_key.split('/')[-1]}"
        s3_hook.download_file(
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            local_path=temp_file_path
        )
        
        # Read the file based on format
        if self.file_format == 'parquet':
            table = pq.read_table(temp_file_path)
            df = table.to_pandas()
        elif self.file_format == 'csv':
            df = pd.read_csv(temp_file_path)
        else:
            raise AirflowException(f"Unsupported file format: {self.file_format}")
        
        # Apply transformation if provided
        if self.transform_function:
            df = self.transform_function(df)
        
        # Add metadata columns
        df['loaded_at'] = pd.Timestamp.now()
        df['source_file'] = self.s3_key
        
        # Get PostgreSQL connection details from environment
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        
        # Create SQLAlchemy engine
        connection_string = postgres_hook.get_uri()
        engine = create_engine(connection_string)
        
        # Load to PostgreSQL
        df.to_sql(
            self.table_name,
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        self.log.info(f"Loaded {len(df)} records to {self.table_name}")


class DbtRunOperator(BaseOperator):
    """
    Custom operator to run dbt commands with proper error handling
    """
    
    @apply_defaults
    def __init__(
        self,
        dbt_project_dir: str,
        dbt_command: str = 'run',
        dbt_models: Optional[List[str]] = None,
        dbt_vars: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dbt_project_dir = dbt_project_dir
        self.dbt_command = dbt_command
        self.dbt_models = dbt_models or []
        self.dbt_vars = dbt_vars or {}

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the dbt command
        """
        import subprocess
        import json
        
        try:
            # Build dbt command
            cmd = ['dbt', self.dbt_command, '--project-dir', self.dbt_project_dir]
            
            # Add models if specified
            if self.dbt_models:
                cmd.extend(['--models'] + self.dbt_models)
            
            # Add vars if specified
            if self.dbt_vars:
                cmd.extend(['--vars', json.dumps(self.dbt_vars)])
            
            # Run dbt command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                cwd=self.dbt_project_dir
            )
            
            self.log.info(f"dbt {self.dbt_command} completed successfully")
            self.log.info(f"Output: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"dbt {self.dbt_command} failed: {e}")
            self.log.error(f"Error output: {e.stderr}")
            raise AirflowException(f"dbt {self.dbt_command} failed: {e}")
        except Exception as e:
            self.log.error(f"Error running dbt {self.dbt_command}: {e}")
            raise AirflowException(f"Error running dbt {self.dbt_command}: {e}")


class DataQualityCheckOperator(BaseOperator):
    """
    Custom operator to perform data quality checks
    """
    
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        table_name: str,
        checks: Dict[str, Dict[str, Any]],  # Dictionary of check_name: check_config
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.checks = checks

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute data quality checks
        """
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        results = {
            'table_name': self.table_name,
            'checks_performed': len(self.checks),
            'passed_checks': 0,
            'failed_checks': 0,
            'check_details': []
        }
        
        for check_name, check_config in self.checks.items():
            try:
                check_type = check_config.get('type')
                column = check_config.get('column')
                expected = check_config.get('expected')
                
                if check_type == 'row_count':
                    query = f"SELECT COUNT(*) FROM {self.table_name}"
                    cursor.execute(query)
                    actual = cursor.fetchone()[0]
                    passed = actual >= expected if expected else True
                    
                elif check_type == 'null_check':
                    query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} IS NULL"
                    cursor.execute(query)
                    null_count = cursor.fetchone()[0]
                    total_count = self._get_row_count(cursor, self.table_name)
                    passed = (null_count / total_count) * 100 <= expected if expected else True
                    
                elif check_type == 'range_check':
                    min_val = check_config.get('min_value')
                    max_val = check_config.get('max_value')
                    query = f"SELECT MIN({column}), MAX({column}) FROM {self.table_name}"
                    cursor.execute(query)
                    min_actual, max_actual = cursor.fetchone()
                    passed = (min_actual >= min_val if min_val else True) and (max_actual <= max_val if max_val else True)
                    
                else:
                    raise AirflowException(f"Unsupported check type: {check_type}")
                
                check_result = {
                    'check_name': check_name,
                    'check_type': check_type,
                    'passed': passed,
                    'details': {
                        'column': column,
                        'expected': expected,
                        'actual': None  # Will be filled in for specific check types
                    }
                }
                
                if check_type == 'row_count':
                    check_result['details']['actual'] = actual
                elif check_type == 'null_check':
                    check_result['details']['actual'] = null_count
                elif check_type == 'range_check':
                    check_result['details']['actual'] = {'min': min_actual, 'max': max_actual}
                
                results['check_details'].append(check_result)
                
                if passed:
                    results['passed_checks'] += 1
                    self.log.info(f"Data quality check passed: {check_name}")
                else:
                    results['failed_checks'] += 1
                    self.log.warning(f"Data quality check failed: {check_name}")
                    
            except Exception as e:
                self.log.error(f"Error performing data quality check {check_name}: {e}")
                results['failed_checks'] += 1
                results['check_details'].append({
                    'check_name': check_name,
                    'check_type': check_type,
                    'passed': False,
                    'error': str(e)
                })
        
        # Check if any critical checks failed
        if results['failed_checks'] > 0:
            failed_checks = [check['check_name'] for check in results['check_details'] if not check['passed']]
            self.log.warning(f"Data quality checks failed: {failed_checks}")
        
        return results
    
    def _get_row_count(self, cursor, table_name):
        """Helper method to get row count"""
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]


class SchemaChangeOperator(BaseOperator):
    """
    Custom operator to handle schema changes with zero-downtime approach
    """
    
    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        table_name: str,
        schema_changes: List[Dict[str, Any]],  # List of schema change operations
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema_changes = schema_changes

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute schema changes with zero-downtime approach
        """
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Begin transaction
        cursor.execute("BEGIN")
        
        try:
            for change in self.schema_changes:
                change_type = change['type']
                column_name = change['column_name']
                
                if change_type == 'add_column':
                    data_type = change['data_type']
                    default_value = change.get('default_value')
                    
                    # Add column with default value
                    if default_value is not None:
                        alter_sql = f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} {data_type} DEFAULT {default_value}"
                    else:
                        alter_sql = f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} {data_type}"
                    
                    cursor.execute(alter_sql)
                    self.log.info(f"Added column {column_name} to {self.table_name}")
                    
                elif change_type == 'modify_column':
                    data_type = change['data_type']
                    alter_sql = f"ALTER TABLE {self.table_name} ALTER COLUMN {column_name} TYPE {data_type}"
                    cursor.execute(alter_sql)
                    self.log.info(f"Modified column {column_name} in {self.table_name}")
                    
                elif change_type == 'rename_column':
                    old_name = change['old_name']
                    alter_sql = f"ALTER TABLE {self.table_name} RENAME COLUMN {old_name} TO {column_name}"
                    cursor.execute(alter_sql)
                    self.log.info(f"Renamed column {old_name} to {column_name} in {self.table_name}")
                    
                else:
                    raise AirflowException(f"Unsupported schema change type: {change_type}")
            
            # Commit transaction
            cursor.execute("COMMIT")
            self.log.info(f"Successfully applied {len(self.schema_changes)} schema changes to {self.table_name}")
            
        except Exception as e:
            # Rollback transaction
            cursor.execute("ROLLBACK")
            self.log.error(f"Error applying schema changes: {e}")
            raise AirflowException(f"Schema changes failed and rolled back: {e}")