"""
Database Operations Module for NYC TLC Data Platform

This module provides functions for interacting with the PostgreSQL data warehouse
for storing processed NYC TLC taxi trip data.
"""

import os
import logging
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import datetime


class DatabaseManager:
    """
    Manages database operations for the NYC TLC Data Platform
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database manager with connection string
        
        Args:
            connection_string: PostgreSQL connection string
        """
        if connection_string is None:
            # Build connection string from environment variables
            user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
            password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
            host = os.getenv('POSTGRES_HOST', 'localhost')
            port = os.getenv('POSTGRES_PORT', '5433')
            database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
            
            connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """
        Get a database connection
        """
        return self.SessionLocal()
    
    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection is successful, else False
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                return result.fetchone()[0] == 1
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
        except SQLAlchemyError as e:
            self.logger.error(f"Query execution failed: {e}")
            return []
    
    def execute_update(self, query: str, params: Optional[Dict] = None) -> bool:
        """
        Execute an INSERT, UPDATE, or DELETE query
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            True if execution was successful, else False
        """
        try:
            with self.engine.connect() as connection:
                trans = connection.begin()
                try:
                    connection.execute(text(query), params or {})
                    trans.commit()
                    return True
                except:
                    trans.rollback()
                    raise
        except SQLAlchemyError as e:
            self.logger.error(f"Update execution failed: {e}")
            return False
    
    def bulk_insert(self, table_name: str, data: List[Dict]) -> bool:
        """
        Perform bulk insert into a table
        
        Args:
            table_name: Name of the table to insert into
            data: List of dictionaries representing rows to insert
            
        Returns:
            True if insert was successful, else False
        """
        if not data:
            return True  # Nothing to insert
            
        try:
            df = pd.DataFrame(data)
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            self.logger.info(f"Successfully inserted {len(data)} rows into {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {e}")
            return False
    
    def insert_pipeline_log(self, pipeline_name: str, run_id: str, status: str, 
                           start_time: datetime, end_time: datetime, 
                           records_processed: int, records_failed: int, 
                           error_message: Optional[str] = None) -> bool:
        """
        Insert a pipeline execution log
        
        Args:
            pipeline_name: Name of the pipeline
            run_id: Unique run identifier
            status: Execution status (SUCCESS, FAILED, RUNNING)
            start_time: Pipeline start time
            end_time: Pipeline end time
            records_processed: Number of records processed
            records_failed: Number of records that failed
            error_message: Error message if any
            
        Returns:
            True if log was inserted, else False
        """
        query = """
        INSERT INTO taxi_data.pipeline_logs 
        (pipeline_name, run_id, status, start_time, end_time, 
         records_processed, records_failed, error_message)
        VALUES (:pipeline_name, :run_id, :status, :start_time, :end_time,
                :records_processed, :records_failed, :error_message)
        """
        
        params = {
            'pipeline_name': pipeline_name,
            'run_id': run_id,
            'status': status,
            'start_time': start_time,
            'end_time': end_time,
            'records_processed': records_processed,
            'records_failed': records_failed,
            'error_message': error_message
        }
        
        return self.execute_update(query, params)
    
    def insert_data_quality_metric(self, table_name: str, metric_type: str, 
                                   metric_name: str, metric_value: float, 
                                   date: datetime.date) -> bool:
        """
        Insert a data quality metric
        
        Args:
            table_name: Name of the table the metric relates to
            metric_type: Type of metric (COUNT, VALIDATION, etc.)
            metric_name: Name of the metric
            metric_value: Value of the metric
            date: Date for the metric
            
        Returns:
            True if metric was inserted, else False
        """
        query = """
        INSERT INTO taxi_data.data_quality_metrics 
        (table_name, metric_type, metric_name, metric_value, date)
        VALUES (:table_name, :metric_type, :metric_name, :metric_value, :date)
        """
        
        params = {
            'table_name': table_name,
            'metric_type': metric_type,
            'metric_name': metric_name,
            'metric_value': metric_value,
            'date': date
        }
        
        return self.execute_update(query, params)
    
    def get_recent_pipeline_runs(self, pipeline_name: str, limit: int = 10) -> List[Dict]:
        """
        Get recent pipeline runs for a specific pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            limit: Number of recent runs to return
            
        Returns:
            List of pipeline run records
        """
        query = """
        SELECT * FROM taxi_data.pipeline_logs 
        WHERE pipeline_name = :pipeline_name 
        ORDER BY created_at DESC 
        LIMIT :limit
        """
        
        params = {
            'pipeline_name': pipeline_name,
            'limit': limit
        }
        
        return self.execute_query(query, params)
    
    def get_data_quality_metrics(self, table_name: str, days: int = 7) -> List[Dict]:
        """
        Get data quality metrics for a table in the last N days
        
        Args:
            table_name: Name of the table
            days: Number of days to look back
            
        Returns:
            List of data quality metric records
        """
        query = """
        SELECT * FROM taxi_data.data_quality_metrics 
        WHERE table_name = :table_name 
        AND date >= CURRENT_DATE - INTERVAL ':days days'
        ORDER BY date DESC, metric_name
        """
        
        params = {
            'table_name': table_name,
            'days': days
        }
        
        return self.execute_query(query, params)
    
    def validate_schema_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, else False
        """
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'taxi_data' 
            AND table_name = :table_name
        );
        """
        
        params = {'table_name': table_name}
        result = self.execute_query(query, params)
        
        if result:
            return result[0]['exists']
        return False


def get_db_connection_string() -> str:
    """
    Get database connection string from environment variables
    """
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5433')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


if __name__ == "__main__":
    # Example usage
    db_manager = DatabaseManager()
    
    if db_manager.test_connection():
        print("Database connection successful!")
        
        # Check if schema exists
        if db_manager.validate_schema_exists('yellow_tripdata'):
            print("Yellow taxi data table exists")
        else:
            print("Yellow taxi data table does not exist")
            
        # Get recent pipeline runs
        recent_runs = db_manager.get_recent_pipeline_runs('yellow_tripdata_etl', 5)
        print(f"Found {len(recent_runs)} recent pipeline runs")
    else:
        print("Database connection failed!")