"""
NYC TLC ETL Pipeline DAG

This DAG orchestrates the ETL process for NYC TLC taxi trip data:
1. Extract: Read data from S3 raw bucket
2. Transform: Clean, validate, and enrich the data
3. Load: Insert processed data into PostgreSQL data warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import pyarrow.parquet as pq
import boto3
from sqlalchemy import create_engine
import logging
from typing import List, Dict
import os
import traceback

# Import the new configuration system
from config import (
    get_s3_raw_bucket,
    get_postgres_connection_string,
    get_s3_client_config
)

# Import the new structured logging utilities
from utils.logging_utils import get_pipeline_logger

# Import the new validation utilities
from utils.validation_utils import validate_taxi_data


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@example.com']
}

# Initialize the DAG
dag = DAG(
    'nyc_tlc_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for NYC TLC taxi trip data',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'etl', 'taxi-data']
)

# Initialize pipeline logger
pipeline_logger = get_pipeline_logger('nyc_tlc_etl_pipeline')


def extract_data_from_s3(**context):
    """
    Extract data from S3 raw bucket
    """
    # Get execution date from context
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    # Get run ID from context
    run_id = context.get('run_id', 'unknown_run')

    # Log task start
    pipeline_logger.log_task_start('extract_data', run_id, year=year, month=month)

    try:
        # Get S3 bucket from environment
        s3_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')

        # Initialize S3 hook
        s3_hook = S3Hook(aws_conn_id='aws_default')

        # Define the S3 key pattern for the current date
        s3_key_pattern = f"taxi-data/yellow/{year}/{year}-{month:02d}.parquet"

        # List all files in the S3 bucket for the current month
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=f"taxi-data/yellow/{year}/{year}-{month:02d}"
        )

        if 'Contents' not in response:
            pipeline_logger.log_warning(f"No files found for {year}-{month:02d}", 'extract_data', year=year, month=month)
            return []

        # Get all Parquet files
        parquet_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.parquet')
        ]

        # Download each file to a temporary location
        temp_files = []
        for s3_key in parquet_files:
            # Download file to temporary location
            local_path = f"/tmp/{s3_key.split('/')[-1]}"
            s3_hook.download_file(
                key=s3_key,
                bucket_name=s3_bucket,
                local_path=local_path
            )
            temp_files.append(local_path)
            pipeline_logger.info(f"Downloaded {s3_key} to {local_path}", task_id='extract_data', file=s3_key)

        pipeline_logger.log_data_processing('extract', len(temp_files), file_count=len(temp_files))
        pipeline_logger.log_task_end('extract_data', run_id, 'SUCCESS', 0, file_count=len(temp_files))

        return temp_files
    except Exception as e:
        error_msg = f"Error extracting data from S3: {str(e)}"
        pipeline_logger.log_error('S3ExtractionError', error_msg, 'extract_data',
                                  year=year, month=month, traceback=traceback.format_exc())
        raise


def transform_data(**context):
    """
    Transform the raw taxi data: clean, validate, and enrich
    """
    # Get run ID from context
    run_id = context.get('run_id', 'unknown_run')

    # Log task start
    pipeline_logger.log_task_start('transform_data', run_id)

    try:
        # Get the list of files from the previous task
        temp_files = context['task_instance'].xcom_pull(task_ids='extract_data')

        if not temp_files:
            pipeline_logger.log_warning("No files to process", 'transform_data')
            return []

        processed_data = []
        total_records = 0

        # Process each downloaded file
        for file_path in temp_files:
            pipeline_logger.info(f"Processing file: {file_path}", task_id='transform_data', file=file_path)

            try:
                # Read the Parquet file
                table = pq.read_table(file_path)
                df = table.to_pandas()

                # Clean the data
                df = clean_taxi_data(df)

                # Enrich with taxi zone data
                df = enrich_with_taxi_zones(df)

                # Add year and month columns
                df['year'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.year
                df['month'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.month

                # Add to processed data
                processed_data.append(df)

                record_count = len(df)
                total_records += record_count
                pipeline_logger.info(f"Successfully processed {record_count} records from {file_path}",
                                   task_id='transform_data', file=file_path, record_count=record_count)

            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                pipeline_logger.log_error('FileProcessingError', error_msg, 'transform_data',
                                        file=file_path, traceback=traceback.format_exc())
                # Continue processing other files
                continue

        # Combine all dataframes if there are multiple files
        if processed_data:
            combined_df = pd.concat(processed_data, ignore_index=True)

            # Save the combined dataframe to a temporary file
            temp_combined_path = "/tmp/processed_taxi_data.parquet"
            combined_df.to_parquet(temp_combined_path, index=False)

            pipeline_logger.log_data_processing('transform', len(combined_df),
                                              file_count=len(processed_data), total_records=len(combined_df))
            pipeline_logger.log_task_end('transform_data', run_id, 'SUCCESS', 0,
                                       file_count=len(processed_data), total_records=len(combined_df))

            return temp_combined_path
        else:
            pipeline_logger.log_warning("No data was processed", 'transform_data')
            pipeline_logger.log_task_end('transform_data', run_id, 'SUCCESS', 0, total_records=0)
            return None
    except Exception as e:
        error_msg = f"Error transforming data: {str(e)}"
        pipeline_logger.log_error('DataTransformationError', error_msg, 'transform_data',
                                traceback=traceback.format_exc())
        raise


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the taxi data: remove invalid records, handle nulls, etc.
    """
    initial_count = len(df)
    pipeline_logger.info(f"Starting data cleaning for {initial_count} records", task_id='clean_taxi_data')

    try:
        # Remove rows with null pickup/dropoff datetimes
        df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

        # Remove rows with invalid coordinates
        df = df[
            (df['pickup_longitude'].between(-74.27, -73.69)) &
            (df['pickup_latitude'].between(40.49, 40.92)) &
            (df['dropoff_longitude'].between(-74.27, -73.69)) &
            (df['dropoff_latitude'].between(40.49, 40.92))
        ]

        # Remove rows with negative fare amounts
        df = df[df['fare_amount'] >= 0]

        # Remove rows with negative passenger count
        df = df[df['passenger_count'] >= 0]

        # Remove rows with negative trip distance
        df = df[df['trip_distance'] >= 0]

        # Convert datetime columns to proper format
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        # Calculate trip duration in minutes
        df['trip_duration_minutes'] = (
            df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
        ).dt.total_seconds() / 60

        # Remove trips with duration less than 1 minute or more than 24 hours
        df = df[
            (df['trip_duration_minutes'] >= 1) &
            (df['trip_duration_minutes'] <= 1440)
        ]

        final_count = len(df)
        removed_count = initial_count - final_count
        pipeline_logger.info(f"Data cleaning completed. {final_count} records remaining, {removed_count} removed",
                           task_id='clean_taxi_data', initial_count=initial_count, final_count=final_count,
                           removed_count=removed_count)

        return df
    except Exception as e:
        error_msg = f"Error cleaning taxi data: {str(e)}"
        pipeline_logger.log_error('DataCleaningError', error_msg, 'clean_taxi_data',
                                initial_count=initial_count, traceback=traceback.format_exc())
        raise


def enrich_with_taxi_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich taxi data with taxi zone information
    """
    pipeline_logger.info("Starting taxi zone enrichment", task_id='enrich_with_taxi_zones', record_count=len(df))

    try:
        # For this example, we'll add the pickup and dropoff location IDs
        # In a real scenario, you'd join with the taxi zone lookup table

        # Ensure pickup and dropoff location IDs exist
        if 'PULocationID' in df.columns:
            df.rename(columns={'PULocationID': 'pickup_location_id'}, inplace=True)
        if 'DOLocationID' in df.columns:
            df.rename(columns={'DOLocationID': 'dropoff_location_id'}, inplace=True)

        # If location IDs don't exist, create them based on coordinates (simplified)
        if 'pickup_location_id' not in df.columns:
            df['pickup_location_id'] = 0  # Default value
        if 'dropoff_location_id' not in df.columns:
            df['dropoff_location_id'] = 0  # Default value

        pipeline_logger.info("Taxi zone enrichment completed", task_id='enrich_with_taxi_zones', record_count=len(df))

        return df
    except Exception as e:
        error_msg = f"Error enriching with taxi zones: {str(e)}"
        pipeline_logger.log_error('TaxiZoneEnrichmentError', error_msg, 'enrich_with_taxi_zones',
                                record_count=len(df), traceback=traceback.format_exc())
        raise


def validate_data_with_great_expectations(df: pd.DataFrame, data_type: str = "yellow") -> Dict[str, Any]:
    """
    Validate data using Great Expectations

    Args:
        df: DataFrame to validate
        data_type: Type of taxi data ("yellow" or "green")

    Returns:
        Validation results dictionary
    """
    try:
        pipeline_logger.info(f"Starting Great Expectations validation for {len(df)} records",
                           task_id='validate_data_with_great_expectations',
                           record_count=len(df), data_type=data_type)

        # Perform validation
        validation_results = validate_taxi_data(df, data_type)

        # Log validation results
        success = validation_results.get('success', False)
        results = validation_results.get('results', [])
        failed_results = [r for r in results if not r.get('success', True)]

        pipeline_logger.info(f"Great Expectations validation completed",
                           task_id='validate_data_with_great_expectations',
                           validation_success=success,
                           total_expectations=len(results),
                           failed_expectations=len(failed_results))

        if not success:
            pipeline_logger.log_warning(f"Validation failed for {len(failed_results)} expectations",
                                      'validate_data_with_great_expectations',
                                      failed_expectations=len(failed_results))

        return validation_results
    except Exception as e:
        error_msg = f"Error during Great Expectations validation: {str(e)}"
        pipeline_logger.log_error('GreatExpectationsValidationError', error_msg,
                                'validate_data_with_great_expectations', traceback=traceback.format_exc())
        raise


def load_data_to_postgres(**context):
    """
    Load transformed data to PostgreSQL data warehouse
    """
    # Get run ID from context
    run_id = context.get('run_id', 'unknown_run')

    # Log task start
    pipeline_logger.log_task_start('load_data', run_id)

    try:
        # Get the processed file path from the previous task
        processed_file_path = context['task_instance'].xcom_pull(task_ids='transform_data')

        if not processed_file_path or not os.path.exists(processed_file_path):
            pipeline_logger.log_warning("No processed data to load", 'load_data')
            return

        # Read the processed data
        processed_df = pd.read_parquet(processed_file_path)

        if processed_df.empty:
            pipeline_logger.log_warning("No data to load to PostgreSQL", 'load_data')
            return

        # Validate the data before loading using Great Expectations
        validation_results = validate_data_with_great_expectations(processed_df, "yellow")
        validation_success = validation_results.get('success', False)

        if not validation_success:
            # Log validation failures but continue with loading
            results = validation_results.get('results', [])
            failed_results = [r for r in results if not r.get('success', True)]
            pipeline_logger.log_warning(f"Data validation failed for {len(failed_results)} expectations, continuing with load",
                                      'load_data', failed_expectations=len(failed_results))

        # Get PostgreSQL connection details from environment
        user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
        password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
        host = os.getenv('POSTGRES_HOST', 'datawarehouse')  # Using service name in Docker
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')

        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Insert data into the yellow_tripdata table
        processed_df.to_sql(
            'yellow_tripdata',
            engine,
            if_exists='append',
            index=False,
            method='multi'  # More efficient for large datasets
        )

        record_count = len(processed_df)
        pipeline_logger.info(f"Successfully loaded {record_count} records to PostgreSQL",
                           task_id='load_data', record_count=record_count, validation_success=validation_success)

        # Insert pipeline log
        insert_pipeline_log(engine, record_count, 0)

        pipeline_logger.log_data_processing('load', record_count, table='yellow_tripdata', validation_success=validation_success)
        pipeline_logger.log_task_end('load_data', run_id, 'SUCCESS', 0, record_count=record_count, validation_success=validation_success)

    except Exception as e:
        error_msg = f"Error loading data to PostgreSQL: {str(e)}"
        pipeline_logger.log_error('PostgreSQLError', error_msg, 'load_data',
                                traceback=traceback.format_exc())

        # Insert failed pipeline log
        try:
            insert_pipeline_log(engine, 0, len(processed_df), str(e))
        except:
            # If even the error logging fails, just log the error
            pipeline_logger.log_error('PipelineLogInsertError',
                                    'Failed to insert pipeline log after load failure',
                                    'load_data', traceback=traceback.format_exc())

        raise


def insert_pipeline_log(engine, records_processed: int, records_failed: int, error_message: str = None):
    """
    Insert a pipeline execution log into the database
    """
    try:
        from uuid import uuid4
        from datetime import datetime

        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        status = 'FAILED' if records_failed > 0 else 'SUCCESS'

        log_query = """
        INSERT INTO taxi_data.pipeline_logs
        (pipeline_name, run_id, status, start_time, end_time,
         records_processed, records_failed, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with engine.connect() as conn:
            conn.execute(
                log_query,
                (
                    'nyc_tlc_etl_pipeline',
                    run_id,
                    status,
                    datetime.now() - timedelta(minutes=1),  # Approx start time
                    datetime.now(),  # End time
                    records_processed,
                    records_failed,
                    error_message
                )
            )
            conn.commit()

        pipeline_logger.info(f"Pipeline log inserted: {status}, {records_processed} processed, {records_failed} failed",
                           task_id='insert_pipeline_log', status=status, records_processed=records_processed,
                           records_failed=records_failed)
    except Exception as e:
        error_msg = f"Error inserting pipeline log: {str(e)}"
        pipeline_logger.log_error('PipelineLogInsertError', error_msg, 'insert_pipeline_log',
                                records_processed=records_processed, records_failed=records_failed,
                                traceback=traceback.format_exc())


def run_quality_checks(**context):
    """
    Run data quality checks on the loaded data
    """
    # Get run ID from context
    run_id = context.get('run_id', 'unknown_run')

    # Log task start
    pipeline_logger.log_task_start('run_quality_checks', run_id)

    try:
        # Get PostgreSQL connection details
        user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
        password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
        host = os.getenv('POSTGRES_HOST', 'datawarehouse')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')

        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Get the date for quality checks (today's date)
        from datetime import date
        check_date = date.today()

        # Run various quality checks
        checks = [
            ("total_records", "COUNT(*)", "SELECT COUNT(*) as count FROM taxi_data.yellow_tripdata"),
            ("null_pickup_datetime_pct", "AVG(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) * 100",
             "SELECT AVG(CASE WHEN pickup_datetime IS NULL THEN 1.0 ELSE 0.0 END) * 100 as pct FROM taxi_data.yellow_tripdata"),
            ("null_dropoff_datetime_pct", "AVG(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) * 100",
             "SELECT AVG(CASE WHEN dropoff_datetime IS NULL THEN 1.0 ELSE 0.0 END) * 100 as pct FROM taxi_data.yellow_tripdata"),
            ("negative_fare_pct", "AVG(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) * 100",
             "SELECT AVG(CASE WHEN fare_amount < 0 THEN 1.0 ELSE 0.0 END) * 100 as pct FROM taxi_data.yellow_tripdata"),
            ("avg_trip_distance", "AVG(trip_distance)",
             "SELECT AVG(trip_distance) as avg FROM taxi_data.yellow_tripdata WHERE trip_distance > 0")
        ]

        for metric_name, metric_type, query in checks:
            with engine.connect() as conn:
                result = conn.execute(query)
                value = result.fetchone()[0]

                # Insert quality metric
                metric_query = """
                INSERT INTO taxi_data.data_quality_metrics
                (table_name, metric_type, metric_name, metric_value, date)
                VALUES (%s, %s, %s, %s, %s)
                """

                conn.execute(
                    metric_query,
                    ('yellow_tripdata', 'VALIDATION', metric_name, float(value or 0), check_date)
                )
                conn.commit()

                pipeline_logger.info(f"Quality check {metric_name}: {value}",
                                   task_id='run_quality_checks', metric_name=metric_name, metric_value=value)

        pipeline_logger.info("Data quality checks completed successfully", task_id='run_quality_checks')
        pipeline_logger.log_task_end('run_quality_checks', run_id, 'SUCCESS', 0)

    except Exception as e:
        error_msg = f"Error running quality checks: {str(e)}"
        pipeline_logger.log_error('QualityCheckError', error_msg, 'run_quality_checks',
                                traceback=traceback.format_exc())
        raise


# Define tasks in the DAG
with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # Extract data from S3
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_s3,
        dag=dag
    )

    # Transform the data
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )

    # Load data to PostgreSQL
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres,
        dag=dag
    )

    # Run quality checks
    quality_check_task = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks,
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_pipeline',
        dag=dag
    )

    # Set task dependencies
    start_task >> extract_task >> transform_task >> load_task >> quality_check_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()