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


def extract_data_from_s3(**context):
    """
    Extract data from S3 raw bucket
    """
    # Get execution date from context
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
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
        logging.info(f"No files found for {year}-{month:02d}")
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
        logging.info(f"Downloaded {s3_key} to {local_path}")
    
    return temp_files


def transform_data(**context):
    """
    Transform the raw taxi data: clean, validate, and enrich
    """
    # Get the list of files from the previous task
    temp_files = context['task_instance'].xcom_pull(task_ids='extract_data')
    
    if not temp_files:
        logging.info("No files to process")
        return []
    
    processed_data = []
    
    # Process each downloaded file
    for file_path in temp_files:
        logging.info(f"Processing file: {file_path}")
        
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
            
            logging.info(f"Successfully processed {len(df)} records from {file_path}")
            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            # Continue processing other files
            continue
    
    # Combine all dataframes if there are multiple files
    if processed_data:
        combined_df = pd.concat(processed_data, ignore_index=True)
        
        # Save the combined dataframe to a temporary file
        temp_combined_path = "/tmp/processed_taxi_data.parquet"
        combined_df.to_parquet(temp_combined_path, index=False)
        
        logging.info(f"Combined {len(processed_data)} files into {temp_combined_path} with {len(combined_df)} records")
        
        return temp_combined_path
    else:
        logging.info("No data was processed")
        return None


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the taxi data: remove invalid records, handle nulls, etc.
    """
    logging.info(f"Starting data cleaning for {len(df)} records")
    
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
    
    logging.info(f"Data cleaning completed. {len(df)} records remaining")
    
    return df


def enrich_with_taxi_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich taxi data with taxi zone information
    """
    logging.info("Starting taxi zone enrichment")
    
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
    
    logging.info("Taxi zone enrichment completed")
    
    return df


def load_data_to_postgres(**context):
    """
    Load transformed data to PostgreSQL data warehouse
    """
    # Get the processed file path from the previous task
    processed_file_path = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    if not processed_file_path or not os.path.exists(processed_file_path):
        logging.info("No processed data to load")
        return
    
    # Read the processed data
    processed_df = pd.read_parquet(processed_file_path)
    
    if processed_df.empty:
        logging.info("No data to load to PostgreSQL")
        return
    
    # Get PostgreSQL connection details from environment
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')  # Using service name in Docker
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
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
        
        logging.info(f"Successfully loaded {len(processed_df)} records to PostgreSQL")
        
        # Insert pipeline log
        insert_pipeline_log(engine, len(processed_df), 0)
        
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")
        
        # Insert failed pipeline log
        insert_pipeline_log(engine, 0, len(processed_df), str(e))
        
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
        
        logging.info(f"Pipeline log inserted: {status}, {records_processed} processed, {records_failed} failed")
    except Exception as e:
        logging.error(f"Error inserting pipeline log: {e}")


def run_quality_checks(**context):
    """
    Run data quality checks on the loaded data
    """
    logging.info("Starting data quality checks")
    
    # Get PostgreSQL connection details
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
    
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    try:
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
                
                logging.info(f"Quality check {metric_name}: {value}")
        
        logging.info("Data quality checks completed successfully")
        
    except Exception as e:
        logging.error(f"Error running quality checks: {e}")
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