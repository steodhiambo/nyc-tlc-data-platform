"""
Modular ELT Pipeline for NYC TLC Data Platform

This module implements a more modular ELT approach where:
1. Raw data is loaded directly to staging tables
2. Transformations happen in dbt (ELT vs ETL approach)
3. DAGs are modular and reusable
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import pandas as pd
import pyarrow.parquet as pq
import boto3
from sqlalchemy import create_engine
import logging
from typing import List, Dict
import os
import requests
from io import StringIO


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


def extract_raw_data_from_s3(data_type: str, **context):
    """
    Extract raw data from S3 without transformation (ELT approach)
    
    Args:
        data_type: Type of data to extract ('yellow', 'green', 'fhv', etc.)
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
    s3_prefix = f"taxi-data/{data_type}/{year}/{year}-{month:02d}"

    # List all files in the S3 bucket for the current month
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=s3_prefix
    )

    if 'Contents' not in response:
        logging.info(f"No files found for {data_type} in {year}-{month:02d}")
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
        local_path = f"/tmp/{s3_key.replace('/', '_')}"
        s3_hook.download_file(
            key=s3_key,
            bucket_name=s3_bucket,
            local_path=local_path
        )
        temp_files.append(local_path)
        logging.info(f"Downloaded {s3_key} to {local_path}")

    return temp_files


def load_raw_data_to_staging(data_type: str, **context):
    """
    Load raw data directly to staging tables without transformation
    
    Args:
        data_type: Type of data to load ('yellow', 'green', 'fhv', etc.)
    """
    # Get the list of files from the previous task
    temp_files = context['task_instance'].xcom_pull(task_ids=f'extract_{data_type}_data')

    if not temp_files:
        logging.info(f"No {data_type} files to load")
        return

    # Get PostgreSQL connection details from environment
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        for file_path in temp_files:
            logging.info(f"Loading raw data from {file_path}")

            # Read the Parquet file
            table = pq.read_table(file_path)
            df = table.to_pandas()

            # Add metadata columns
            df['loaded_at'] = datetime.now()
            df['source_file'] = os.path.basename(file_path)
            df['year'] = execution_date.year
            df['month'] = execution_date.month

            # Determine target table based on data type
            table_name = f"stg_{data_type}_tripdata"

            # Load to staging table
            df.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )

            logging.info(f"Successfully loaded {len(df)} records to {table_name}")

    except Exception as e:
        logging.error(f"Error loading raw data to staging: {e}")
        raise


def run_dbt_transformations(dbt_project_path: str = "/home/steodhiambo/nyc-tlc-data-platform/dbt"):
    """
    Run dbt transformations to process staging data into dimensional models
    """
    import subprocess
    
    try:
        # Run dbt seed to load lookup tables
        seed_result = subprocess.run(
            ["dbt", "seed", "--project-dir", dbt_project_path],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt seed completed: {seed_result.stdout}")

        # Run dbt models to transform staging data
        run_result = subprocess.run(
            ["dbt", "run", "--project-dir", dbt_project_path],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt run completed: {run_result.stdout}")

        # Run dbt tests to validate transformed data
        test_result = subprocess.run(
            ["dbt", "test", "--project-dir", dbt_project_path],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info(f"dbt test completed: {test_result.stdout}")

        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"dbt command failed: {e}")
        logging.error(f"dbt stderr: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"Error running dbt transformations: {e}")
        raise


def load_taxi_zone_data(**context):
    """
    Load taxi zone lookup data from CSV source
    """
    # Taxi zone data URL (this would be the actual NYC TLC taxi zone CSV)
    taxi_zone_url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    
    try:
        # Download taxi zone data
        response = requests.get(taxi_zone_url)
        response.raise_for_status()
        
        # Read CSV data
        df = pd.read_csv(StringIO(response.text))
        
        # Clean and standardize column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
        
        # Add metadata
        df['loaded_at'] = datetime.now()
        
        # Get PostgreSQL connection details
        user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
        password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
        host = os.getenv('POSTGRES_HOST', 'datawarehouse')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')

        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Load to staging table
        df.to_sql(
            'stg_taxi_zone_lookup',
            engine,
            if_exists='replace',  # Replace for lookup tables
            index=False,
            method='multi'
        )
        
        logging.info(f"Loaded {len(df)} taxi zone records")
        
        return len(df)
        
    except Exception as e:
        logging.error(f"Error loading taxi zone data: {e}")
        raise


def load_weather_data(**context):
    """
    Load weather data for enrichment (simulated API call)
    """
    # In a real implementation, this would call a weather API
    # For now, we'll create simulated weather data
    
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    # Generate simulated weather data for NYC for the month
    import numpy as np
    
    # Create date range for the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate simulated weather data
    weather_data = []
    for date in date_range:
        for hour in range(24):
            weather_data.append({
                'date': date.date(),
                'hour': hour,
                'temperature': np.random.uniform(32, 85),  # Fahrenheit
                'precipitation': np.random.choice([0, 0.1, 0.2, 0.5, 1.0], p=[0.7, 0.15, 0.1, 0.04, 0.01]),
                'weather_condition': np.random.choice(['Clear', 'Cloudy', 'Rain', 'Snow'], p=[0.6, 0.25, 0.1, 0.05]),
                'location_borough': 'Manhattan',  # Simplified for NYC
                'loaded_at': datetime.now()
            })
    
    weather_df = pd.DataFrame(weather_data)
    
    # Get PostgreSQL connection details
    user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
    password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
    host = os.getenv('POSTGRES_HOST', 'datawarehouse')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Load to staging table
    weather_df.to_sql(
        'stg_weather_data',
        engine,
        if_exists='replace',  # Replace for daily weather data
        index=False,
        method='multi'
    )
    
    logging.info(f"Loaded {len(weather_df)} weather records")
    
    return len(weather_df)


# Initialize the DAG
dag = DAG(
    'nyc_tlc_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for NYC TLC taxi trip data (modular approach)',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'elt', 'taxi-data', 'modular']
)


with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_elt_pipeline',
        dag=dag
    )

    # Task group for yellow taxi data processing
    with TaskGroup(group_id='yellow_taxi_processing', dag=dag) as yellow_taxi_group:
        extract_yellow_task = PythonOperator(
            task_id='extract_yellow_data',
            python_callable=extract_raw_data_from_s3,
            op_kwargs={'data_type': 'yellow'},
            dag=dag
        )

        load_yellow_task = PythonOperator(
            task_id='load_yellow_to_staging',
            python_callable=load_raw_data_to_staging,
            op_kwargs={'data_type': 'yellow'},
            dag=dag
        )

        extract_yellow_task >> load_yellow_task

    # Task group for green taxi data processing
    with TaskGroup(group_id='green_taxi_processing', dag=dag) as green_taxi_group:
        extract_green_task = PythonOperator(
            task_id='extract_green_data',
            python_callable=extract_raw_data_from_s3,
            op_kwargs={'data_type': 'green'},
            dag=dag
        )

        load_green_task = PythonOperator(
            task_id='load_green_to_staging',
            python_callable=load_raw_data_to_staging,
            op_kwargs={'data_type': 'green'},
            dag=dag
        )

        extract_green_task >> load_green_task

    # Load taxi zone lookup data
    load_taxi_zone_task = PythonOperator(
        task_id='load_taxi_zone_data',
        python_callable=load_taxi_zone_data,
        dag=dag
    )

    # Load weather data
    load_weather_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
        dag=dag
    )

    # Run dbt transformations
    run_dbt_task = PythonOperator(
        task_id='run_dbt_transformations',
        python_callable=run_dbt_transformations,
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_elt_pipeline',
        dag=dag
    )

    # Set task dependencies
    start_task >> [yellow_taxi_group, green_taxi_group, load_taxi_zone_task, load_weather_task]
    [yellow_taxi_group, green_taxi_group, load_taxi_zone_task, load_weather_task] >> run_dbt_task
    run_dbt_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()