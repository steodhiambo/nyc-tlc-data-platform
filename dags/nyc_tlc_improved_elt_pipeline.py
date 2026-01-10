"""
Improved ELT Pipeline for NYC TLC Data Platform

This DAG implements an enhanced ELT (Extract, Load, Transform) approach where:
1. Raw data is extracted from S3 and loaded directly to staging tables in PostgreSQL
2. Transformations happen within the database using SQL operations
3. This approach leverages PostgreSQL's computational power for better performance
4. Includes incremental loading and advanced data quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SqlOperator
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
            df['year'] = context['execution_date'].year
            df['month'] = context['execution_date'].month

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


def run_incremental_load_sql(**context):
    """
    Execute SQL for incremental loading from staging to final tables
    This performs the transformation in the database using SQL
    """
    # Get execution date from context
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month

    # SQL for incremental load with transformations
    incremental_load_sql = f"""
    -- Create temporary table for cleaned data
    CREATE TEMP TABLE temp_cleaned_data AS
    SELECT 
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        rate_code_id,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        year,
        month,
        loaded_at,
        source_file,
        -- Calculate derived fields in SQL
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 AS trip_duration_minutes,
        -- Clean coordinates to be within NYC bounds
        CASE 
            WHEN pickup_longitude BETWEEN -74.27 AND -73.69 THEN pickup_longitude
            ELSE NULL
        END AS pickup_longitude_clean,
        CASE 
            WHEN pickup_latitude BETWEEN 40.49 AND 40.92 THEN pickup_latitude
            ELSE NULL
        END AS pickup_latitude_clean,
        CASE 
            WHEN dropoff_longitude BETWEEN -74.27 AND -73.69 THEN dropoff_longitude
            ELSE NULL
        END AS dropoff_longitude_clean,
        CASE 
            WHEN dropoff_latitude BETWEEN 40.49 AND 40.92 THEN dropoff_latitude
            ELSE NULL
        END AS dropoff_latitude_clean
    FROM stg_yellow_tripdata
    WHERE year = {year} AND month = {month}
      AND tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND fare_amount >= 0
      AND passenger_count >= 0
      AND trip_distance >= 0
      -- Filter for trips between 1 minute and 24 hours
      AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 BETWEEN 1 AND 1440;

    -- Insert cleaned data into the final table
    INSERT INTO taxi_data.yellow_tripdata (
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        rate_code_id,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        year,
        month,
        created_at
    )
    SELECT 
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude_clean,
        pickup_latitude_clean,
        rate_code_id,
        store_and_fwd_flag,
        dropoff_longitude_clean,
        dropoff_latitude_clean,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        year,
        month,
        loaded_at
    FROM temp_cleaned_data
    WHERE pickup_longitude_clean IS NOT NULL
      AND pickup_latitude_clean IS NOT NULL
      AND dropoff_longitude_clean IS NOT NULL
      AND dropoff_latitude_clean IS NOT NULL;

    -- Log the number of records processed
    INSERT INTO taxi_data.pipeline_logs (
        pipeline_name, run_id, status, start_time, end_time,
        records_processed, records_failed
    )
    VALUES (
        'nyc_tlc_improved_elt_pipeline',
        '{context['run_id']}',
        'SUCCESS',
        NOW() - INTERVAL '5 minutes',
        NOW(),
        (SELECT COUNT(*) FROM temp_cleaned_data),
        0
    );

    -- Clean up temporary table
    DROP TABLE temp_cleaned_data;
    """

    # Execute the SQL using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run(incremental_load_sql)
    
    logging.info(f"Incremental load completed for {year}-{month:02d}")


def run_advanced_data_quality_checks(**context):
    """
    Run advanced data quality checks using SQL
    """
    # Advanced SQL for data quality checks
    quality_check_sql = """
    -- Insert data quality metrics
    INSERT INTO taxi_data.data_quality_metrics (
        table_name, metric_type, metric_name, metric_value, date
    )
    VALUES
    -- Total records
    ('yellow_tripdata', 'COUNT', 'total_records', (SELECT COUNT(*) FROM taxi_data.yellow_tripdata), CURRENT_DATE),
    
    -- Null percentage checks
    ('yellow_tripdata', 'VALIDATION', 'null_pickup_datetime_pct', 
     (SELECT AVG(CASE WHEN pickup_datetime IS NULL THEN 1.0 ELSE 0.0 END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    ('yellow_tripdata', 'VALIDATION', 'null_dropoff_datetime_pct', 
     (SELECT AVG(CASE WHEN dropoff_datetime IS NULL THEN 1.0 ELSE 0.0 END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    -- Negative value checks
    ('yellow_tripdata', 'VALIDATION', 'negative_fare_pct', 
     (SELECT AVG(CASE WHEN fare_amount < 0 THEN 1.0 ELSE 0.0 END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    ('yellow_tripdata', 'VALIDATION', 'negative_passenger_count_pct', 
     (SELECT AVG(CASE WHEN passenger_count < 0 THEN 1.0 ELSE 0.0 END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    -- Geographic boundary checks
    ('yellow_tripdata', 'VALIDATION', 'invalid_coordinates_pct', 
     (SELECT AVG(CASE 
         WHEN pickup_longitude IS NOT NULL AND (pickup_longitude < -74.27 OR pickup_longitude > -73.69) THEN 1.0
         WHEN pickup_latitude IS NOT NULL AND (pickup_latitude < 40.49 OR pickup_latitude > 40.92) THEN 1.0
         WHEN dropoff_longitude IS NOT NULL AND (dropoff_longitude < -74.27 OR dropoff_longitude > -73.69) THEN 1.0
         WHEN dropoff_latitude IS NOT NULL AND (dropoff_latitude < 40.49 OR dropoff_latitude > 40.92) THEN 1.0
         ELSE 0.0
     END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    -- Trip duration checks
    ('yellow_tripdata', 'VALIDATION', 'invalid_trip_duration_pct', 
     (SELECT AVG(CASE 
         WHEN pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL 
              AND (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) < 1 THEN 1.0
         WHEN pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL 
              AND (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60) > 1440 THEN 1.0
         ELSE 0.0
     END) * 100 FROM taxi_data.yellow_tripdata), CURRENT_DATE),
     
    -- Average values
    ('yellow_tripdata', 'STATS', 'avg_trip_distance', 
     (SELECT AVG(trip_distance) FROM taxi_data.yellow_tripdata WHERE trip_distance > 0), CURRENT_DATE),
     
    ('yellow_tripdata', 'STATS', 'avg_fare_amount', 
     (SELECT AVG(fare_amount) FROM taxi_data.yellow_tripdata WHERE fare_amount > 0), CURRENT_DATE);
    """

    # Execute the SQL using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    postgres_hook.run(quality_check_sql)
    
    logging.info("Advanced data quality checks completed")


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

        # Then load to final table with proper schema
        zone_insert_sql = """
        INSERT INTO taxi_data.taxi_zone_lookup (location_id, borough, zone, service_zone, created_at)
        SELECT 
            CAST(locationid AS INTEGER),
            UPPER(borough),
            INITCAP(zone),
            INITCAP(service_zone),
            NOW()
        FROM stg_taxi_zone_lookup
        ON CONFLICT (location_id) DO UPDATE SET
            borough = EXCLUDED.borough,
            zone = EXCLUDED.zone,
            service_zone = EXCLUDED.service_zone,
            updated_at = NOW();
        """
        
        with engine.connect() as conn:
            conn.execute(zone_insert_sql)
            conn.commit()

        logging.info(f"Loaded {len(df)} taxi zone records")

        return len(df)

    except Exception as e:
        logging.error(f"Error loading taxi zone data: {e}")
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


# Initialize the DAG
dag = DAG(
    'nyc_tlc_improved_elt_pipeline',
    default_args=default_args,
    description='Improved ELT pipeline for NYC TLC taxi trip data with database-side transformations',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'improved-elt', 'taxi-data', 'database-transformations']
)


with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_improved_elt_pipeline',
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

    # Run database-side transformations
    run_database_transforms_task = PythonOperator(
        task_id='run_database_transforms',
        python_callable=run_incremental_load_sql,
        dag=dag
    )

    # Run advanced data quality checks
    run_quality_checks_task = PythonOperator(
        task_id='run_advanced_quality_checks',
        python_callable=run_advanced_data_quality_checks,
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
        task_id='end_improved_elt_pipeline',
        dag=dag
    )

    # Set task dependencies
    start_task >> [yellow_taxi_group, green_taxi_group, load_taxi_zone_task]
    [yellow_taxi_group, green_taxi_group, load_taxi_zone_task] >> run_database_transforms_task
    run_database_transforms_task >> run_quality_checks_task
    run_quality_checks_task >> run_dbt_task
    run_dbt_task >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()