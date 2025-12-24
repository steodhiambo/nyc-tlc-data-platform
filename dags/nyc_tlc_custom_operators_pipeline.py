"""
NYC TLC Data Pipeline using Custom Operators

This DAG demonstrates the use of custom operators for the NYC TLC data pipeline
with modular design and ELT approach.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from scripts.custom_airflow_operators import (
    S3ToPostgresOperator, 
    DbtRunOperator, 
    DataQualityCheckOperator,
    SchemaChangeOperator
)
import logging


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
    'nyc_tlc_custom_operators_pipeline',
    default_args=default_args,
    description='NYC TLC pipeline using custom operators',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['nyc-tlc', 'custom-operators', 'modular']
)


with dag:
    # Start task
    start_task = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # Load yellow taxi data from S3 to staging
    load_yellow_data = S3ToPostgresOperator(
        task_id='load_yellow_taxi_data',
        s3_bucket='{{ var.value.s3_raw_bucket }}',
        s3_key='taxi-data/yellow/{{ execution_date.year }}/{{ execution_date.year }}-{{ "%02d" % execution_date.month }}.parquet',
        postgres_conn_id='postgres_default',
        table_name='stg_yellow_tripdata',
        file_format='parquet',
        dag=dag
    )

    # Load green taxi data from S3 to staging
    load_green_data = S3ToPostgresOperator(
        task_id='load_green_taxi_data',
        s3_bucket='{{ var.value.s3_raw_bucket }}',
        s3_key='taxi-data/green/{{ execution_date.year }}/{{ execution_date.year }}-{{ "%02d" % execution_date.month }}.parquet',
        postgres_conn_id='postgres_default',
        table_name='stg_green_tripdata',
        file_format='parquet',
        dag=dag
    )

    # Load taxi zone data from S3 to staging
    load_taxi_zone = S3ToPostgresOperator(
        task_id='load_taxi_zone_data',
        s3_bucket='{{ var.value.s3_raw_bucket }}',
        s3_key='taxi-data/taxi_zone_lookup.csv',
        postgres_conn_id='postgres_default',
        table_name='stg_taxi_zone_lookup',
        file_format='csv',
        dag=dag
    )

    # Run data quality checks on staging tables
    quality_check_yellow = DataQualityCheckOperator(
        task_id='quality_check_yellow',
        table_name='stg_yellow_tripdata',
        checks={
            'row_count_check': {
                'type': 'row_count',
                'expected': 1000  # Minimum expected rows
            },
            'pickup_datetime_null_check': {
                'type': 'null_check',
                'column': 'tpep_pickup_datetime',
                'expected': 1.0  # Max 1% null values allowed
            },
            'fare_amount_range_check': {
                'type': 'range_check',
                'column': 'fare_amount',
                'min_value': 0,
                'max_value': 1000
            }
        },
        dag=dag
    )

    quality_check_green = DataQualityCheckOperator(
        task_id='quality_check_green',
        table_name='stg_green_tripdata',
        checks={
            'row_count_check': {
                'type': 'row_count',
                'expected': 1000  # Minimum expected rows
            },
            'pickup_datetime_null_check': {
                'type': 'null_check',
                'column': 'lpep_pickup_datetime',
                'expected': 1.0  # Max 1% null values allowed
            },
            'fare_amount_range_check': {
                'type': 'range_check',
                'column': 'fare_amount',
                'min_value': 0,
                'max_value': 1000
            }
        },
        dag=dag
    )

    # Run dbt transformations
    run_dbt_transformations = DbtRunOperator(
        task_id='run_dbt_transformations',
        dbt_project_dir='/home/steodhiambo/nyc-tlc-data-platform/dbt',
        dbt_command='run',
        dbt_models=['staging', 'core'],  # Run staging and core models
        dag=dag
    )

    # Run dbt tests
    run_dbt_tests = DbtRunOperator(
        task_id='run_dbt_tests',
        dbt_project_dir='/home/steodhiambo/nyc-tlc-data-platform/dbt',
        dbt_command='test',
        dag=dag
    )

    # Example of schema change (for demonstration - would be conditional in real use)
    schema_change_example = SchemaChangeOperator(
        task_id='schema_change_example',
        table_name='stg_yellow_tripdata',
        schema_changes=[
            {
                'type': 'add_column',
                'column_name': 'weather_condition',
                'data_type': 'VARCHAR(50)',
                'default_value': "'unknown'"
            }
        ],
        dag=dag
    )

    # End task
    end_task = DummyOperator(
        task_id='end_pipeline',
        dag=dag
    )

    # Set task dependencies
    start_task >> [load_yellow_data, load_green_data, load_taxi_zone]
    
    # Data loading must complete before quality checks
    [load_yellow_data, load_green_data, load_taxi_zone] >> [quality_check_yellow, quality_check_green]
    
    # Quality checks must pass before dbt transformations
    [quality_check_yellow, quality_check_green] >> run_dbt_transformations
    
    # Run tests after transformations
    run_dbt_transformations >> [run_dbt_tests, schema_change_example]
    
    # Both tests and schema changes must complete before end
    [run_dbt_tests, schema_change_example] >> end_task


if __name__ == "__main__":
    # For testing purposes
    dag.test()