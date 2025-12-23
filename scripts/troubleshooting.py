#!/usr/bin/env python3
"""
Production Issue Simulation and Troubleshooting Script

This script simulates common production issues in the NYC TLC Data Platform
and provides troubleshooting utilities to help diagnose and resolve them.
"""

import os
import sys
import time
import random
import logging
import subprocess
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json
import pandas as pd
from sqlalchemy import create_engine
import boto3
from botocore.exceptions import ClientError


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('troubleshooting.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProductionIssueSimulator:
    """
    Simulates production issues for the NYC TLC Data Platform
    """
    
    def __init__(self):
        """
        Initialize the simulator with connection details
        """
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.s3_raw_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
        self.s3_processed_bucket = os.getenv('S3_PROCESSED_BUCKET', 'nyc-tlc-processed-data-us-east-1')
        
        # PostgreSQL connection details
        user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
        password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5433')
        database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
        
        self.db_connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.db_connection_string)
    
    def simulate_delayed_files(self) -> bool:
        """
        Simulate delayed files in S3 (files that should have been processed but weren't)
        """
        logger.info("Simulating delayed files in S3...")
        
        try:
            # List files in S3 raw bucket for the last 2 days
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_raw_bucket,
                Prefix='taxi-data/yellow/',
                MaxKeys=100
            )
            
            if 'Contents' not in response:
                logger.warning("No files found in raw bucket")
                return False
            
            # Find files older than 1 day that might be delayed
            delayed_files = []
            one_day_ago = datetime.now() - timedelta(days=1)
            
            for obj in response['Contents']:
                if obj['LastModified'] < one_day_ago:
                    delayed_files.append(obj['Key'])
            
            if delayed_files:
                logger.info(f"Found {len(delayed_files)} potentially delayed files:")
                for file_key in delayed_files[:5]:  # Show first 5
                    logger.info(f"  - {file_key}")
                
                # In a real scenario, we would trigger an alert
                logger.warning("ALERT: Potential delayed files detected!")
                return True
            else:
                logger.info("No delayed files detected")
                return False
                
        except ClientError as e:
            logger.error(f"Error accessing S3: {e}")
            return False
    
    def simulate_corrupt_rows(self) -> bool:
        """
        Simulate corrupt rows in the data by inserting malformed records
        """
        logger.info("Simulating corrupt rows in the database...")
        
        try:
            # Create a test table with some corrupt data
            with self.engine.connect() as conn:
                # Insert some valid records
                valid_records = [
                    {
                        'vendor_id': 1,
                        'pickup_datetime': '2023-01-01 10:00:00',
                        'dropoff_datetime': '2023-01-01 10:15:00',
                        'passenger_count': 1,
                        'trip_distance': 2.5,
                        'pickup_location_id': 234,
                        'dropoff_location_id': 140,
                        'fare_amount': 12.5,
                        'year': 2023,
                        'month': 1
                    }
                ]
                
                # Insert some corrupt records
                corrupt_records = [
                    {
                        'vendor_id': None,  # Null value where it shouldn't be
                        'pickup_datetime': 'invalid_date',  # Invalid date format
                        'dropoff_datetime': '2023-01-01 10:15:00',
                        'passenger_count': -1,  # Negative value
                        'trip_distance': -2.5,  # Negative distance
                        'pickup_location_id': 0,  # Invalid location ID
                        'dropoff_location_id': 99999,  # Non-existent location ID
                        'fare_amount': -12.5,  # Negative fare
                        'year': 2023,
                        'month': 1
                    },
                    {
                        'vendor_id': 2,
                        'pickup_datetime': '2023-01-01 11:00:00',
                        'dropoff_datetime': '2023-01-01 10:45:00',  # Dropoff before pickup
                        'passenger_count': 5,
                        'trip_distance': 0,  # Zero distance with different times
                        'pickup_location_id': 161,
                        'dropoff_location_id': 162,
                        'fare_amount': 0,  # Zero fare
                        'year': 2023,
                        'month': 1
                    }
                ]
                
                # Insert corrupt records into a test table
                for record in corrupt_records:
                    insert_query = """
                    INSERT INTO taxi_data.yellow_tripdata 
                    (vendor_id, pickup_datetime, dropoff_datetime, passenger_count, 
                     trip_distance, pickup_location_id, dropoff_location_id, 
                     fare_amount, year, month)
                    VALUES (%(vendor_id)s, %(pickup_datetime)s, %(dropoff_datetime)s, 
                            %(passenger_count)s, %(trip_distance)s, %(pickup_location_id)s, 
                            %(dropoff_location_id)s, %(fare_amount)s, %(year)s, %(month)s)
                    """
                    
                    conn.execute(insert_query, record)
                
                conn.commit()
                logger.info(f"Inserted {len(corrupt_records)} corrupt records for testing")
                return True
                
        except Exception as e:
            logger.error(f"Error inserting corrupt records: {e}")
            return False
    
    def simulate_pipeline_failure(self) -> bool:
        """
        Simulate a pipeline failure by creating a failed pipeline log entry
        """
        logger.info("Simulating pipeline failure...")
        
        try:
            with self.engine.connect() as conn:
                insert_query = """
                INSERT INTO taxi_data.pipeline_logs 
                (pipeline_name, run_id, status, start_time, end_time, 
                 records_processed, records_failed, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                conn.execute(
                    insert_query,
                    (
                        'nyc_tlc_etl_pipeline',
                        f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}_simulated',
                        'FAILED',
                        datetime.now() - timedelta(minutes=10),
                        datetime.now() - timedelta(minutes=5),
                        5000,
                        100,
                        'Connection timeout to S3 bucket'
                    )
                )
                
                conn.commit()
                logger.info("Simulated pipeline failure log entry created")
                return True
                
        except Exception as e:
            logger.error(f"Error creating simulated failure: {e}")
            return False
    
    def simulate_high_resource_usage(self) -> bool:
        """
        Simulate high resource usage by creating artificial load
        """
        logger.info("Simulating high resource usage...")
        
        # This is a simulation - in real production, you'd monitor actual resource usage
        # For demonstration, we'll just log what would happen
        logger.info("Resource usage simulation: CPU at 95%, Memory at 88%, Disk at 92%")
        logger.warning("ALERT: High resource usage detected!")
        
        # In a real system, you would trigger alerts based on actual metrics
        return True
    
    def run_all_simulations(self) -> Dict[str, bool]:
        """
        Run all production issue simulations
        """
        logger.info("Running all production issue simulations...")
        
        results = {
            'delayed_files': self.simulate_delayed_files(),
            'corrupt_rows': self.simulate_corrupt_rows(),
            'pipeline_failure': self.simulate_pipeline_failure(),
            'high_resource_usage': self.simulate_high_resource_usage()
        }
        
        logger.info("Simulation results:")
        for issue, success in results.items():
            logger.info(f"  {issue}: {'SUCCESS' if success else 'FAILED'}")
        
        return results


class Troubleshooter:
    """
    Provides troubleshooting utilities for the NYC TLC Data Platform
    """
    
    def __init__(self):
        """
        Initialize troubleshooter with connection details
        """
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.s3_raw_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
        self.s3_processed_bucket = os.getenv('S3_PROCESSED_BUCKET', 'nyc-tlc-processed-data-us-east-1')
        
        # PostgreSQL connection details
        user = os.getenv('POSTGRES_USER', 'nyc_tlc_user')
        password = os.getenv('POSTGRES_PASSWORD', 'nyc_tlc_password')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5433')
        database = os.getenv('POSTGRES_DB', 'nyc_tlc_dw')
        
        self.db_connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.db_connection_string)
    
    def check_s3_connectivity(self) -> bool:
        """
        Check if S3 buckets are accessible
        """
        logger.info("Checking S3 connectivity...")
        
        try:
            # Check raw bucket
            self.s3_client.head_bucket(Bucket=self.s3_raw_bucket)
            logger.info(f"Successfully connected to raw bucket: {self.s3_raw_bucket}")
            
            # Check processed bucket
            self.s3_client.head_bucket(Bucket=self.s3_processed_bucket)
            logger.info(f"Successfully connected to processed bucket: {self.s3_processed_bucket}")
            
            return True
        except ClientError as e:
            logger.error(f"S3 connectivity check failed: {e}")
            return False
    
    def check_db_connectivity(self) -> bool:
        """
        Check if database is accessible
        """
        logger.info("Checking database connectivity...")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT 1")
                if result.fetchone()[0] == 1:
                    logger.info("Database connectivity test passed")
                    return True
        except Exception as e:
            logger.error(f"Database connectivity check failed: {e}")
            return False
    
    def check_pipeline_status(self) -> Dict[str, Any]:
        """
        Check the status of recent pipeline runs
        """
        logger.info("Checking pipeline status...")
        
        try:
            with self.engine.connect() as conn:
                # Get recent pipeline runs
                query = """
                SELECT pipeline_name, status, COUNT(*) as count
                FROM taxi_data.pipeline_logs
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY pipeline_name, status
                ORDER BY pipeline_name, status
                """
                
                result = conn.execute(query)
                status_counts = {}
                
                for row in result:
                    pipeline_name, status, count = row
                    if pipeline_name not in status_counts:
                        status_counts[pipeline_name] = {}
                    status_counts[pipeline_name][status] = count
                
                logger.info("Pipeline status summary:")
                for pipeline, statuses in status_counts.items():
                    logger.info(f"  {pipeline}: {statuses}")
                
                return status_counts
                
        except Exception as e:
            logger.error(f"Error checking pipeline status: {e}")
            return {}
    
    def check_data_quality(self) -> Dict[str, Any]:
        """
        Check data quality metrics
        """
        logger.info("Checking data quality...")
        
        try:
            with self.engine.connect() as conn:
                # Check for common data quality issues
                issues = {}
                
                # Check for null pickup datetime
                null_pickup_query = """
                SELECT COUNT(*) as count
                FROM taxi_data.yellow_tripdata
                WHERE pickup_datetime IS NULL
                """
                result = conn.execute(null_pickup_query)
                issues['null_pickup_datetime'] = result.fetchone()['count']
                
                # Check for negative fares
                negative_fare_query = """
                SELECT COUNT(*) as count
                FROM taxi_data.yellow_tripdata
                WHERE fare_amount < 0
                """
                result = conn.execute(negative_fare_query)
                issues['negative_fares'] = result.fetchone()['count']
                
                # Check for invalid location IDs
                invalid_location_query = """
                SELECT COUNT(*) as count
                FROM taxi_data.yellow_tripdata
                WHERE pickup_location_id NOT IN (SELECT location_id FROM taxi_data.taxi_zone_lookup)
                OR dropoff_location_id NOT IN (SELECT location_id FROM taxi_data.taxi_zone_lookup)
                """
                result = conn.execute(invalid_location_query)
                issues['invalid_location_ids'] = result.fetchone()['count']
                
                logger.info("Data quality issues found:")
                for issue, count in issues.items():
                    if count > 0:
                        logger.warning(f"  {issue}: {count}")
                    else:
                        logger.info(f"  {issue}: {count}")
                
                return issues
                
        except Exception as e:
            logger.error(f"Error checking data quality: {e}")
            return {}
    
    def check_s3_data_flow(self) -> Dict[str, Any]:
        """
        Check if data is flowing properly through S3
        """
        logger.info("Checking S3 data flow...")
        
        try:
            # Check raw bucket for recent files
            raw_response = self.s3_client.list_objects_v2(
                Bucket=self.s3_raw_bucket,
                Prefix='taxi-data/yellow/',
                MaxKeys=10
            )
            
            raw_count = raw_response.get('KeyCount', 0)
            logger.info(f"Found {raw_count} files in raw bucket")
            
            # Check processed bucket for recent files
            processed_response = self.s3_client.list_objects_v2(
                Bucket=self.s3_processed_bucket,
                Prefix='taxi-data/yellow/',
                MaxKeys=10
            )
            
            processed_count = processed_response.get('KeyCount', 0)
            logger.info(f"Found {processed_count} files in processed bucket")
            
            return {
                'raw_files_count': raw_count,
                'processed_files_count': processed_count,
                'data_flow_status': 'OK' if raw_count > 0 and processed_count > 0 else 'ISSUE'
            }
            
        except ClientError as e:
            logger.error(f"Error checking S3 data flow: {e}")
            return {'error': str(e)}
    
    def run_troubleshooting_checks(self) -> Dict[str, Any]:
        """
        Run all troubleshooting checks
        """
        logger.info("Running comprehensive troubleshooting checks...")
        
        results = {
            's3_connectivity': self.check_s3_connectivity(),
            'db_connectivity': self.check_db_connectivity(),
            'pipeline_status': self.check_pipeline_status(),
            'data_quality': self.check_data_quality(),
            's3_data_flow': self.check_s3_data_flow()
        }
        
        logger.info("Troubleshooting results:")
        for check, result in results.items():
            logger.info(f"  {check}: {result}")
        
        return results


def main():
    """
    Main function to run issue simulation and troubleshooting
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Production Issue Simulation and Troubleshooting for NYC TLC Data Platform')
    parser.add_argument('action', choices=['simulate', 'troubleshoot', 'both'], 
                       help='Action to perform')
    
    args = parser.parse_args()
    
    if args.action in ['simulate', 'both']:
        logger.info("Starting production issue simulation...")
        simulator = ProductionIssueSimulator()
        simulation_results = simulator.run_all_simulations()
    
    if args.action in ['troubleshoot', 'both']:
        logger.info("Starting troubleshooting...")
        troubleshooter = Troubleshooter()
        troubleshooting_results = troubleshooter.run_troubleshooting_checks()
    
    logger.info("Issue simulation and troubleshooting completed!")


if __name__ == "__main__":
    main()