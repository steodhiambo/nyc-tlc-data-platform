#!/usr/bin/env python3
"""
S3 Lifecycle Management and Glue Partitioning Script

This script manages S3 lifecycle policies for archiving old Parquet files
and handles AWS Glue partitioning for the NYC TLC Data Platform.
"""

import boto3
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
import time
from botocore.exceptions import ClientError


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_lifecycle_management.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class S3LifecycleManager:
    """
    Manages S3 lifecycle policies for cost optimization
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize S3 lifecycle manager
        
        Args:
            region_name: AWS region
        """
        self.region = region_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.glue_client = boto3.client('glue', region_name=region_name)
    
    def create_lifecycle_policy(self, bucket_name: str, policy_config: Dict):
        """
        Create or update S3 lifecycle policy for a bucket
        
        Args:
            bucket_name: Name of the S3 bucket
            policy_config: Configuration for lifecycle policy
        """
        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=policy_config
            )
            logger.info(f"Lifecycle policy updated for bucket: {bucket_name}")
        except ClientError as e:
            logger.error(f"Error updating lifecycle policy for {bucket_name}: {e}")
            raise
    
    def get_default_lifecycle_policy(self) -> Dict:
        """
        Get default lifecycle policy for NYC TLC data
        
        Returns:
            Lifecycle policy configuration
        """
        return {
            "Rules": [
                {
                    "ID": "ArchiveOldVersions",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "taxi-data/"},
                    "NoncurrentVersionTransitions": [
                        {
                            "NoncurrentDays": 30,
                            "StorageClass": "STANDARD_IA"
                        },
                        {
                            "NoncurrentDays": 90,
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": 365
                    },
                    "Transitions": [
                        {
                            "Days": 30,
                            "StorageClass": "STANDARD_IA"
                        },
                        {
                            "Days": 90,
                            "StorageClass": "GLACIER"
                        },
                        {
                            "Days": 365,
                            "StorageClass": "DEEP_ARCHIVE"
                        }
                    ],
                    "Expiration": {
                        "Days": 1825  # 5 years
                    }
                },
                {
                    "ID": "AccessLogLifecycle",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "logs/"},
                    "Transitions": [
                        {
                            "Days": 7,
                            "StorageClass": "STANDARD_IA"
                        },
                        {
                            "Days": 30,
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "Expiration": {
                        "Days": 90
                    }
                }
            ]
        }
    
    def apply_lifecycle_to_buckets(self, bucket_names: List[str]):
        """
        Apply default lifecycle policy to multiple buckets
        
        Args:
            bucket_names: List of bucket names to apply policy to
        """
        policy = self.get_default_lifecycle_policy()
        
        for bucket_name in bucket_names:
            try:
                self.create_lifecycle_policy(bucket_name, policy)
                logger.info(f"Applied lifecycle policy to {bucket_name}")
            except Exception as e:
                logger.error(f"Failed to apply lifecycle policy to {bucket_name}: {e}")
    
    def get_bucket_inventory_config(self, bucket_name: str) -> Dict:
        """
        Get bucket inventory configuration for cost analysis
        
        Args:
            bucket_name: Name of the S3 bucket
            
        Returns:
            Inventory configuration
        """
        return {
            "Destination": {
                "S3BucketDestination": {
                    "Bucket": f"arn:aws:s3:::{bucket_name}-inventory-reports",
                    "Format": "CSV",
                    "Prefix": "inventory-reports"
                }
            },
            "IsEnabled": True,
            "Id": f"{bucket_name}-inventory",
            "IncludedObjectVersions": "All",
            "Schedule": {
                "Frequency": "Weekly"
            },
            "OptionalFields": [
                "Size",
                "LastModifiedDate",
                "StorageClass",
                "ETag",
                "IsMultipartUploaded",
                "ReplicationStatus",
                "EncryptionStatus",
                "ObjectLockRetainUntilDate",
                "ObjectLockMode",
                "ObjectLockLegalHoldStatus",
                "IntelligentTieringAccessTier"
            ]
        }
    
    def setup_inventory_reports(self, bucket_names: List[str]):
        """
        Set up inventory reports for buckets to track storage usage
        
        Args:
            bucket_names: List of bucket names to set up inventory for
        """
        for bucket_name in bucket_names:
            try:
                # Create a separate bucket for inventory reports if it doesn't exist
                inventory_bucket = f"{bucket_name}-inventory-reports"
                try:
                    self.s3_client.create_bucket(
                        Bucket=inventory_bucket,
                        CreateBucketConfiguration={'LocationConstraint': self.region}
                    )
                    logger.info(f"Created inventory bucket: {inventory_bucket}")
                except ClientError as e:
                    if e.response['Error']['Code'] != 'BucketAlreadyExists':
                        logger.error(f"Error creating inventory bucket {inventory_bucket}: {e}")
                
                # Enable versioning on inventory bucket
                self.s3_client.put_bucket_versioning(
                    Bucket=inventory_bucket,
                    VersioningConfiguration={'Status': 'Enabled'}
                )
                
                # Configure inventory for the original bucket
                inventory_config = self.get_bucket_inventory_config(bucket_name)
                
                self.s3_client.put_bucket_inventory_configuration(
                    Bucket=bucket_name,
                    Id=f"{bucket_name}-inventory",
                    InventoryConfiguration=inventory_config
                )
                
                logger.info(f"Set up inventory reports for {bucket_name}")
            except ClientError as e:
                logger.error(f"Error setting up inventory for {bucket_name}: {e}")


class GluePartitionManager:
    """
    Manages AWS Glue partitions for data catalog optimization
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize Glue partition manager
        
        Args:
            region_name: AWS region
        """
        self.region = region_name
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
    
    def create_database_if_not_exists(self, database_name: str, description: str = ""):
        """
        Create Glue database if it doesn't exist
        
        Args:
            database_name: Name of the database
            description: Description of the database
        """
        try:
            self.glue_client.create_database(
                DatabaseInput={
                    'Name': database_name,
                    'Description': description
                }
            )
            logger.info(f"Created Glue database: {database_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.info(f"Database {database_name} already exists")
            else:
                logger.error(f"Error creating database {database_name}: {e}")
    
    def create_table_if_not_exists(self, database_name: str, table_name: str, 
                                 location: str, columns: List[Dict]):
        """
        Create Glue table if it doesn't exist
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            location: S3 location of the data
            columns: List of column definitions
        """
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                },
                'Parameters': {
                    'classification': 'parquet',
                    'compressionType': 'snappy'
                }
            },
            'PartitionKeys': [
                {'Name': 'year', 'Type': 'string'},
                {'Name': 'month', 'Type': 'string'}
            ],
            'TableType': 'EXTERNAL_TABLE'
        }
        
        try:
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            logger.info(f"Created Glue table: {database_name}.{table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.info(f"Table {database_name}.{table_name} already exists")
            else:
                logger.error(f"Error creating table {database_name}.{table_name}: {e}")
    
    def add_partition(self, database_name: str, table_name: str, 
                     year: str, month: str, location: str):
        """
        Add a partition to a Glue table
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            year: Year partition value
            month: Month partition value
            location: S3 location for this partition
        """
        partition_input = {
            'Values': [year, month],
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'vendor_id', 'Type': 'int'},
                    {'Name': 'pickup_datetime', 'Type': 'timestamp'},
                    {'Name': 'dropoff_datetime', 'Type': 'timestamp'},
                    {'Name': 'passenger_count', 'Type': 'int'},
                    {'Name': 'trip_distance', 'Type': 'double'},
                    {'Name': 'pickup_location_id', 'Type': 'int'},
                    {'Name': 'dropoff_location_id', 'Type': 'int'},
                    {'Name': 'payment_type', 'Type': 'int'},
                    {'Name': 'fare_amount', 'Type': 'double'},
                    {'Name': 'total_amount', 'Type': 'double'}
                ],
                'Location': location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            }
        }
        
        try:
            self.glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput=partition_input
            )
            logger.info(f"Added partition {year}-{month} to {database_name}.{table_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.info(f"Partition {year}-{month} already exists in {database_name}.{table_name}")
            else:
                logger.error(f"Error adding partition {year}-{month} to {database_name}.{table_name}: {e}")
    
    def batch_add_partitions(self, database_name: str, table_name: str, 
                           partitions: List[Dict[str, str]]):
        """
        Add multiple partitions to a Glue table in batch
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            partitions: List of partition dictionaries with 'year', 'month', and 'location'
        """
        partition_inputs = []
        
        for partition in partitions:
            partition_input = {
                'Values': [partition['year'], partition['month']],
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'vendor_id', 'Type': 'int'},
                        {'Name': 'pickup_datetime', 'Type': 'timestamp'},
                        {'Name': 'dropoff_datetime', 'Type': 'timestamp'},
                        {'Name': 'passenger_count', 'Type': 'int'},
                        {'Name': 'trip_distance', 'Type': 'double'},
                        {'Name': 'pickup_location_id', 'Type': 'int'},
                        {'Name': 'dropoff_location_id', 'Type': 'int'},
                        {'Name': 'payment_type', 'Type': 'int'},
                        {'Name': 'fare_amount', 'Type': 'double'},
                        {'Name': 'total_amount', 'Type': 'double'}
                    ],
                    'Location': partition['location'],
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
            partition_inputs.append(partition_input)
        
        # Process in batches of 100 (Glue limit)
        batch_size = 100
        for i in range(0, len(partition_inputs), batch_size):
            batch = partition_inputs[i:i + batch_size]
            try:
                response = self.glue_client.batch_create_partition(
                    DatabaseName=database_name,
                    TableName=table_name,
                    PartitionInputList=batch
                )
                logger.info(f"Added batch of {len(batch)} partitions to {database_name}.{table_name}")
                
                # Log any errors
                if 'Errors' in response and response['Errors']:
                    for error in response['Errors']:
                        logger.error(f"Partition error: {error}")
            except ClientError as e:
                logger.error(f"Error in batch partition creation: {e}")
    
    def update_partition_locations(self, database_name: str, table_name: str, 
                                 year: str, month: str, new_location: str):
        """
        Update the location of an existing partition
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            year: Year partition value
            month: Month partition value
            new_location: New S3 location for the partition
        """
        partition_input = {
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'vendor_id', 'Type': 'int'},
                    {'Name': 'pickup_datetime', 'Type': 'timestamp'},
                    {'Name': 'dropoff_datetime', 'Type': 'timestamp'},
                    {'Name': 'passenger_count', 'Type': 'int'},
                    {'Name': 'trip_distance', 'Type': 'double'},
                    {'Name': 'pickup_location_id', 'Type': 'int'},
                    {'Name': 'dropoff_location_id', 'Type': 'int'},
                    {'Name': 'payment_type', 'Type': 'int'},
                    {'Name': 'fare_amount', 'Type': 'double'},
                    {'Name': 'total_amount', 'Type': 'double'}
                ],
                'Location': new_location,
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                }
            }
        }
        
        try:
            self.glue_client.update_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionValueList=[year, month],
                PartitionInput=partition_input
            )
            logger.info(f"Updated partition location for {year}-{month} in {database_name}.{table_name}")
        except ClientError as e:
            logger.error(f"Error updating partition location for {year}-{month}: {e}")
    
    def sync_partitions_with_s3(self, database_name: str, table_name: str, 
                               s3_bucket: str, s3_prefix: str):
        """
        Sync Glue partitions with S3 directory structure
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            s3_bucket: S3 bucket containing the data
            s3_prefix: S3 prefix to scan for partitions
        """
        try:
            # List all prefixes under the given path to find year/month partitions
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=s3_bucket,
                Prefix=s3_prefix,
                Delimiter='/'
            )
            
            partitions_to_add = []
            
            for page in pages:
                if 'CommonPrefixes' in page:
                    for prefix in page['CommonPrefixes']:
                        # Extract year and month from path like taxi-data/yellow/2023/01/
                        path_parts = prefix['Prefix'].rstrip('/').split('/')
                        if len(path_parts) >= 4:  # expecting taxi-data/type/year/month
                            year = path_parts[-2]
                            month = path_parts[-1]
                            
                            # Validate year and month format
                            try:
                                datetime(int(year), int(month), 1)
                                location = f"s3://{s3_bucket}/{prefix['Prefix']}"
                                
                                partitions_to_add.append({
                                    'year': year,
                                    'month': month,
                                    'location': location
                                })
                            except ValueError:
                                logger.warning(f"Invalid year/month format: {year}/{month}")
            
            # Add all discovered partitions
            if partitions_to_add:
                logger.info(f"Discovered {len(partitions_to_add)} partitions to sync")
                self.batch_add_partitions(database_name, table_name, partitions_to_add)
            else:
                logger.info("No new partitions found to sync")
                
        except ClientError as e:
            logger.error(f"Error syncing partitions with S3: {e}")


def main():
    """
    Main function to run S3 lifecycle and Glue partitioning
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='S3 Lifecycle and Glue Partitioning for NYC TLC Data Platform')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--setup-lifecycle', action='store_true', help='Set up S3 lifecycle policies')
    parser.add_argument('--setup-inventory', action='store_true', help='Set up S3 inventory reports')
    parser.add_argument('--sync-partitions', action='store_true', help='Sync Glue partitions with S3')
    parser.add_argument('--buckets', nargs='+', help='S3 bucket names to manage')
    parser.add_argument('--database', default='nyc_tlc_data', help='Glue database name')
    parser.add_argument('--table', default='taxi_trips', help='Glue table name')
    parser.add_argument('--s3-bucket', help='S3 bucket for Glue sync')
    parser.add_argument('--s3-prefix', default='taxi-data/', help='S3 prefix for Glue sync')
    
    args = parser.parse_args()
    
    # Initialize managers
    lifecycle_manager = S3LifecycleManager(region_name=args.region)
    partition_manager = GluePartitionManager(region_name=args.region)
    
    # Set up lifecycle policies
    if args.setup_lifecycle and args.buckets:
        lifecycle_manager.apply_lifecycle_to_buckets(args.buckets)
    
    # Set up inventory reports
    if args.setup_inventory and args.buckets:
        lifecycle_manager.setup_inventory_reports(args.buckets)
    
    # Sync partitions
    if args.sync_partitions and args.s3_bucket:
        partition_manager.create_database_if_not_exists(
            args.database, 
            "NYC TLC Taxi Trip Data"
        )
        
        # Define table columns
        columns = [
            {'Name': 'vendor_id', 'Type': 'int'},
            {'Name': 'pickup_datetime', 'Type': 'timestamp'},
            {'Name': 'dropoff_datetime', 'Type': 'timestamp'},
            {'Name': 'passenger_count', 'Type': 'int'},
            {'Name': 'trip_distance', 'Type': 'double'},
            {'Name': 'pickup_location_id', 'Type': 'int'},
            {'Name': 'dropoff_location_id', 'Type': 'int'},
            {'Name': 'payment_type', 'Type': 'int'},
            {'Name': 'fare_amount', 'Type': 'double'},
            {'Name': 'total_amount', 'Type': 'double'}
        ]
        
        partition_manager.create_table_if_not_exists(
            args.database,
            args.table,
            f"s3://{args.s3_bucket}/{args.s3_prefix}",
            columns
        )
        
        # Sync partitions
        partition_manager.sync_partitions_with_s3(
            args.database,
            args.table,
            args.s3_bucket,
            args.s3_prefix
        )


if __name__ == "__main__":
    main()