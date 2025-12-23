"""
S3 Operations Module for NYC TLC Data Platform

This module provides functions for interacting with AWS S3
for storing and retrieving NYC TLC taxi trip data.
"""

import boto3
import logging
from botocore.exceptions import ClientError
from typing import List, Optional
import os
from datetime import datetime


class S3Manager:
    """
    Manages S3 operations for the NYC TLC Data Platform
    """

    def __init__(self, aws_region: str = 'us-east-1'):
        """
        Initialize S3 manager with AWS credentials

        Args:
            aws_region: AWS region for S3 operations
        """
        self.region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.logger = logging.getLogger(__name__)

    def upload_file(self, file_path: str, bucket: str, object_name: Optional[str] = None) -> bool:
        """
        Upload a file to an S3 bucket

        Args:
            file_path: File to upload
            bucket: Bucket to upload to
            object_name: S3 object name. If not specified, file_path name is used

        Returns:
            True if file was uploaded, else False
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.s3_client.upload_file(file_path, bucket, object_name)
            self.logger.info(f"Uploaded {file_path} to {bucket}/{object_name}")
            return True
        except ClientError as e:
            self.logger.error(f"Error uploading {file_path} to {bucket}: {e}")
            return False

    def download_file(self, bucket: str, object_name: str, file_path: str) -> bool:
        """
        Download a file from an S3 bucket

        Args:
            bucket: Bucket to download from
            object_name: S3 object name
            file_path: Local file path to save to

        Returns:
            True if file was downloaded, else False
        """
        try:
            self.s3_client.download_file(bucket, object_name, file_path)
            self.logger.info(f"Downloaded {bucket}/{object_name} to {file_path}")
            return True
        except ClientError as e:
            self.logger.error(f"Error downloading {bucket}/{object_name}: {e}")
            return False

    def list_objects(self, bucket: str, prefix: str = '') -> List[str]:
        """
        List objects in an S3 bucket with optional prefix

        Args:
            bucket: Bucket name
            prefix: Prefix to filter objects

        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            self.logger.error(f"Error listing objects in {bucket}: {e}")
            return []

    def object_exists(self, bucket: str, object_name: str) -> bool:
        """
        Check if an object exists in S3 bucket

        Args:
            bucket: Bucket name
            object_name: Object key

        Returns:
            True if object exists, else False
        """
        try:
            self.s3_client.head_object(Bucket=bucket, Key=object_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                self.logger.error(f"Error checking existence of {bucket}/{object_name}: {e}")
                return False

    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create an S3 bucket

        Args:
            bucket_name: Name of the bucket to create

        Returns:
            True if bucket was created, else False
        """
        try:
            if self.region == 'us-east-1':
                # us-east-1 doesn't accept LocationConstraint
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region}
                )
            self.logger.info(f"Created bucket {bucket_name}")
            return True
        except ClientError as e:
            self.logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False

    def set_bucket_lifecycle(self, bucket_name: str, days_to_archive: int = 30, days_to_delete: int = 365):
        """
        Set lifecycle policy on bucket to manage costs

        Args:
            bucket_name: Name of the bucket
            days_to_archive: Days after which objects move to IA
            days_to_delete: Days after which objects are deleted
        """
        lifecycle_config = {
            'Rules': [
                {
                    'ID': 'ArchiveOldObjects',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Transitions': [
                        {
                            'Days': days_to_archive,
                            'StorageClass': 'STANDARD_IA'
                        }
                    ],
                    'Expiration': {
                        'Days': days_to_delete
                    }
                }
            ]
        }

        try:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            self.logger.info(f"Set lifecycle policy for {bucket_name}")
        except ClientError as e:
            self.logger.error(f"Error setting lifecycle policy for {bucket_name}: {e}")

    def sync_directory_to_s3(self, local_dir: str, bucket: str, s3_prefix: str = '') -> bool:
        """
        Sync a local directory to S3 bucket

        Args:
            local_dir: Local directory to sync
            bucket: Destination S3 bucket
            s3_prefix: S3 prefix to prepend to object keys

        Returns:
            True if sync completed successfully, else False
        """
        try:
            import os
            for root, dirs, files in os.walk(local_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, local_dir)
                    s3_key = f"{s3_prefix}{relative_path}".replace("\\", "/")  # Handle Windows paths

                    if self.upload_file(local_path, bucket, s3_key):
                        self.logger.info(f"Synced {local_path} to s3://{bucket}/{s3_key}")
                    else:
                        return False
            return True
        except Exception as e:
            self.logger.error(f"Error syncing directory {local_dir} to S3: {e}")
            return False


def get_s3_buckets_for_env() -> tuple:
    """
    Get S3 bucket names from environment variables or return defaults
    """
    raw_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
    processed_bucket = os.getenv('S3_PROCESSED_BUCKET', 'nyc-tlc-processed-data-us-east-1')

    return raw_bucket, processed_bucket


if __name__ == "__main__":
    # Example usage
    s3_manager = S3Manager()

    # Get bucket names from environment
    raw_bucket, processed_bucket = get_s3_buckets_for_env()

    print(f"Raw data bucket: {raw_bucket}")
    print(f"Processed data bucket: {processed_bucket}")

    # List objects in raw bucket
    objects = s3_manager.list_objects(raw_bucket, 'taxi-data/')
    print(f"Found {len(objects)} objects in raw bucket with prefix 'taxi-data/'")