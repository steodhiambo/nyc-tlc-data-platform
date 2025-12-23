#!/usr/bin/env python3
"""
CloudWatch Metrics Configuration Script

This script sets up CloudWatch monitoring for AWS resources used in the NYC TLC Data Platform:
- S3 buckets
- RDS PostgreSQL instance
- EC2 instances (if applicable)
"""

import boto3
import json
import logging
from botocore.exceptions import ClientError
import os
from datetime import datetime, timedelta


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cloudwatch_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CloudWatchSetup:
    """
    Sets up CloudWatch monitoring for AWS resources
    """
    
    def __init__(self):
        """
        Initialize CloudWatch client
        """
        self.cloudwatch = boto3.client('cloudwatch', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.rds_client = boto3.client('rds', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        
        # Get bucket names from environment
        self.raw_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
        self.processed_bucket = os.getenv('S3_PROCESSED_BUCKET', 'nyc-tlc-processed-data-us-east-1')
        self.db_instance_identifier = os.getenv('RDS_INSTANCE_IDENTIFIER', 'nyc-tlc-datawarehouse')
    
    def setup_s3_monitoring(self):
        """
        Set up CloudWatch alarms for S3 buckets
        """
        logger.info("Setting up S3 monitoring...")
        
        # S3 bucket size monitoring
        try:
            # Raw bucket size alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'S3-{self.raw_bucket}-Size-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='BucketSizeBytes',
                Namespace='AWS/S3',
                Period=86400,  # 24 hours
                Statistic='Average',
                Threshold=1000000000000,  # 1TB
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.raw_bucket} size exceeds 1TB',
                Unit='Bytes',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': self.raw_bucket
                    },
                    {
                        'Name': 'StorageType',
                        'Value': 'StandardStorage'
                    }
                ]
            )
            logger.info(f"Created size alarm for raw bucket: {self.raw_bucket}")
            
            # Processed bucket size alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'S3-{self.processed_bucket}-Size-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='BucketSizeBytes',
                Namespace='AWS/S3',
                Period=86400,  # 24 hours
                Statistic='Average',
                Threshold=500000000000,  # 500GB
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.processed_bucket} size exceeds 500GB',
                Unit='Bytes',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': self.processed_bucket
                    },
                    {
                        'Name': 'StorageType',
                        'Value': 'StandardStorage'
                    }
                ]
            )
            logger.info(f"Created size alarm for processed bucket: {self.processed_bucket}")
            
            # Request error rate alarm for raw bucket
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'S3-{self.raw_bucket}-ErrorRate-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='4xxErrors',
                Namespace='AWS/S3',
                Period=300,  # 5 minutes
                Statistic='Sum',
                Threshold=10,
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.raw_bucket} has more than 10 4xx errors in 5 minutes',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': self.raw_bucket
                    }
                ]
            )
            logger.info(f"Created error rate alarm for raw bucket: {self.raw_bucket}")
            
        except ClientError as e:
            logger.error(f"Error setting up S3 monitoring: {e}")
    
    def setup_rds_monitoring(self):
        """
        Set up CloudWatch alarms for RDS PostgreSQL instance
        """
        logger.info("Setting up RDS monitoring...")
        
        try:
            # CPU utilization alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'RDS-{self.db_instance_identifier}-CPU-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='CPUUtilization',
                Namespace='AWS/RDS',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=80.0,
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.db_instance_identifier} CPU exceeds 80%',
                Unit='Percent',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_identifier
                    }
                ]
            )
            logger.info(f"Created CPU alarm for RDS instance: {self.db_instance_identifier}")
            
            # Freeable memory alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'RDS-{self.db_instance_identifier}-Memory-Alarm',
                ComparisonOperator='LessThanThreshold',
                EvaluationPeriods=2,
                MetricName='FreeableMemory',
                Namespace='AWS/RDS',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=104857600,  # 100MB in bytes
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.db_instance_identifier} freeable memory drops below 100MB',
                Unit='Bytes',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_identifier
                    }
                ]
            )
            logger.info(f"Created memory alarm for RDS instance: {self.db_instance_identifier}")
            
            # Free storage space alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'RDS-{self.db_instance_identifier}-Storage-Alarm',
                ComparisonOperator='LessThanThreshold',
                EvaluationPeriods=2,
                MetricName='FreeStorageSpace',
                Namespace='AWS/RDS',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=5368709120,  # 5GB in bytes
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.db_instance_identifier} free storage drops below 5GB',
                Unit='Bytes',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_identifier
                    }
                ]
            )
            logger.info(f"Created storage alarm for RDS instance: {self.db_instance_identifier}")
            
            # Database connections alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'RDS-{self.db_instance_identifier}-Connections-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='DatabaseConnections',
                Namespace='AWS/RDS',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=100,
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.db_instance_identifier} connections exceed 100',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_identifier
                    }
                ]
            )
            logger.info(f"Created connections alarm for RDS instance: {self.db_instance_identifier}")
            
            # Read IOPS alarm
            self.cloudwatch.put_metric_alarm(
                AlarmName=f'RDS-{self.db_instance_identifier}-ReadIOPS-Alarm',
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='ReadIOPS',
                Namespace='AWS/RDS',
                Period=300,  # 5 minutes
                Statistic='Average',
                Threshold=1000,
                ActionsEnabled=True,
                AlarmDescription=f'Alarm when {self.db_instance_identifier} read IOPS exceeds 1000',
                Unit='Count/Second',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': self.db_instance_identifier
                    }
                ]
            )
            logger.info(f"Created read IOPS alarm for RDS instance: {self.db_instance_identifier}")
            
        except ClientError as e:
            logger.error(f"Error setting up RDS monitoring: {e}")
    
    def setup_custom_metrics_namespace(self):
        """
        Create a custom namespace for application-specific metrics
        """
        logger.info("Setting up custom metrics namespace...")
        
        try:
            # Example: Put a dummy metric to create the namespace
            self.cloudwatch.put_metric_data(
                Namespace='NYC-TLC-Data-Platform',
                MetricData=[
                    {
                        'MetricName': 'PipelineHealth',
                        'Value': 1.0,
                        'Unit': 'Count',
                        'Timestamp': datetime.utcnow(),
                        'Dimensions': [
                            {
                                'Name': 'PipelineName',
                                'Value': 'ExamplePipeline'
                            }
                        ]
                    }
                ]
            )
            logger.info("Created custom metrics namespace: NYC-TLC-Data-Platform")
        except ClientError as e:
            logger.error(f"Error setting up custom metrics namespace: {e}")
    
    def setup_s3_lifecycle_notifications(self):
        """
        Set up S3 lifecycle notifications to CloudWatch
        """
        logger.info("Setting up S3 lifecycle notifications...")
        
        try:
            # Configure raw bucket for event notifications
            self.s3_client.put_bucket_notification_configuration(
                Bucket=self.raw_bucket,
                NotificationConfiguration={
                    'CloudWatchConfiguration': {
                        'CloudWatchConfiguration': {
                            'LogGroupName': f'nyc-tlc-{self.raw_bucket}-logs'
                        }
                    }
                }
            )
            logger.info(f"Configured lifecycle notifications for raw bucket: {self.raw_bucket}")
        except ClientError as e:
            logger.error(f"Error setting up S3 lifecycle notifications: {e}")
    
    def run_setup(self):
        """
        Run the complete CloudWatch setup
        """
        logger.info("Starting CloudWatch setup for NYC TLC Data Platform...")
        
        self.setup_s3_monitoring()
        self.setup_rds_monitoring()
        self.setup_custom_metrics_namespace()
        
        logger.info("CloudWatch setup completed!")


def main():
    """
    Main function to run the CloudWatch setup
    """
    logger.info("Starting CloudWatch configuration...")
    
    # Check if AWS credentials are available
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        logger.error("AWS credentials not found in environment variables")
        return
    
    # Initialize and run setup
    cw_setup = CloudWatchSetup()
    cw_setup.run_setup()


if __name__ == "__main__":
    main()