#!/usr/bin/env python3
"""
CloudWatch Monitoring Script for NYC TLC Data Platform

This script monitors AWS resources and provides performance insights
for the NYC TLC Data Platform infrastructure.
"""

import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import pandas as pd
from botocore.exceptions import ClientError


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cloudwatch_monitoring.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CloudWatchMonitor:
    """
    Monitors AWS resources using CloudWatch metrics
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize CloudWatch monitor
        
        Args:
            region_name: AWS region to monitor
        """
        self.region = region_name
        self.cloudwatch = boto3.client('cloudwatch', region_name=region_name)
        self.rds = boto3.client('rds', region_name=region_name)
        self.s3 = boto3.client('s3', region_name=region_name)
        self.ec2 = boto3.client('ec2', region_name=region_name)
        
        # Get resource identifiers from environment or use defaults
        self.rds_instance_id = 'nyc-tlc-datawarehouse'  # Replace with actual instance ID
        self.s3_buckets = [
            'nyc-tlc-raw-data',
            'nyc-tlc-processed-data'
        ]
    
    def get_rds_metrics(self, instance_id: str = None) -> Dict:
        """
        Get RDS performance metrics
        
        Args:
            instance_id: RDS instance identifier
            
        Returns:
            Dictionary with RDS metrics
        """
        if instance_id is None:
            instance_id = self.rds_instance_id
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)
        
        metrics = {}
        
        try:
            # CPU Utilization
            cpu_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5 minutes
                Statistics=['Average', 'Maximum']
            )
            metrics['cpu_utilization'] = cpu_response['Datapoints']
            
            # Database Connections
            conn_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='DatabaseConnections',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average', 'Maximum']
            )
            metrics['database_connections'] = conn_response['Datapoints']
            
            # Freeable Memory
            memory_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='FreeableMemory',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average', 'Minimum']
            )
            metrics['freeable_memory'] = memory_response['Datapoints']
            
            # Free Storage Space
            storage_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='FreeStorageSpace',
                Dimensions=[
                    {
                        'Name': 'DBInstanceIdentifier',
                        'Value': instance_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,
                Statistics=['Average', 'Minimum']
            )
            metrics['free_storage_space'] = storage_response['Datapoints']
            
            logger.info(f"Retrieved RDS metrics for {instance_id}")
            
        except ClientError as e:
            logger.error(f"Error retrieving RDS metrics: {e}")
        
        return metrics
    
    def get_s3_metrics(self, bucket_names: List[str] = None) -> Dict:
        """
        Get S3 bucket metrics
        
        Args:
            bucket_names: List of S3 bucket names to monitor
            
        Returns:
            Dictionary with S3 metrics
        """
        if bucket_names is None:
            bucket_names = self.s3_buckets
        
        metrics = {}
        
        for bucket_name in bucket_names:
            try:
                # Bucket size
                size_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {
                            'Name': 'BucketName',
                            'Value': bucket_name
                        },
                        {
                            'Name': 'StorageType',
                            'Value': 'StandardStorage'
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(days=7),
                    EndTime=datetime.utcnow(),
                    Period=86400,  # 24 hours
                    Statistics=['Average']
                )
                metrics[f'{bucket_name}_size'] = size_response['Datapoints']
                
                # Number of objects
                objects_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {
                            'Name': 'BucketName',
                            'Value': bucket_name
                        },
                        {
                            'Name': 'StorageType',
                            'Value': 'AllStorageTypes'
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(days=7),
                    EndTime=datetime.utcnow(),
                    Period=86400,
                    Statistics=['Average']
                )
                metrics[f'{bucket_name}_objects'] = objects_response['Datapoints']
                
                logger.info(f"Retrieved S3 metrics for {bucket_name}")
                
            except ClientError as e:
                logger.error(f"Error retrieving S3 metrics for {bucket_name}: {e}")
        
        return metrics
    
    def get_ec2_metrics(self, instance_ids: List[str]) -> Dict:
        """
        Get EC2 instance metrics
        
        Args:
            instance_ids: List of EC2 instance IDs to monitor
            
        Returns:
            Dictionary with EC2 metrics
        """
        metrics = {}
        
        for instance_id in instance_ids:
            try:
                # CPU Utilization
                cpu_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/EC2',
                    MetricName='CPUUtilization',
                    Dimensions=[
                        {
                            'Name': 'InstanceId',
                            'Value': instance_id
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(hours=1),
                    EndTime=datetime.utcnow(),
                    Period=300,
                    Statistics=['Average', 'Maximum']
                )
                metrics[f'{instance_id}_cpu'] = cpu_response['Datapoints']
                
                # Network In/Out
                for metric_name in ['NetworkIn', 'NetworkOut']:
                    network_response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/EC2',
                        MetricName=metric_name,
                        Dimensions=[
                            {
                                'Name': 'InstanceId',
                                'Value': instance_id
                            }
                        ],
                        StartTime=datetime.utcnow() - timedelta(hours=1),
                        EndTime=datetime.utcnow(),
                        Period=300,
                        Statistics=['Sum']
                    )
                    metrics[f'{instance_id}_{metric_name.lower()}'] = network_response['Datapoints']
                
                logger.info(f"Retrieved EC2 metrics for {instance_id}")
                
            except ClientError as e:
                logger.error(f"Error retrieving EC2 metrics for {instance_id}: {e}")
        
        return metrics
    
    def get_lambda_metrics(self, function_names: List[str]) -> Dict:
        """
        Get Lambda function metrics
        
        Args:
            function_names: List of Lambda function names to monitor
            
        Returns:
            Dictionary with Lambda metrics
        """
        metrics = {}
        
        for function_name in function_names:
            try:
                # Invocations
                invocations_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Invocations',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': function_name
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(hours=1),
                    EndTime=datetime.utcnow(),
                    Period=300,
                    Statistics=['Sum']
                )
                metrics[f'{function_name}_invocations'] = invocations_response['Datapoints']
                
                # Duration
                duration_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Duration',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': function_name
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(hours=1),
                    EndTime=datetime.utcnow(),
                    Period=300,
                    Statistics=['Average', 'Maximum']
                )
                metrics[f'{function_name}_duration'] = duration_response['Datapoints']
                
                # Errors
                errors_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Errors',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': function_name
                        }
                    ],
                    StartTime=datetime.utcnow() - timedelta(hours=1),
                    EndTime=datetime.utcnow(),
                    Period=300,
                    Statistics=['Sum']
                )
                metrics[f'{function_name}_errors'] = errors_response['Datapoints']
                
                logger.info(f"Retrieved Lambda metrics for {function_name}")
                
            except ClientError as e:
                logger.error(f"Error retrieving Lambda metrics for {function_name}: {e}")
        
        return metrics
    
    def generate_performance_report(self) -> Dict:
        """
        Generate a comprehensive performance report
        
        Returns:
            Dictionary with performance metrics and recommendations
        """
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'rds_metrics': self.get_rds_metrics(),
            's3_metrics': self.get_s3_metrics(),
            'recommendations': []
        }
        
        # Analyze RDS metrics for recommendations
        rds_metrics = report['rds_metrics']
        if 'cpu_utilization' in rds_metrics and rds_metrics['cpu_utilization']:
            latest_cpu = max(rds_metrics['cpu_utilization'], key=lambda x: x['Timestamp'])
            if latest_cpu['Average'] > 80:
                report['recommendations'].append({
                    'resource': 'RDS',
                    'issue': 'High CPU utilization',
                    'severity': 'high',
                    'recommendation': 'Consider scaling up RDS instance or optimizing queries'
                })
        
        if 'freeable_memory' in rds_metrics and rds_metrics['freeable_memory']:
            latest_memory = min(rds_metrics['freeable_memory'], key=lambda x: x['Timestamp'])
            if latest_memory['Minimum'] < 100 * 1024 * 1024:  # Less than 100MB
                report['recommendations'].append({
                    'resource': 'RDS',
                    'issue': 'Low freeable memory',
                    'severity': 'high',
                    'recommendation': 'Consider scaling up RDS instance memory'
                })
        
        # Analyze S3 metrics for recommendations
        s3_metrics = report['s3_metrics']
        for bucket_key, datapoints in s3_metrics.items():
            if 'size' in bucket_key and datapoints:
                latest_size = max(datapoints, key=lambda x: x['Timestamp'])
                if latest_size['Average'] > 100 * 1024 * 1024 * 1024:  # More than 100GB
                    report['recommendations'].append({
                        'resource': 'S3',
                        'issue': f'Large bucket size: {bucket_key}',
                        'severity': 'medium',
                        'recommendation': 'Consider implementing lifecycle policies for old data'
                    })
        
        logger.info("Generated performance report with recommendations")
        return report
    
    def create_custom_metrics_namespace(self):
        """
        Create a custom namespace for application-specific metrics
        """
        try:
            # Put a dummy metric to create the namespace
            self.cloudwatch.put_metric_data(
                Namespace='NYC-TLC-Data-Platform',
                MetricData=[
                    {
                        'MetricName': 'DataProcessingVolume',
                        'Value': 0,
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
            logger.info("Created custom CloudWatch namespace: NYC-TLC-Data-Platform")
        except ClientError as e:
            logger.error(f"Error creating custom namespace: {e}")
    
    def put_custom_metric(self, metric_name: str, value: float, 
                         dimensions: List[Dict] = None, unit: str = 'Count'):
        """
        Put a custom metric to CloudWatch
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            dimensions: List of dimensions for the metric
            unit: Unit of the metric
        """
        if dimensions is None:
            dimensions = [{'Name': 'Service', 'Value': 'DataPlatform'}]
        
        try:
            self.cloudwatch.put_metric_data(
                Namespace='NYC-TLC-Data-Platform',
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': datetime.utcnow(),
                        'Dimensions': dimensions
                    }
                ]
            )
            logger.info(f"Posted custom metric: {metric_name} = {value}")
        except ClientError as e:
            logger.error(f"Error posting custom metric {metric_name}: {e}")


def main():
    """
    Main function to run CloudWatch monitoring
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='CloudWatch Monitoring for NYC TLC Data Platform')
    parser.add_argument('--region', default='us-east-1', help='AWS region to monitor')
    parser.add_argument('--report', action='store_true', help='Generate performance report')
    parser.add_argument('--rds-instance', help='RDS instance identifier')
    parser.add_argument('--s3-buckets', nargs='+', help='S3 bucket names to monitor')
    
    args = parser.parse_args()
    
    monitor = CloudWatchMonitor(region_name=args.region)
    
    if args.rds_instance:
        monitor.rds_instance_id = args.rds_instance
    
    if args.s3_buckets:
        monitor.s3_buckets = args.s3_buckets
    
    if args.report:
        report = monitor.generate_performance_report()
        print(json.dumps(report, indent=2, default=str))
    else:
        # Just run basic monitoring
        rds_metrics = monitor.get_rds_metrics()
        s3_metrics = monitor.get_s3_metrics()
        
        print(f"Retrieved metrics for {len(rds_metrics)} RDS metrics and {len(s3_metrics)} S3 metrics")


if __name__ == "__main__":
    main()