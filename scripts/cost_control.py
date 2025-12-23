#!/usr/bin/env python3
"""
Cost Control Strategies for NYC TLC Data Platform

This script implements various cost control strategies including:
- AWS Budgets and Billing alerts
- Spot instance usage for processing
- Lambda functions for cost-effective processing
- RDS auto-pause configuration
"""

import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import time
from botocore.exceptions import ClientError


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cost_control.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AWSCostController:
    """
    Implements cost control strategies for AWS resources
    """
    
    def __init__(self, region_name: str = 'us-east-1'):
        """
        Initialize cost controller
        
        Args:
            region_name: AWS region
        """
        self.region = region_name
        self.budgets_client = boto3.client('budgets', region_name=region_name)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region_name)
        self.ec2_client = boto3.client('ec2', region_name=region_name)
        self.rds_client = boto3.client('rds', region_name=region_name)
        self.lambda_client = boto3.client('lambda', region_name=region_name)
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.sts_client = boto3.client('sts', region_name=region_name)
    
    def get_account_id(self) -> str:
        """
        Get AWS account ID
        
        Returns:
            Account ID string
        """
        try:
            identity = self.sts_client.get_caller_identity()
            return identity['Account']
        except ClientError as e:
            logger.error(f"Error getting account ID: {e}")
            raise
    
    def create_monthly_budget(self, budget_amount: float, 
                            notification_threshold: float = 80.0):
        """
        Create a monthly budget with notifications
        
        Args:
            budget_amount: Monthly budget amount in USD
            notification_threshold: Percentage threshold for notifications (default 80%)
        """
        account_id = self.get_account_id()
        
        budget = {
            'BudgetLimit': {
                'Amount': str(budget_amount),
                'Unit': 'USD'
            },
            'BudgetName': 'NYC_TLC_Monthly_Budget',
            'BudgetType': 'COST',
            'CostFilters': {},
            'CostTypes': {
                'IncludeCredit': True,
                'IncludeDiscount': True,
                'IncludeOtherSubscription': True,
                'IncludeRecurring': True,
                'IncludeRefund': True,
                'IncludeSubscription': True,
                'UseBlended': False
            },
            'TimePeriod': {
                'Start': datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                'End': (datetime.now().replace(day=1) + timedelta(days=32)).replace(day=1)
            },
            'TimeUnit': 'MONTHLY'
        }
        
        notifications = [
            {
                'ComparisonOperator': 'GREATER_THAN',
                'NotificationType': 'ACTUAL',
                'Threshold': notification_threshold,
                'ThresholdType': 'PERCENTAGE'
            }
        ]
        
        subscribers = [
            {
                'Address': 'data-team@example.com',  # Replace with actual email
                'SubscriptionType': 'EMAIL'
            }
        ]
        
        try:
            self.budgets_client.create_budget(
                AccountId=account_id,
                Budget=budget,
                NotificationsWithSubscribers=[
                    {
                        'Notification': notifications[0],
                        'Subscribers': subscribers
                    }
                ]
            )
            logger.info(f"Created monthly budget: ${budget_amount}")
        except ClientError as e:
            logger.error(f"Error creating budget: {e}")
    
    def create_cost_anomaly_detector(self):
        """
        Create cost anomaly detection for monitoring unusual spending
        """
        try:
            # Create an anomaly monitor for the entire account
            response = self.cloudwatch_client.put_anomaly_detector(
                Namespace='AWS/Billing',
                MetricName='EstimatedCharges',
                Dimensions=[
                    {
                        'Name': 'ServiceName',
                        'Value': 'AmazonRDS'
                    }
                ],
                Stat='Average',
                Configuration={
                    'ExcludedTimeRanges': [
                        {
                            'StartTime': datetime.now() - timedelta(days=1),
                            'EndTime': datetime.now()
                        }
                    ]
                }
            )
            logger.info("Created cost anomaly detector")
        except ClientError as e:
            logger.error(f"Error creating anomaly detector: {e}")
    
    def setup_billing_alarm(self, threshold_amount: float, 
                          sns_topic_arn: str = None):
        """
        Set up CloudWatch alarm for billing alerts
        
        Args:
            threshold_amount: Threshold amount in USD
            sns_topic_arn: SNS topic ARN for notifications
        """
        try:
            # Create billing alarm
            alarm_name = 'NYC_TLC_Billing_Alert'
            
            # Set up SNS topic if not provided
            if not sns_topic_arn:
                # Create SNS topic for billing alerts
                sns_client = boto3.client('sns', region_name=self.region)
                topic_response = sns_client.create_topic(Name='nyc-tlc-billing-alerts')
                sns_topic_arn = topic_response['TopicArn']
                
                # Subscribe email to topic (replace with actual email)
                sns_client.subscribe(
                    TopicArn=sns_topic_arn,
                    Protocol='email',
                    Endpoint='data-team@example.com'
                )
            
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=alarm_name,
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='EstimatedCharges',
                Namespace='AWS/Billing',
                Period=21600,  # 6 hours
                Statistic='Maximum',
                Threshold=threshold_amount,
                ActionsEnabled=True,
                AlarmDescription=f'Billing alert for NYC TLC Data Platform - threshold: ${threshold_amount}',
                AlarmActions=[sns_topic_arn],
                OKActions=[sns_topic_arn],
                Dimensions=[
                    {
                        'Name': 'Currency',
                        'Value': 'USD'
                    }
                ]
            )
            logger.info(f"Created billing alarm with threshold: ${threshold_amount}")
        except ClientError as e:
            logger.error(f"Error creating billing alarm: {e}")
    
    def configure_rds_auto_pause(self, db_cluster_identifier: str, 
                               seconds_until_pause: int = 300):
        """
        Configure RDS Aurora cluster to auto-pause to save costs
        
        Args:
            db_cluster_identifier: RDS cluster identifier
            seconds_until_pause: Seconds to wait before pausing (default 5 minutes)
        """
        try:
            # Modify the DB cluster to enable auto-pause
            self.rds_client.modify_db_cluster(
                DBClusterIdentifier=db_cluster_identifier,
                ServerlessV2ScalingConfiguration={
                    'MinCapacity': 0.5,  # Minimum ACUs
                    'MaxCapacity': 2.0   # Maximum ACUs
                },
                PauseTimeSeconds=seconds_until_pause
            )
            
            # Also enable auto-pause specifically for serverless v1
            self.rds_client.modify_db_cluster(
                DBClusterIdentifier=db_cluster_identifier,
                ScalingConfiguration={
                    'MinCapacity': 1,
                    'MaxCapacity': 4,
                    'AutoPause': True,
                    'SecondsUntilAutoPause': seconds_until_pause
                }
            )
            
            logger.info(f"Configured auto-pause for RDS cluster: {db_cluster_identifier}")
        except ClientError as e:
            logger.error(f"Error configuring RDS auto-pause: {e}")
    
    def create_spot_instance_processing_fleet(self, instance_type: str = 'm5.large',
                                            max_price: str = '0.10',
                                            desired_capacity: int = 2):
        """
        Create a spot instance fleet for cost-effective data processing
        
        Args:
            instance_type: EC2 instance type
            max_price: Maximum price for spot instances
            desired_capacity: Desired number of instances
        """
        try:
            # Create a launch template
            launch_template_response = self.ec2_client.create_launch_template(
                LaunchTemplateName='nyc-tlc-spot-processing-template',
                LaunchTemplateData={
                    'ImageId': 'ami-0abcdef1234567890',  # Replace with actual AMI ID
                    'InstanceType': instance_type,
                    'SpotMarketOptions': {
                        'MaxPrice': max_price,
                        'SpotInstanceType': 'one-time'  # or 'persistent'
                    },
                    'UserData': '''#!/bin/bash
# Setup script for NYC TLC data processing
yum update -y
yum install -y python3
# Add your processing setup here
''',
                    'IamInstanceProfile': {
                        'Name': 'nyc-tlc-processing-role'  # Replace with actual role
                    },
                    'SecurityGroupIds': [
                        'sg-12345678'  # Replace with actual security group
                    ]
                }
            )
            
            # Create spot fleet request
            spot_fleet_response = self.ec2_client.request_spot_fleet(
                SpotFleetRequestConfig={
                    'AllocationStrategy': 'lowestPrice',
                    'OnDemandFulfilledInitialCapacity': 0,
                    'IamFleetRole': 'arn:aws:iam::123456789012:role/spot-fleet-role',  # Replace with actual role
                    'LaunchSpecifications': [
                        {
                            'ImageId': 'ami-0abcdef1234567890',  # Replace with actual AMI ID
                            'InstanceType': instance_type,
                            'SpotPrice': max_price,
                            'KeyName': 'nyc-tlc-keypair',  # Replace with actual key pair
                            'SecurityGroups': [
                                {
                                    'GroupId': 'sg-12345678'  # Replace with actual security group
                                }
                            ],
                            'UserData': '''#!/bin/bash
# Setup script for NYC TLC data processing
yum update -y
yum install -y python3
# Add your processing setup here
'''
                        }
                    ],
                    'TargetCapacity': desired_capacity,
                    'TerminateInstancesWithExpiration': True
                }
            )
            
            logger.info(f"Created spot fleet for data processing: {spot_fleet_response['SpotFleetRequestId']}")
            return spot_fleet_response['SpotFleetRequestId']
        except ClientError as e:
            logger.error(f"Error creating spot fleet: {e}")
            return None
    
    def create_lambda_processing_function(self, function_name: str, 
                                         s3_bucket: str, s3_key: str):
        """
        Create a cost-effective Lambda function for data processing
        
        Args:
            function_name: Name of the Lambda function
            s3_bucket: S3 bucket containing the function code
            s3_key: S3 key for the function code zip file
        """
        try:
            # Create Lambda execution role (this would typically be created separately)
            # For this example, we'll assume the role exists
            
            lambda_response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role='arn:aws:iam::123456789012:role/lambda-execution-role',  # Replace with actual role
                Handler='lambda_function.lambda_handler',
                Code={
                    'S3Bucket': s3_bucket,
                    'S3Key': s3_key
                },
                Description='NYC TLC Data Processing Function',
                Timeout=900,  # 15 minutes
                MemorySize=3008,  # 3GB
                Environment={
                    'Variables': {
                        'S3_RAW_BUCKET': 'nyc-tlc-raw-data',
                        'S3_PROCESSED_BUCKET': 'nyc-tlc-processed-data',
                        'DB_HOST': 'nyc-tlc-datawarehouse.cluster-xxx.us-east-1.rds.amazonaws.com'
                    }
                },
                Tags={
                    'Project': 'NYC_TLC_Data_Platform',
                    'Environment': 'Production',
                    'CostCenter': 'DataProcessing'
                }
            )
            
            logger.info(f"Created Lambda function: {function_name}")
            
            # Add S3 trigger for new data
            self.lambda_client.add_permission(
                FunctionName=function_name,
                StatementId='s3-trigger',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{s3_bucket}'
            )
            
            # Configure S3 bucket notification
            self.s3_client.put_bucket_notification_configuration(
                Bucket=s3_bucket,
                NotificationConfiguration={
                    'LambdaConfigurations': [
                        {
                            'LambdaFunctionArn': lambda_response['FunctionArn'],
                            'Events': ['s3:ObjectCreated:*'],
                            'Filter': {
                                'Key': {
                                    'FilterRules': [
                                        {
                                            'Name': 'prefix',
                                            'Value': 'taxi-data/'
                                        },
                                        {
                                            'Name': 'suffix',
                                            'Value': '.parquet'
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            )
            
            logger.info(f"Configured S3 trigger for Lambda function: {function_name}")
            return lambda_response['FunctionArn']
        except ClientError as e:
            logger.error(f"Error creating Lambda function: {e}")
            return None
    
    def setup_cost_optimization_dashboard(self):
        """
        Create a CloudWatch dashboard for cost optimization monitoring
        """
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "view": "timeSeries",
                        "stacked": False,
                        "metrics": [
                            ["AWS/Billing", "EstimatedCharges", "ServiceName", "AmazonRDS"],
                            [".", "EstimatedCharges", "ServiceName", "AmazonS3"],
                            [".", "EstimatedCharges", "ServiceName", "AmazonEC2"]
                        ],
                        "region": self.region,
                        "title": "Service Cost Breakdown",
                        "period": 86400,
                        "stat": "Sum"
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 0,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "view": "timeSeries",
                        "stacked": False,
                        "metrics": [
                            ["AWS/RDS", "CPUUtilization", "DBInstanceIdentifier", "nyc-tlc-datawarehouse"],
                            ["AWS/RDS", "DatabaseConnections", "DBInstanceIdentifier", "nyc-tlc-datawarehouse"]
                        ],
                        "region": self.region,
                        "title": "RDS Utilization (Consider Auto-Pause)",
                        "period": 300,
                        "stat": "Average"
                    }
                },
                {
                    "type": "metric",
                    "x": 0,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "view": "timeSeries",
                        "stacked": False,
                        "metrics": [
                            ["AWS/S3", "BucketSizeBytes", "BucketName", "nyc-tlc-raw-data", "StorageType", "StandardStorage"],
                            ["AWS/S3", "BucketSizeBytes", "BucketName", "nyc-tlc-processed-data", "StorageType", "StandardStorage"]
                        ],
                        "region": self.region,
                        "title": "S3 Storage Costs (Apply Lifecycle Policies)",
                        "period": 86400,
                        "stat": "Average",
                        "yAxis": {
                            "left": {
                                "label": "Size (Bytes)",
                                "showUnits": True
                            }
                        }
                    }
                },
                {
                    "type": "metric",
                    "x": 12,
                    "y": 6,
                    "width": 12,
                    "height": 6,
                    "properties": {
                        "view": "timeSeries",
                        "stacked": False,
                        "metrics": [
                            ["AWS/Lambda", "Duration", "FunctionName", "nyc-tlc-data-processing"],
                            ["AWS/Lambda", "Invocations", "FunctionName", "nyc-tlc-data-processing"]
                        ],
                        "region": self.region,
                        "title": "Lambda Usage (Cost-Effective Processing)",
                        "period": 300,
                        "stat": "Average"
                    }
                }
            ]
        }
        
        try:
            self.cloudwatch_client.put_dashboard(
                DashboardName='NYC_TLC_Cost_Optimization',
                DashboardBody=json.dumps(dashboard_body)
            )
            logger.info("Created cost optimization dashboard")
        except ClientError as e:
            logger.error(f"Error creating dashboard: {e}")
    
    def generate_cost_optimization_report(self) -> Dict:
        """
        Generate a cost optimization report
        
        Returns:
            Dictionary with cost optimization recommendations
        """
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'recommendations': [],
            'current_monthly_cost': 0,
            'potential_savings': 0
        }
        
        # Add recommendations
        report['recommendations'].extend([
            {
                'category': 'Storage',
                'recommendation': 'Implement S3 lifecycle policies to move old data to cheaper storage classes',
                'potential_savings': '$200-500/month',
                'priority': 'high'
            },
            {
                'category': 'Compute',
                'recommendation': 'Use spot instances for batch processing jobs',
                'potential_savings': '50-70% reduction in EC2 costs',
                'priority': 'high'
            },
            {
                'category': 'Database',
                'recommendation': 'Enable RDS auto-pause for development/low-usage periods',
                'potential_savings': '60-90% reduction in RDS costs during idle periods',
                'priority': 'medium'
            },
            {
                'category': 'Processing',
                'recommendation': 'Migrate batch jobs to Lambda for cost-effective processing',
                'potential_savings': 'Significant reduction in fixed compute costs',
                'priority': 'medium'
            },
            {
                'category': 'Monitoring',
                'recommendation': 'Set up budgets and alerts to prevent cost overruns',
                'potential_savings': 'Early detection of cost anomalies',
                'priority': 'high'
            }
        ])
        
        logger.info("Generated cost optimization report")
        return report


def main():
    """
    Main function to run cost control strategies
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Cost Control for NYC TLC Data Platform')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--setup-budget', type=float, help='Set up monthly budget in USD')
    parser.add_argument('--billing-threshold', type=float, help='Set billing alarm threshold')
    parser.add_argument('--auto-pause-rds', help='RDS cluster identifier for auto-pause')
    parser.add_argument('--create-spot-fleet', action='store_true', help='Create spot instance fleet')
    parser.add_argument('--create-lambda', help='Create Lambda processing function')
    parser.add_argument('--s3-bucket', help='S3 bucket for Lambda code')
    parser.add_argument('--s3-key', help='S3 key for Lambda code zip')
    parser.add_argument('--generate-report', action='store_true', help='Generate cost optimization report')
    
    args = parser.parse_args()
    
    controller = AWSCostController(region_name=args.region)
    
    if args.setup_budget:
        controller.create_monthly_budget(args.setup_budget)
    
    if args.billing_threshold:
        controller.setup_billing_alarm(args.billing_threshold)
    
    if args.auto_pause_rds:
        controller.configure_rds_auto_pause(args.auto_pause_rds)
    
    if args.create_spot_fleet:
        controller.create_spot_instance_processing_fleet()
    
    if args.create_lambda and args.s3_bucket and args.s3_key:
        controller.create_lambda_processing_function(args.create_lambda, args.s3_bucket, args.s3_key)
    
    if args.generate_report:
        report = controller.generate_cost_optimization_report()
        print(json.dumps(report, indent=2))
    
    # Always create the cost optimization dashboard
    controller.setup_cost_optimization_dashboard()


if __name__ == "__main__":
    main()