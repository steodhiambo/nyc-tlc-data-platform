#!/usr/bin/env python3
"""
Weekly Audit Scripts and Scale Testing for NYC TLC Data Platform

This script performs weekly audits of the data platform and runs scale tests
with full-year data to ensure performance and cost optimization.
"""

import os
import sys
import subprocess
import logging
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from botocore.exceptions import ClientError
import numpy as np


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weekly_audit.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WeeklyAuditor:
    """
    Performs weekly audits of the NYC TLC Data Platform
    """
    
    def __init__(self, db_connection_params: Dict, aws_region: str = 'us-east-1'):
        """
        Initialize the weekly auditor
        
        Args:
            db_connection_params: Database connection parameters
            aws_region: AWS region
        """
        self.db_params = db_connection_params
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
        self.rds_client = boto3.client('rds', region_name=aws_region)
    
    def connect_to_database(self):
        """
        Establish connection to PostgreSQL database
        
        Returns:
            Database connection object
        """
        try:
            conn = psycopg2.connect(**self.db_params)
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def audit_table_sizes(self) -> Dict[str, int]:
        """
        Audit table sizes to identify growth patterns
        
        Returns:
            Dictionary with table names and sizes in MB
        """
        conn = self.connect_to_database()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) as size_pretty,
                        pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) / 1024 / 1024 as size_mb
                    FROM pg_tables 
                    WHERE schemaname IN ('nyc_taxi_dw', 'taxi_data')
                    ORDER BY size_mb DESC;
                """)
                
                results = cursor.fetchall()
                table_sizes = {f"{row['schemaname']}.{row['tablename']}": float(row['size_mb']) for row in results}
                
                logger.info(f"Audited {len(table_sizes)} tables")
                return table_sizes
        finally:
            conn.close()
    
    def audit_index_usage(self) -> Dict[str, Dict]:
        """
        Audit index usage to identify unused indexes
        
        Returns:
            Dictionary with index information
        """
        conn = self.connect_to_database()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        indexname,
                        idx_tup_read,
                        idx_tup_fetch,
                        idx_scan
                    FROM pg_stat_user_indexes
                    WHERE schemaname IN ('nyc_taxi_dw', 'taxi_data')
                    ORDER BY idx_scan ASC;
                """)
                
                results = cursor.fetchall()
                index_stats = {}
                
                for row in results:
                    index_name = f"{row['schemaname']}.{row['indexname']}"
                    index_stats[index_name] = {
                        'table': f"{row['schemaname']}.{row['tablename']}",
                        'tuples_read': row['idx_tup_read'],
                        'tuples_fetch': row['idx_tup_fetch'],
                        'scans': row['idx_scan']
                    }
                
                logger.info(f"Audited {len(index_stats)} indexes")
                return index_stats
        finally:
            conn.close()
    
    def audit_query_performance(self) -> List[Dict]:
        """
        Audit query performance using pg_stat_statements
        
        Returns:
            List of slow query information
        """
        conn = self.connect_to_database()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Make sure pg_stat_statements is installed
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
                
                cursor.execute("""
                    SELECT 
                        query,
                        calls,
                        total_time,
                        mean_time,
                        stddev_time,
                        rows,
                        100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
                    FROM pg_stat_statements
                    WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user)
                    ORDER BY total_time DESC
                    LIMIT 20;
                """)
                
                results = cursor.fetchall()
                logger.info(f"Audited top 20 queries by execution time")
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error auditing query performance: {e}")
            return []
        finally:
            conn.close()
    
    def audit_data_quality(self) -> Dict[str, int]:
        """
        Audit data quality metrics
        
        Returns:
            Dictionary with quality metrics
        """
        conn = self.connect_to_database()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check for common data quality issues
                quality_checks = {}
                
                # Check for null pickup_datetime in fact table
                cursor.execute("""
                    SELECT COUNT(*) as null_count
                    FROM nyc_taxi_dw.trips_fact
                    WHERE pickup_datetime IS NULL;
                """)
                result = cursor.fetchone()
                quality_checks['null_pickup_datetime'] = result['null_count']
                
                # Check for negative fares
                cursor.execute("""
                    SELECT COUNT(*) as negative_count
                    FROM nyc_taxi_dw.trips_fact
                    WHERE total_amount < 0;
                """)
                result = cursor.fetchone()
                quality_checks['negative_fares'] = result['negative_count']
                
                # Check for invalid location IDs
                cursor.execute("""
                    SELECT COUNT(*) as invalid_count
                    FROM nyc_taxi_dw.trips_fact
                    WHERE pickup_location_key NOT IN (SELECT location_key FROM nyc_taxi_dw.location_dim);
                """)
                result = cursor.fetchone()
                quality_checks['invalid_pickup_locations'] = result['invalid_count']
                
                # Check for trips with zero distance but non-zero time
                cursor.execute("""
                    SELECT COUNT(*) as anomalous_count
                    FROM nyc_taxi_dw.trips_fact
                    WHERE trip_distance = 0 AND trip_duration_minutes > 0;
                """)
                result = cursor.fetchone()
                quality_checks['zero_distance_nonzero_time'] = result['anomalous_count']
                
                logger.info("Completed data quality audit")
                return quality_checks
        finally:
            conn.close()
    
    def audit_storage_costs(self, s3_buckets: List[str]) -> Dict[str, Dict]:
        """
        Audit S3 storage costs and usage
        
        Args:
            s3_buckets: List of S3 bucket names to audit
            
        Returns:
            Dictionary with storage metrics
        """
        storage_metrics = {}
        
        for bucket in s3_buckets:
            try:
                # Get bucket size metrics
                response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {
                            'Name': 'BucketName',
                            'Value': bucket
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
                
                # Get object count metrics
                objects_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {
                            'Name': 'BucketName',
                            'Value': bucket
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
                
                storage_metrics[bucket] = {
                    'latest_size_bytes': response['Datapoints'][-1]['Average'] if response['Datapoints'] else 0,
                    'latest_objects': objects_response['Datapoints'][-1]['Average'] if objects_response['Datapoints'] else 0,
                    'size_trend': len(response['Datapoints']),
                    'cost_estimation': self.estimate_s3_cost(response['Datapoints'][-1]['Average'] if response['Datapoints'] else 0)
                }
            except ClientError as e:
                logger.error(f"Error auditing S3 bucket {bucket}: {e}")
                storage_metrics[bucket] = {'error': str(e)}
        
        logger.info(f"Audited {len(s3_buckets)} S3 buckets")
        return storage_metrics
    
    def estimate_s3_cost(self, size_bytes: float) -> float:
        """
        Estimate monthly S3 cost based on size
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Estimated monthly cost in USD
        """
        size_gb = size_bytes / (1024**3)
        # Simplified cost calculation: $0.023 per GB for standard storage
        estimated_cost = size_gb * 0.023
        return round(estimated_cost, 2)
    
    def run_weekly_audit(self, s3_buckets: List[str]) -> Dict:
        """
        Run comprehensive weekly audit
        
        Args:
            s3_buckets: List of S3 bucket names to include in audit
            
        Returns:
            Complete audit report
        """
        logger.info("Starting weekly audit...")
        
        audit_report = {
            'timestamp': datetime.utcnow().isoformat(),
            'table_sizes': self.audit_table_sizes(),
            'index_usage': self.audit_index_usage(),
            'query_performance': self.audit_query_performance(),
            'data_quality': self.audit_data_quality(),
            'storage_metrics': self.audit_storage_costs(s3_buckets),
            'recommendations': []
        }
        
        # Generate recommendations based on audit results
        recommendations = self.generate_recommendations(audit_report)
        audit_report['recommendations'] = recommendations
        
        logger.info("Weekly audit completed")
        return audit_report
    
    def generate_recommendations(self, audit_report: Dict) -> List[Dict]:
        """
        Generate recommendations based on audit results
        
        Args:
            audit_report: Complete audit report
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        # Table size recommendations
        large_tables = {k: v for k, v in audit_report['table_sizes'].items() if v > 1000}  # > 1GB
        if large_tables:
            recommendations.append({
                'category': 'Storage',
                'issue': 'Large tables detected',
                'details': f"Tables larger than 1GB: {list(large_tables.keys())}",
                'recommendation': 'Consider partitioning large tables or archiving old data',
                'priority': 'high'
            })
        
        # Index recommendations
        unused_indexes = {k: v for k, v in audit_report['index_usage'].items() if v['scans'] == 0}
        if unused_indexes:
            recommendations.append({
                'category': 'Performance',
                'issue': 'Unused indexes detected',
                'details': f"Indexes with 0 scans: {list(unused_indexes.keys())}",
                'recommendation': 'Consider dropping unused indexes to save storage and maintenance overhead',
                'priority': 'medium'
            })
        
        # Data quality recommendations
        quality_issues = {k: v for k, v in audit_report['data_quality'].items() if v > 0}
        if quality_issues:
            recommendations.append({
                'category': 'Data Quality',
                'issue': 'Data quality issues detected',
                'details': f"Quality issues: {quality_issues}",
                'recommendation': 'Implement data validation checks and monitoring',
                'priority': 'high'
            })
        
        # Query performance recommendations
        slow_queries = [q for q in audit_report['query_performance'] if q['mean_time'] > 1000]  # > 1 second
        if slow_queries:
            recommendations.append({
                'category': 'Performance',
                'issue': 'Slow queries detected',
                'details': f"Queries with mean time > 1 second: {len(slow_queries)}",
                'recommendation': 'Review and optimize slow queries, consider adding indexes',
                'priority': 'high'
            })
        
        return recommendations


class ScaleTester:
    """
    Performs scale testing with full-year data
    """
    
    def __init__(self, db_connection_params: Dict, aws_region: str = 'us-east-1'):
        """
        Initialize the scale tester
        
        Args:
            db_connection_params: Database connection parameters
            aws_region: AWS region
        """
        self.db_params = db_connection_params
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
    
    def generate_test_data(self, year: int, num_records: int = 100000) -> List[Dict]:
        """
        Generate test data for scale testing
        
        Args:
            year: Year for test data
            num_records: Number of records to generate
            
        Returns:
            List of test records
        """
        logger.info(f"Generating {num_records} test records for year {year}")
        
        # Generate synthetic NYC TLC data
        test_data = []
        base_datetime = datetime(year, 1, 1)
        
        for i in range(num_records):
            # Generate random trip data
            pickup_time = base_datetime + timedelta(
                days=np.random.randint(0, 365),
                hours=np.random.randint(0, 23),
                minutes=np.random.randint(0, 59)
            )
            trip_duration = np.random.exponential(15)  # Average 15 minutes
            dropoff_time = pickup_time + timedelta(minutes=trip_duration)
            
            record = {
                'vendor_key': np.random.randint(1, 3),
                'pickup_location_key': np.random.randint(1, 264),  # NYC has ~265 zones
                'dropoff_location_key': np.random.randint(1, 264),
                'pickup_time_key': 1,  # Simplified for test
                'dropoff_time_key': 1,
                'payment_key': np.random.randint(1, 3),
                'passenger_count': np.random.randint(1, 5),
                'trip_distance': round(np.random.exponential(2.0), 2),  # Average 2 miles
                'trip_duration_minutes': round(trip_duration, 2),
                'fare_amount': round(np.random.uniform(5.0, 50.0), 2),
                'total_amount': round(np.random.uniform(8.0, 60.0), 2),
                'pickup_datetime': pickup_time,
                'dropoff_datetime': dropoff_time,
                'year': year,
                'month': pickup_time.month
            }
            test_data.append(record)
        
        logger.info(f"Generated {len(test_data)} test records")
        return test_data
    
    def run_query_performance_test(self, test_queries: List[str], iterations: int = 5) -> Dict:
        """
        Run performance tests on queries
        
        Args:
            test_queries: List of queries to test
            iterations: Number of iterations for each query
            
        Returns:
            Performance test results
        """
        conn = self.connect_to_database()
        results = {}
        
        try:
            for i, query in enumerate(test_queries):
                logger.info(f"Running performance test {i+1}/{len(test_queries)}")
                
                execution_times = []
                for iteration in range(iterations):
                    start_time = time.time()
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(query)
                            cursor.fetchall()  # Fetch results to complete execution
                    except Exception as e:
                        logger.error(f"Query failed: {e}")
                        continue
                    finally:
                        end_time = time.time()
                        execution_times.append((end_time - start_time) * 1000)  # Convert to milliseconds
                
                if execution_times:
                    results[f"query_{i+1}"] = {
                        'query': query[:100] + "..." if len(query) > 100 else query,
                        'execution_times_ms': execution_times,
                        'avg_time_ms': round(np.mean(execution_times), 2),
                        'min_time_ms': round(np.min(execution_times), 2),
                        'max_time_ms': round(np.max(execution_times), 2),
                        'std_dev_ms': round(np.std(execution_times), 2)
                    }
        
        finally:
            conn.close()
        
        logger.info(f"Completed {len(results)} query performance tests")
        return results
    
    def run_data_loading_test(self, year: int, num_records: int = 100000) -> Dict:
        """
        Test data loading performance
        
        Args:
            year: Year for test data
            num_records: Number of records to load
            
        Returns:
            Loading test results
        """
        logger.info(f"Running data loading test with {num_records} records for year {year}")
        
        # Generate test data
        test_data = self.generate_test_data(year, num_records)
        
        # Measure loading time
        conn = self.connect_to_database()
        start_time = time.time()
        
        try:
            with conn.cursor() as cursor:
                # Insert test data using batch insert
                insert_query = """
                INSERT INTO nyc_taxi_dw.trips_fact (
                    vendor_key, pickup_location_key, dropoff_location_key,
                    pickup_time_key, dropoff_time_key, payment_key,
                    passenger_count, trip_distance, trip_duration_minutes,
                    fare_amount, total_amount, pickup_datetime, dropoff_datetime,
                    year, month
                ) VALUES %s
                """
                
                # Prepare data for batch insert
                values = []
                for record in test_data:
                    values.append((
                        record['vendor_key'], record['pickup_location_key'], record['dropoff_location_key'],
                        record['pickup_time_key'], record['dropoff_time_key'], record['payment_key'],
                        record['passenger_count'], record['trip_distance'], record['trip_duration_minutes'],
                        record['fare_amount'], record['total_amount'], record['pickup_datetime'], 
                        record['dropoff_datetime'], record['year'], record['month']
                    ))
                
                # Execute batch insert
                from psycopg2.extras import execute_values
                execute_values(cursor, insert_query, values, template=None, page_size=1000)
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Data loading test failed: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
        
        end_time = time.time()
        loading_time = end_time - start_time
        
        results = {
            'records_loaded': len(test_data),
            'loading_time_seconds': round(loading_time, 2),
            'records_per_second': round(len(test_data) / loading_time, 2),
            'bytes_loaded': len(json.dumps(test_data)) if test_data else 0
        }
        
        logger.info(f"Data loading test completed: {results['records_per_second']} records/sec")
        return results
    
    def connect_to_database(self):
        """
        Establish connection to PostgreSQL database
        
        Returns:
            Database connection object
        """
        try:
            conn = psycopg2.connect(**self.db_params)
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def run_scale_test(self, year: int, num_records: int = 100000) -> Dict:
        """
        Run comprehensive scale test
        
        Args:
            year: Year for test data
            num_records: Number of records to test with
            
        Returns:
            Complete scale test report
        """
        logger.info(f"Starting scale test for year {year} with {num_records} records...")
        
        # Define test queries that represent common analytical patterns
        test_queries = [
            "SELECT COUNT(*) FROM nyc_taxi_dw.trips_fact WHERE year = %s" % year,
            "SELECT pickup_location_key, COUNT(*) FROM nyc_taxi_dw.trips_fact WHERE year = %s GROUP BY pickup_location_key ORDER BY COUNT(*) DESC LIMIT 10" % year,
            "SELECT EXTRACT(HOUR FROM pickup_datetime), COUNT(*) FROM nyc_taxi_dw.trips_fact WHERE year = %s GROUP BY EXTRACT(HOUR FROM pickup_datetime) ORDER BY COUNT(*) DESC" % year,
            "SELECT payment_key, AVG(total_amount) FROM nyc_taxi_dw.trips_fact WHERE year = %s GROUP BY payment_key" % year,
            "SELECT vendor_key, SUM(total_amount) FROM nyc_taxi_dw.trips_fact WHERE year = %s GROUP BY vendor_key" % year
        ]
        
        scale_report = {
            'timestamp': datetime.utcnow().isoformat(),
            'test_year': year,
            'test_records': num_records,
            'data_loading_results': self.run_data_loading_test(year, num_records),
            'query_performance_results': self.run_query_performance_test(test_queries),
            'scalability_metrics': {}
        }
        
        # Calculate scalability metrics
        loading_results = scale_report['data_loading_results']
        scale_report['scalability_metrics'] = {
            'loading_efficiency': loading_results['records_per_second'],
            'linear_scaling_ratio': loading_results['records_per_second'] / (num_records / 1000),  # Normalize to 1K records
            'performance_score': self.calculate_performance_score(scale_report)
        }
        
        logger.info("Scale test completed")
        return scale_report
    
    def calculate_performance_score(self, scale_report: Dict) -> float:
        """
        Calculate an overall performance score
        
        Args:
            scale_report: Complete scale test report
            
        Returns:
            Performance score (0-100)
        """
        # Calculate score based on various factors
        loading_score = min(scale_report['data_loading_results']['records_per_second'] / 1000 * 10, 25)  # Max 25 points
        query_score = min(25, 25)  # Placeholder - would calculate based on query performance
        
        # Additional factors
        num_queries = len(scale_report['query_performance_results'])
        avg_query_time = np.mean([q['avg_time_ms'] for q in scale_report['query_performance_results'].values()]) if num_queries > 0 else 1000
        query_time_score = max(0, 25 - (avg_query_time / 100))  # Lower time = higher score
        
        # Data quality factor (assuming perfect in test)
        quality_score = 25
        
        total_score = min(100, loading_score + query_score + query_time_score + quality_score)
        return round(total_score, 2)


def main():
    """
    Main function to run weekly audits and scale testing
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Weekly Audit and Scale Testing for NYC TLC Data Platform')
    parser.add_argument('--audit', action='store_true', help='Run weekly audit')
    parser.add_argument('--scale-test', action='store_true', help='Run scale testing')
    parser.add_argument('--year', type=int, default=2023, help='Year for scale testing')
    parser.add_argument('--records', type=int, default=100000, help='Number of records for scale testing')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5433, help='Database port')
    parser.add_argument('--database', default='nyc_tlc_dw', help='Database name')
    parser.add_argument('--user', default='nyc_tlc_user', help='Database user')
    parser.add_argument('--password', default='nyc_tlc_password', help='Database password')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--s3-buckets', nargs='+', default=['nyc-tlc-raw-data', 'nyc-tlc-processed-data'], 
                       help='S3 buckets to audit')
    
    args = parser.parse_args()
    
    # Database connection parameters
    db_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    if args.audit:
        auditor = WeeklyAuditor(db_params, args.region)
        audit_report = auditor.run_weekly_audit(args.s3_buckets)
        
        # Save audit report
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        with open(f'weekly_audit_report_{timestamp}.json', 'w') as f:
            json.dump(audit_report, f, indent=2, default=str)
        
        print(f"Weekly audit completed. Report saved as weekly_audit_report_{timestamp}.json")
        print(f"Found {len(audit_report['recommendations'])} recommendations")
        
        for rec in audit_report['recommendations']:
            print(f"  - {rec['priority'].upper()}: {rec['recommendation']} ({rec['category']})")
    
    if args.scale_test:
        tester = ScaleTester(db_params, args.region)
        scale_report = tester.run_scale_test(args.year, args.records)
        
        # Save scale test report
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        with open(f'scale_test_report_{timestamp}.json', 'w') as f:
            json.dump(scale_report, f, indent=2, default=str)
        
        print(f"Scale test completed for year {args.year} with {args.records} records.")
        print(f"Performance Score: {scale_report['scalability_metrics']['performance_score']}/100")
        print(f"Loading Rate: {scale_report['data_loading_results']['records_per_second']} records/sec")
        
        avg_query_time = np.mean([q['avg_time_ms'] for q in scale_report['query_performance_results'].values()])
        print(f"Average Query Time: {avg_query_time:.2f} ms")


if __name__ == "__main__":
    main()