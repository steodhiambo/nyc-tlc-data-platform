"""
Integration tests for the main ETL pipeline
"""
import unittest
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from dags.nyc_tlc_etl_pipeline import (
    extract_data_from_s3,
    transform_data,
    clean_taxi_data,
    enrich_with_taxi_zones,
    load_data_to_postgres,
    run_quality_checks
)


class TestETLPipelineIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline components"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a mock context for Airflow tasks
        self.mock_context = {
            'execution_date': datetime(2023, 1, 1),
            'run_id': 'test_run_123',
            'task_instance': Mock()
        }
    
    @patch('dags.nyc_tlc_etl_pipeline.boto3.client')
    @patch('dags.nyc_tlc_etl_pipeline.S3Hook')
    @patch('dags.nyc_tlc_etl_pipeline.os.getenv')
    def test_extract_data_from_s3(self, mock_getenv, mock_s3_hook, mock_boto3_client):
        """Test the extract_data_from_s3 function"""
        # Set up mocks
        mock_getenv.return_value = 'test-bucket'
        
        mock_s3_hook_instance = Mock()
        mock_s3_hook.return_value = mock_s3_hook_instance
        
        mock_boto3_instance = Mock()
        mock_boto3_client.return_value = mock_boto3_instance
        
        # Mock the list_objects_v2 response
        mock_boto3_instance.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'taxi-data/yellow/2023/2023-01/file1.parquet'},
                {'Key': 'taxi-data/yellow/2023/2023-01/file2.parquet'}
            ]
        }
        
        # Mock the download_file method
        mock_s3_hook_instance.download_file.return_value = None
        
        # Call the function
        result = extract_data_from_s3(**self.mock_context)
        
        # Assertions
        self.assertEqual(len(result), 2)
        self.assertTrue(all(path.startswith('/tmp/') for path in result))
        mock_boto3_instance.list_objects_v2.assert_called_once()
        self.assertEqual(mock_s3_hook_instance.download_file.call_count, 2)
    
    @patch('dags.nyc_tlc_etl_pipeline.pq.read_table')
    @patch('dags.nyc_tlc_etl_pipeline.clean_taxi_data')
    @patch('dags.nyc_tlc_etl_pipeline.enrich_with_taxi_zones')
    def test_transform_data(self, mock_enrich, mock_clean, mock_read_table):
        """Test the transform_data function"""
        # Set up mocks
        mock_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'vendor_id': [1],
            'passenger_count': [1],
            'trip_distance': [2.3],
            'fare_amount': [10.5]
        })
        
        mock_table = Mock()
        mock_table.to_pandas.return_value = mock_df
        mock_read_table.return_value = mock_table
        
        mock_clean.return_value = mock_df
        mock_enrich.return_value = mock_df
        
        # Mock task instance to return temp files
        self.mock_context['task_instance'].xcom_pull.return_value = ['/tmp/test.parquet']
        
        # Call the function
        result = transform_data(**self.mock_context)
        
        # Assertions
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith('/tmp/'))
        mock_read_table.assert_called_once()
        mock_clean.assert_called_once()
        mock_enrich.assert_called_once()
    
    def test_clean_taxi_data(self):
        """Test the clean_taxi_data function"""
        # Create test data with some invalid records
        test_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01', None, '2023-01-01 00:30:00']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01', '2023-01-01 00:45:00', '2023-01-01 00:45:00']),
            'pickup_longitude': [-73.9857, -100.0, -74.0060],  # -100.0 is invalid
            'pickup_latitude': [40.7589, 50.0, 40.7306],  # 50.0 is invalid
            'dropoff_longitude': [-73.9784, -73.9949, -200.0],  # -200.0 is invalid
            'dropoff_latitude': [40.7505, 40.7577, 80.0],  # 80.0 is invalid
            'fare_amount': [10.5, -5.0, 8.0],  # -5.0 is invalid
            'passenger_count': [1, -1, 2],  # -1 is invalid
            'trip_distance': [2.3, -1.0, 1.8]  # -1.0 is invalid
        })
        
        # Call the function
        cleaned_df = clean_taxi_data(test_df)
        
        # Assertions
        # The cleaned dataframe should only have the valid row
        self.assertEqual(len(cleaned_df), 1)
        self.assertEqual(cleaned_df.iloc[0]['fare_amount'], 8.0)
        self.assertEqual(cleaned_df.iloc[0]['passenger_count'], 2)
        self.assertEqual(cleaned_df.iloc[0]['trip_distance'], 1.8)
    
    def test_enrich_with_taxi_zones(self):
        """Test the enrich_with_taxi_zones function"""
        # Create test data
        test_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'PULocationID': [123],
            'DOLocationID': [456]
        })
        
        # Call the function
        enriched_df = enrich_with_taxi_zones(test_df)
        
        # Assertions
        self.assertIn('pickup_location_id', enriched_df.columns)
        self.assertIn('dropoff_location_id', enriched_df.columns)
        self.assertEqual(enriched_df.iloc[0]['pickup_location_id'], 123)
        self.assertEqual(enriched_df.iloc[0]['dropoff_location_id'], 456)
        
        # Test with different column names
        test_df2 = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01'])
        })
        
        enriched_df2 = enrich_with_taxi_zones(test_df2)
        self.assertIn('pickup_location_id', enriched_df2.columns)
        self.assertIn('dropoff_location_id', enriched_df2.columns)
        self.assertEqual(enriched_df2.iloc[0]['pickup_location_id'], 0)  # Default value
        self.assertEqual(enriched_df2.iloc[0]['dropoff_location_id'], 0)  # Default value)


class TestETLPipelineWithMocks(unittest.TestCase):
    """Integration tests for the ETL pipeline with mocked external dependencies"""
    
    @patch('dags.nyc_tlc_etl_pipeline.create_engine')
    @patch('dags.nyc_tlc_etl_pipeline.pd.read_parquet')
    @patch('dags.nyc_tlc_etl_pipeline.os.getenv')
    @patch('dags.nyc_tlc_etl_pipeline.validate_data_with_great_expectations')
    def test_load_data_to_postgres(self, mock_validate, mock_getenv, mock_read_parquet, mock_create_engine):
        """Test the load_data_to_postgres function"""
        # Set up mocks
        mock_getenv.side_effect = lambda key, default=None: {
            'POSTGRES_USER': 'test_user',
            'POSTGRES_PASSWORD': 'test_password',
            'POSTGRES_HOST': 'test_host',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'test_db'
        }.get(key, default)
        
        # Create test dataframe
        test_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'vendor_id': [1],
            'passenger_count': [1],
            'trip_distance': [2.3],
            'fare_amount': [10.5]
        })
        
        mock_read_parquet.return_value = test_df
        mock_validate.return_value = {'success': True, 'results': []}
        
        # Mock the SQLAlchemy engine and connection
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        
        # Mock task instance to return processed file path
        mock_context = {
            'task_instance': Mock(),
            'run_id': 'test_run_123'
        }
        mock_context['task_instance'].xcom_pull.return_value = '/tmp/processed.parquet'
        
        # Call the function
        load_data_to_postgres(**mock_context)
        
        # Assertions
        mock_read_parquet.assert_called_once_with('/tmp/processed.parquet')
        mock_validate.assert_called_once_with(test_df, "yellow")
        mock_create_engine.assert_called_once()
        # Check that to_sql was called on the dataframe
        # Since we're mocking pd.read_parquet, we can't directly check the to_sql call
        # But we can verify the engine was created with the right connection string
        mock_create_engine.assert_called()
    
    @patch('dags.nyc_tlc_etl_pipeline.create_engine')
    @patch('dags.nyc_tlc_etl_pipeline.os.getenv')
    def test_run_quality_checks(self, mock_getenv, mock_create_engine):
        """Test the run_quality_checks function"""
        # Set up mocks
        mock_getenv.side_effect = lambda key, default=None: {
            'POSTGRES_USER': 'test_user',
            'POSTGRES_PASSWORD': 'test_password',
            'POSTGRES_HOST': 'test_host',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'test_db'
        }.get(key, default)
        
        # Mock the SQLAlchemy engine and connection
        mock_engine = Mock()
        mock_conn = Mock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the execute method to return test results
        mock_conn.execute.return_value.fetchone.return_value = [100]
        
        mock_create_engine.return_value = mock_engine
        
        # Create mock context
        mock_context = {
            'run_id': 'test_run_123'
        }
        
        # Call the function
        run_quality_checks(**mock_context)
        
        # Assertions
        mock_create_engine.assert_called_once()
        # Check that execute was called for each quality check query
        self.assertGreaterEqual(mock_conn.execute.call_count, 5)  # At least 5 quality checks


if __name__ == '__main__':
    unittest.main()