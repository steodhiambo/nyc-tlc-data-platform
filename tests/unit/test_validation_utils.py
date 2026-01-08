"""
Unit tests for validation utilities
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from utils.validation_utils import (
    DataValidator,
    validate_taxi_data,
    get_yellow_taxi_expectations,
    get_green_taxi_expectations
)


class TestDataValidator(unittest.TestCase):
    """Test cases for the DataValidator class"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the Great Expectations context initialization
        with patch('utils.validation_utils.DataContext') as mock_context_class:
            mock_context = Mock()
            mock_context_class.return_value = mock_context
            self.validator = DataValidator()
            self.validator.context = mock_context
    
    def test_get_yellow_taxi_expectations(self):
        """Test that yellow taxi expectations are properly defined"""
        expectations = get_yellow_taxi_expectations()
        
        # Check that we have the expected number of expectations
        self.assertGreater(len(expectations), 0)
        
        # Check that each expectation has the required fields
        for exp in expectations:
            self.assertIn('expectation_type', exp)
            self.assertIn('kwargs', exp)
            self.assertIsInstance(exp['kwargs'], dict)
    
    def test_get_green_taxi_expectations(self):
        """Test that green taxi expectations are properly defined"""
        expectations = get_green_taxi_expectations()
        
        # Check that we have the expected number of expectations
        self.assertGreater(len(expectations), 0)
        
        # Check that each expectation has the required fields
        for exp in expectations:
            self.assertIn('expectation_type', exp)
            self.assertIn('kwargs', exp)
            self.assertIsInstance(exp['kwargs'], dict)
    
    @patch('utils.validation_utils.PandasDataset')
    def test_validate_dataframe(self, mock_pandas_dataset):
        """Test DataFrame validation"""
        # Create a mock dataset
        mock_dataset = Mock()
        mock_dataset.validate.return_value = {
            'success': True,
            'results': [],
            'statistics': {'evaluated_expectations': 0, 'successful_expectations': 0}
        }
        mock_pandas_dataset.return_value = mock_dataset
        
        # Create test data
        test_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'vendor_id': [1],
            'passenger_count': [1],
            'trip_distance': [2.3],
            'fare_amount': [10.5],
            'pickup_longitude': [-73.9857],
            'pickup_latitude': [40.7589],
            'dropoff_longitude': [-73.9784],
            'dropoff_latitude': [40.7505]
        })
        
        # Create a mock expectation suite
        mock_suite = Mock()
        mock_suite.expectation_suite_name = 'test_suite'
        
        # Call the method
        result = self.validator.validate_dataframe(test_df, mock_suite)
        
        # Assertions
        self.assertIsNotNone(result)
        mock_pandas_dataset.assert_called_once_with(test_df)
        mock_dataset.validate.assert_called_once()
    
    def test_create_expectation_suite(self):
        """Test creation of expectation suite"""
        expectations = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "test_column"},
                "meta": {"notes": "Test expectation"}
            }
        ]
        
        suite = self.validator.create_expectation_suite("test_suite", expectations)
        
        self.assertEqual(suite.expectation_suite_name, "test_suite")
        self.assertEqual(len(suite.expectations), 1)


class TestValidateTaxiData(unittest.TestCase):
    """Test cases for the validate_taxi_data function"""
    
    @patch('utils.validation_utils.DataValidator')
    def test_validate_yellow_taxi_data(self, mock_validator_class):
        """Test validation of yellow taxi data"""
        # Create mocks
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        
        mock_suite = Mock()
        mock_validator.create_expectation_suite.return_value = mock_suite
        
        mock_result = {'success': True, 'results': []}
        mock_validator.validate_dataframe.return_value = mock_result
        
        # Create test data
        test_df = pd.DataFrame({
            'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'vendor_id': [1],
            'passenger_count': [1],
            'trip_distance': [2.3],
            'fare_amount': [10.5]
        })
        
        # Call the function
        result = validate_taxi_data(test_df, "yellow")
        
        # Assertions
        self.assertEqual(result, mock_result)
        mock_validator.create_expectation_suite.assert_called_once()
        mock_validator.validate_dataframe.assert_called_once_with(test_df, mock_suite)
    
    @patch('utils.validation_utils.DataValidator')
    def test_validate_green_taxi_data(self, mock_validator_class):
        """Test validation of green taxi data"""
        # Create mocks
        mock_validator = Mock()
        mock_validator_class.return_value = mock_validator
        
        mock_suite = Mock()
        mock_validator.create_expectation_suite.return_value = mock_suite
        
        mock_result = {'success': True, 'results': []}
        mock_validator.validate_dataframe.return_value = mock_result
        
        # Create test data
        test_df = pd.DataFrame({
            'lpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01']),
            'lpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01']),
            'vendor_id': [1],
            'passenger_count': [1],
            'trip_distance': [2.3],
            'fare_amount': [10.5]
        })
        
        # Call the function
        result = validate_taxi_data(test_df, "green")
        
        # Assertions
        self.assertEqual(result, mock_result)
        mock_validator.create_expectation_suite.assert_called_once()
        mock_validator.validate_dataframe.assert_called_once_with(test_df, mock_suite)
    
    def test_validate_taxi_data_invalid_type(self):
        """Test validation with invalid data type"""
        test_df = pd.DataFrame({'col1': [1, 2, 3]})
        
        with self.assertRaises(ValueError) as context:
            validate_taxi_data(test_df, "invalid_type")
        
        self.assertIn("Unsupported data_type", str(context.exception))


if __name__ == '__main__':
    unittest.main()