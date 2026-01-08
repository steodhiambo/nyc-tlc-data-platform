"""
Great Expectations Validation Module

This module provides data validation capabilities using Great Expectations
for the NYC TLC Data Platform.
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
from great_expectations.validator.validator import Validator
from great_expectations.dataset import PandasDataset
import logging
from utils.logging_utils import get_pipeline_logger


class DataValidator:
    """
    A class to handle data validation using Great Expectations
    """
    
    def __init__(self, datasource_name: str = "nyc_tlc_postgres"):
        self.datasource_name = datasource_name
        self.logger = get_pipeline_logger("data_validation")
        self.context = self._initialize_data_context()
    
    def _initialize_data_context(self):
        """
        Initialize Great Expectations data context
        """
        try:
            from great_expectations.data_context import DataContext
            
            # Use the existing great_expectations directory
            context = DataContext(
                context_root_dir="/opt/airflow/great_expectations"
            )
            return context
        except Exception as e:
            self.logger.log_error("DataContextInitError", f"Failed to initialize data context: {str(e)}", 
                                "data_validator_init", traceback=str(e))
            raise
    
    def create_expectation_suite(self, suite_name: str, expectations: List[Dict[str, Any]]) -> ExpectationSuite:
        """
        Create an expectation suite from a list of expectation definitions
        
        Args:
            suite_name: Name of the expectation suite
            expectations: List of expectation definitions
            
        Returns:
            ExpectationSuite object
        """
        try:
            suite = ExpectationSuite(expectation_suite_name=suite_name)
            
            for exp_def in expectations:
                expectation = ExpectationConfiguration(
                    expectation_type=exp_def["expectation_type"],
                    kwargs=exp_def["kwargs"],
                    meta=exp_def.get("meta", {})
                )
                suite.add_expectation(expectation)
            
            self.logger.info(f"Created expectation suite: {suite_name}", 
                           task_id="create_expectation_suite", suite_name=suite_name)
            return suite
        except Exception as e:
            self.logger.log_error("ExpectationSuiteCreationError", 
                                f"Failed to create expectation suite {suite_name}: {str(e)}",
                                "create_expectation_suite", suite_name=suite_name, traceback=str(e))
            raise
    
    def validate_dataframe(self, df: pd.DataFrame, expectation_suite: ExpectationSuite) -> Dict[str, Any]:
        """
        Validate a pandas DataFrame against an expectation suite
        
        Args:
            df: DataFrame to validate
            expectation_suite: Expectation suite to validate against
            
        Returns:
            Validation result dictionary
        """
        try:
            # Create a temporary validator for the DataFrame
            dataset = PandasDataset(df)
            dataset._expectation_suite = expectation_suite
            
            # Run validation
            results = dataset.validate()
            
            self.logger.info(f"DataFrame validation completed", 
                           task_id="validate_dataframe", 
                           record_count=len(df),
                           success_count=results.get("success_count", 0),
                           failed_count=results.get("results", []),
                           suite_name=expectation_suite.expectation_suite_name)
            
            return results
        except Exception as e:
            self.logger.log_error("DataFrameValidationError", 
                                f"Failed to validate DataFrame: {str(e)}",
                                "validate_dataframe", traceback=str(e))
            raise
    
    def validate_postgres_table(self, table_name: str, expectation_suite: ExpectationSuite, 
                              schema_name: str = "taxi_data") -> Dict[str, Any]:
        """
        Validate a PostgreSQL table against an expectation suite
        
        Args:
            table_name: Name of the table to validate
            expectation_suite: Expectation suite to validate against
            schema_name: Schema name (default: taxi_data)
            
        Returns:
            Validation result dictionary
        """
        try:
            # Get the datasource
            datasource = self.context.get_datasource(self.datasource_name)
            
            # Create a batch request for the table
            batch_request = {
                "datasource_name": self.datasource_name,
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": f"{schema_name}.{table_name}",
            }
            
            # Create validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite=expectation_suite
            )
            
            # Run validation
            results = validator.validate()
            
            self.logger.info(f"Table validation completed", 
                           task_id="validate_postgres_table", 
                           table_name=table_name,
                           schema_name=schema_name,
                           success_count=results.get("success_count", 0),
                           failed_count=len([r for r in results.get("results", []) if not r.get("success", True)]),
                           suite_name=expectation_suite.expectation_suite_name)
            
            return results
        except Exception as e:
            self.logger.log_error("PostgresTableValidationError", 
                                f"Failed to validate table {schema_name}.{table_name}: {str(e)}",
                                "validate_postgres_table", 
                                table_name=table_name, schema_name=schema_name, traceback=str(e))
            raise
    
    def save_expectation_suite(self, expectation_suite: ExpectationSuite):
        """
        Save an expectation suite to the data context
        
        Args:
            expectation_suite: Expectation suite to save
        """
        try:
            self.context.save_expectation_suite(expectation_suite)
            self.logger.info(f"Saved expectation suite: {expectation_suite.expectation_suite_name}", 
                           task_id="save_expectation_suite", 
                           suite_name=expectation_suite.expectation_suite_name)
        except Exception as e:
            self.logger.log_error("ExpectationSuiteSaveError", 
                                f"Failed to save expectation suite {expectation_suite.expectation_suite_name}: {str(e)}",
                                "save_expectation_suite", 
                                suite_name=expectation_suite.expectation_suite_name, traceback=str(e))
            raise


def get_yellow_taxi_expectations() -> List[Dict[str, Any]]:
    """
    Get default expectations for yellow taxi data
    
    Returns:
        List of expectation definitions
    """
    return [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "tpep_pickup_datetime"
            },
            "meta": {
                "notes": "Pickup datetime should not be null"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "tpep_dropoff_datetime"
            },
            "meta": {
                "notes": "Dropoff datetime should not be null"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "fare_amount",
                "min_value": 0,
                "max_value": 1000  # Adjust based on expected range
            },
            "meta": {
                "notes": "Fare amount should be between 0 and 1000"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "passenger_count",
                "min_value": 0,
                "max_value": 10  # Adjust based on expected range
            },
            "meta": {
                "notes": "Passenger count should be between 0 and 10"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "trip_distance",
                "min_value": 0,
                "max_value": 1000  # Adjust based on expected range
            },
            "meta": {
                "notes": "Trip distance should be between 0 and 1000 miles"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "pickup_longitude",
                "min_value": -74.27,
                "max_value": -73.69
            },
            "meta": {
                "notes": "Pickup longitude should be within NYC bounds"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "pickup_latitude",
                "min_value": 40.49,
                "max_value": 40.92
            },
            "meta": {
                "notes": "Pickup latitude should be within NYC bounds"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "dropoff_longitude",
                "min_value": -74.27,
                "max_value": -73.69
            },
            "meta": {
                "notes": "Dropoff longitude should be within NYC bounds"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "dropoff_latitude",
                "min_value": 40.49,
                "max_value": 40.92
            },
            "meta": {
                "notes": "Dropoff latitude should be within NYC bounds"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "vendor_id"
            },
            "meta": {
                "notes": "Vendor ID should not be null"
            }
        }
    ]


def get_green_taxi_expectations() -> List[Dict[str, Any]]:
    """
    Get default expectations for green taxi data
    
    Returns:
        List of expectation definitions
    """
    # Similar to yellow taxi but may have different fields
    return [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "lpep_pickup_datetime"
            },
            "meta": {
                "notes": "Pickup datetime should not be null"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "lpep_dropoff_datetime"
            },
            "meta": {
                "notes": "Dropoff datetime should not be null"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "fare_amount",
                "min_value": 0,
                "max_value": 1000
            },
            "meta": {
                "notes": "Fare amount should be between 0 and 1000"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "passenger_count",
                "min_value": 0,
                "max_value": 10
            },
            "meta": {
                "notes": "Passenger count should be between 0 and 10"
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "trip_distance",
                "min_value": 0,
                "max_value": 1000
            },
            "meta": {
                "notes": "Trip distance should be between 0 and 1000 miles"
            }
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "vendor_id"
            },
            "meta": {
                "notes": "Vendor ID should not be null"
            }
        }
    ]


def validate_taxi_data(df: pd.DataFrame, data_type: str = "yellow") -> Dict[str, Any]:
    """
    Validate taxi data using appropriate expectations
    
    Args:
        df: DataFrame containing taxi data
        data_type: Type of taxi data ("yellow" or "green")
        
    Returns:
        Validation result dictionary
    """
    validator = DataValidator()
    
    # Get appropriate expectations
    if data_type.lower() == "yellow":
        expectations = get_yellow_taxi_expectations()
        suite_name = "yellow_taxi_data.default"
    elif data_type.lower() == "green":
        expectations = get_green_taxi_expectations()
        suite_name = "green_taxi_data.default"
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")
    
    # Create expectation suite
    suite = validator.create_expectation_suite(suite_name, expectations)
    
    # Validate the DataFrame
    results = validator.validate_dataframe(df, suite)
    
    return results


def validate_taxi_table(table_name: str, data_type: str = "yellow", 
                       schema_name: str = "taxi_data") -> Dict[str, Any]:
    """
    Validate a taxi data table in PostgreSQL
    
    Args:
        table_name: Name of the table to validate
        data_type: Type of taxi data ("yellow" or "green")
        schema_name: Schema name (default: taxi_data)
        
    Returns:
        Validation result dictionary
    """
    validator = DataValidator()
    
    # Get appropriate expectations
    if data_type.lower() == "yellow":
        expectations = get_yellow_taxi_expectations()
        suite_name = f"{data_type}_taxi_data.default"
    elif data_type.lower() == "green":
        expectations = get_green_taxi_expectations()
        suite_name = f"{data_type}_taxi_data.default"
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")
    
    # Create expectation suite
    suite = validator.create_expectation_suite(suite_name, expectations)
    
    # Validate the table
    results = validator.validate_postgres_table(table_name, suite, schema_name)
    
    return results


# Example usage
if __name__ == "__main__":
    # Example of how to use the validation utilities
    import pandas as pd
    import numpy as np
    
    # Create sample data for testing
    sample_data = pd.DataFrame({
        'tpep_pickup_datetime': pd.to_datetime(['2023-01-01 00:00:01', '2023-01-01 00:30:00']),
        'tpep_dropoff_datetime': pd.to_datetime(['2023-01-01 00:15:01', '2023-01-01 00:45:00']),
        'vendor_id': [1, 2],
        'passenger_count': [1, 2],
        'trip_distance': [2.3, 1.8],
        'fare_amount': [10.5, 8.0],
        'pickup_longitude': [-73.9857, -74.0060],
        'pickup_latitude': [40.7589, 40.7306],
        'dropoff_longitude': [-73.9784, -73.9949],
        'dropoff_latitude': [40.7505, 40.7577]
    })
    
    # Validate the sample data
    try:
        results = validate_taxi_data(sample_data, "yellow")
        print(f"Validation results: {results}")
        print(f"Validation success: {results.get('success', False)}")
    except Exception as e:
        print(f"Validation failed: {e}")