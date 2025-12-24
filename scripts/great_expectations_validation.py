"""
Great Expectations validation for NYC TLC Data Pipeline

This script runs data quality checks using Great Expectations
and integrates with Airflow for monitoring and alerting.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from great_expectations.data_context.types.base import FilesystemStoreBackendDefaults
from great_expectations.validator.validator import Validator

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_data_context_config():
    """
    Create a Great Expectations data context configuration
    """
    return DataContextConfig(
        config_version=3.0,
        datasources={
            "nyc_tlc_postgres": {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
                },
                "data_connectors": {
                    "default_runtime_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    },
                    "default_inferred_data_connector_name": {
                        "class_name": "InferredAssetSqlDataConnector",
                        "name": "whole_table",
                        "include_schema_name": True
                    }
                }
            }
        },
        store_backend_defaults=FilesystemStoreBackendDefaults(
            root_directory="/home/steodhiambo/nyc-tlc-data-platform/great_expectations"
        )
    )


def validate_yellow_taxi_data(table_name: str = "yellow_tripdata") -> Dict:
    """
    Validate yellow taxi data using Great Expectations
    
    Args:
        table_name: Name of the table to validate
        
    Returns:
        Dictionary containing validation results
    """
    try:
        # Create data context
        context = gx.get_context(
            context_root_dir="/home/steodhiambo/nyc-tlc-data-platform/great_expectations"
        )
        
        # Create batch request for the table
        batch_request = {
            "datasource_name": "nyc_tlc_postgres",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": table_name,
        }
        
        # Run validation using checkpoint
        checkpoint_config = {
            "name": f"validate_{table_name}_checkpoint",
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": f"{table_name.split('_')[0]}_tripdata_suite"
                }
            ]
        }
        
        checkpoint = context.add_checkpoint(**checkpoint_config)
        
        # Run validation
        results = checkpoint.run(run_name_template=f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        # Extract validation results
        validation_results = {
            "success": results.success,
            "results": results.run_results,
            "statistics": {
                "evaluated_expectations": results.run_results[list(results.run_results.keys())[0]]["validation_result"].statistics.get("evaluated_expectations", 0),
                "successful_expectations": results.run_results[list(results.run_results.keys())[0]]["validation_result"].statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": results.run_results[list(results.run_results.keys())[0]]["validation_result"].statistics.get("unsuccessful_expectations", 0),
                "success_percent": results.run_results[list(results.run_results.keys())[0]]["validation_result"].statistics.get("success_percent", 0)
            }
        }
        
        logger.info(f"Validation completed for {table_name}: Success={results.success}")
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise


def validate_green_taxi_data(table_name: str = "green_tripdata") -> Dict:
    """
    Validate green taxi data using Great Expectations
    
    Args:
        table_name: Name of the table to validate
        
    Returns:
        Dictionary containing validation results
    """
    return validate_yellow_taxi_data(table_name)


def run_data_quality_checks() -> Dict:
    """
    Run data quality checks for all taxi data tables
    
    Returns:
        Dictionary containing validation results for all tables
    """
    results = {}
    
    # Validate yellow taxi data
    try:
        yellow_results = validate_yellow_taxi_data("yellow_tripdata")
        results["yellow_tripdata"] = yellow_results
        logger.info("Yellow taxi data validation completed")
    except Exception as e:
        logger.error(f"Error validating yellow taxi data: {e}")
        results["yellow_tripdata"] = {"success": False, "error": str(e)}
    
    # Validate green taxi data
    try:
        green_results = validate_green_taxi_data("green_tripdata")
        results["green_tripdata"] = green_results
        logger.info("Green taxi data validation completed")
    except Exception as e:
        logger.error(f"Error validating green taxi data: {e}")
        results["green_tripdata"] = {"success": False, "error": str(e)}
    
    return results


def create_quality_alerts(validation_results: Dict) -> List[Dict]:
    """
    Create alerts based on validation results
    
    Args:
        validation_results: Results from data quality validation
        
    Returns:
        List of alert dictionaries
    """
    alerts = []
    
    for table_name, result in validation_results.items():
        if not result.get("success", False):
            alerts.append({
                "table": table_name,
                "type": "validation_error",
                "message": f"Validation failed for {table_name}: {result.get('error', 'Unknown error')}",
                "severity": "high"
            })
        else:
            stats = result.get("statistics", {})
            success_percent = stats.get("success_percent", 0)
            
            if success_percent < 95:  # Less than 95% success rate
                alerts.append({
                    "table": table_name,
                    "type": "quality_threshold_breached",
                    "message": f"Quality threshold breached for {table_name}: {success_percent:.2f}% success rate",
                    "severity": "medium"
                })
            elif success_percent < 90:  # Less than 90% success rate
                alerts.append({
                    "table": table_name,
                    "type": "quality_threshold_breached",
                    "message": f"Critical quality threshold breached for {table_name}: {success_percent:.2f}% success rate",
                    "severity": "high"
                })
    
    return alerts


def save_validation_results(validation_results: Dict, run_id: str = None):
    """
    Save validation results to database or file system
    
    Args:
        validation_results: Results from data quality validation
        run_id: Optional run identifier
    """
    import json
    from datetime import datetime
    
    if run_id is None:
        run_id = f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Save results to JSON file
    results_file = f"/home/steodhiambo/nyc-tlc-data-platform/great_expectations/validation_results_{run_id}.json"
    
    with open(results_file, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    logger.info(f"Validation results saved to {results_file}")


if __name__ == "__main__":
    # Run data quality checks
    validation_results = run_data_quality_checks()
    
    # Create alerts based on results
    alerts = create_quality_alerts(validation_results)
    
    # Print results
    for table, result in validation_results.items():
        print(f"\nValidation results for {table}:")
        print(f"  Success: {result.get('success', 'N/A')}")
        stats = result.get("statistics", {})
        print(f"  Success Rate: {stats.get('success_percent', 'N/A')}%")
    
    # Print alerts
    if alerts:
        print(f"\nGenerated {len(alerts)} alerts:")
        for alert in alerts:
            print(f"  - {alert['message']} (Severity: {alert['severity']})")
    else:
        print("\nNo alerts generated - all validations passed")
    
    # Save results
    save_validation_results(validation_results)