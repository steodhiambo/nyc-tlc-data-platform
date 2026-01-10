"""
Data Profiling Module for NYC TLC Data Platform

This module implements automatic data profiling to detect schema changes,
anomalies, and generate profiling reports.
"""

import pandas as pd
import great_expectations as gx
from typing import Dict, Any, List
import logging
from datetime import datetime
import json
import hashlib

logger = logging.getLogger(__name__)


class DataProfiler:
    """Automatic data profiler for detecting schema changes and anomalies"""

    def __init__(self, context):
        self.context = context

    def profile_dataset(self, batch_request: Dict, auto_identify_changes: bool = True):
        """
        Profile a dataset and detect schema changes or anomalies

        Args:
            batch_request: Batch request configuration
            auto_identify_changes: Whether to automatically identify schema changes
        """
        try:
            # Get validator for the dataset
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=f"profile_{batch_request['data_asset_name']}_suite"
            )

            # Generate profiling results
            profiling_results = self._generate_profile(validator)

            # Detect schema changes if requested
            if auto_identify_changes:
                schema_changes = self._detect_schema_changes(
                    batch_request['data_asset_name'],
                    profiling_results['columns']
                )

                profiling_results['schema_changes'] = schema_changes

            # Detect anomalies
            anomalies = self._detect_anomalies(profiling_results)
            profiling_results['anomalies'] = anomalies

            return profiling_results

        except Exception as e:
            logger.error(f"Error during data profiling: {str(e)}")
            raise

    def _generate_profile(self, validator):
        """Generate data profile using Great Expectations profiler"""
        # Get basic statistics about the dataset
        profile = {
            'dataset_info': {
                'row_count': validator.get_metric(metric_name="table.row_count"),
                'column_count': len(validator.columns()),
                'profile_timestamp': datetime.now().isoformat()
            },
            'columns': {},
            'data_types': {},
            'missing_values': {},
            'duplicates': {}
        }

        # Profile each column
        for column in validator.columns():
            try:
                # Get column statistics
                column_profile = {
                    'type': self._infer_column_type(validator, column),
                    'non_null_count': validator.get_metric(
                        metric_name="column_values.nonnull.count",
                        column=column
                    ),
                    'null_count': validator.get_metric(
                        metric_name="column_values.null.count",
                        column=column
                    ),
                    'unique_count': validator.get_metric(
                        metric_name="column.distinct_values.count",
                        column=column
                    ),
                    'duplicate_count': validator.get_metric(
                        metric_name="column_values.duplicate.count",
                        column=column
                    )
                }

                # Add statistics for numeric columns
                if self._is_numeric_column(validator, column):
                    column_profile.update({
                        'mean': validator.get_metric(
                            metric_name="column.mean",
                            column=column,
                            ignore_row_if="all_values_are_missing"
                        ),
                        'std': validator.get_metric(
                            metric_name="column.standard_deviation",
                            column=column,
                            ignore_row_if="all_values_are_missing"
                        ),
                        'min': validator.get_metric(
                            metric_name="column.min",
                            column=column,
                            ignore_row_if="all_values_are_missing"
                        ),
                        'max': validator.get_metric(
                            metric_name="column.max",
                            column=column,
                            ignore_row_if="all_values_are_missing"
                        ),
                        'quantiles': validator.get_metric(
                            metric_name="column.quantile_values",
                            column=column,
                            quantiles=[0.25, 0.5, 0.75],
                            ignore_row_if="all_values_are_missing"
                        )
                    })

                # Add statistics for string columns
                if self._is_string_column(validator, column):
                    column_profile.update({
                        'avg_length': validator.get_metric(
                            metric_name="column.string.length.average_value",
                            column=column
                        ),
                        'min_length': validator.get_metric(
                            metric_name="column.string.length.min_value",
                            column=column
                        ),
                        'max_length': validator.get_metric(
                            metric_name="column.string.length.max_value",
                            column=column
                        )
                    })

                profile['columns'][column] = column_profile

            except Exception as e:
                logger.warning(f"Could not profile column {column}: {str(e)}")
                continue

        return profile

    def _infer_column_type(self, validator, column: str) -> str:
        """Infer the data type of a column"""
        try:
            # Try to get the type by inspecting sample values
            sample_values = validator.get_metric(
                metric_name="column.sample_values",
                column=column,
                n_rows=10
            )

            if sample_values:
                # Check if all values are numeric
                numeric_count = sum(1 for val in sample_values if pd.api.types.is_numeric_dtype(type(val)) or str(val).replace('.', '').replace('-', '').isdigit())
                if numeric_count / len(sample_values) > 0.8:  # 80% numeric values
                    return 'numeric'

                # Check if all values look like dates
                date_count = 0
                for val in sample_values:
                    try:
                        pd.to_datetime(str(val))
                        date_count += 1
                    except:
                        pass

                if date_count / len(sample_values) > 0.8:  # 80% date-like values
                    return 'datetime'

                return 'string'

            return 'unknown'
        except:
            return 'unknown'

    def _is_numeric_column(self, validator, column: str) -> bool:
        """Check if a column is numeric"""
        col_type = self._infer_column_type(validator, column)
        return col_type == 'numeric'

    def _is_string_column(self, validator, column: str) -> bool:
        """Check if a column is string"""
        col_type = self._infer_column_type(validator, column)
        return col_type == 'string'

    def _detect_schema_changes(self, table_name: str, current_columns: Dict) -> Dict:
        """Detect schema changes compared to previous profiles"""
        # Load previous schema if available
        previous_schema = self._load_previous_schema(table_name)

        changes = {
            'added_columns': [],
            'removed_columns': [],
            'modified_columns': [],
            'data_type_changes': []
        }

        if previous_schema:
            current_col_names = set(current_columns.keys())
            previous_col_names = set(previous_schema.keys())

            # Find added and removed columns
            changes['added_columns'] = list(current_col_names - previous_col_names)
            changes['removed_columns'] = list(previous_col_names - current_col_names)

            # Find modified columns (same name, different characteristics)
            common_columns = current_col_names.intersection(previous_col_names)
            for col in common_columns:
                prev_col = previous_schema[col]
                curr_col = current_columns[col]

                # Check for data type changes
                if prev_col.get('type') != curr_col.get('type'):
                    changes['data_type_changes'].append({
                        'column': col,
                        'previous_type': prev_col.get('type'),
                        'current_type': curr_col.get('type')
                    })

                # Check for significant changes in null percentages
                prev_non_null = prev_col.get('non_null_count', 0)
                prev_total = prev_col.get('non_null_count', 0) + prev_col.get('null_count', 0)
                curr_non_null = curr_col.get('non_null_count', 0)
                curr_total = curr_col.get('non_null_count', 0) + curr_col.get('null_count', 0)

                if prev_total > 0 and curr_total > 0:
                    prev_null_pct = (prev_total - prev_non_null) / prev_total
                    curr_null_pct = (curr_total - curr_non_null) / curr_total

                    # If null percentage changed significantly (>10%), mark as modified
                    if abs(prev_null_pct - curr_null_pct) > 0.1:
                        changes['modified_columns'].append({
                            'column': col,
                            'change_type': 'null_percentage_change',
                            'previous': f"{prev_null_pct:.2%}",
                            'current': f"{curr_null_pct:.2%}"
                        })

        # Save current schema for future comparisons
        self._save_current_schema(table_name, current_columns)

        return changes

    def _load_previous_schema(self, table_name: str) -> Dict:
        """Load previous schema for comparison"""
        try:
            schema_file = f"/home/steodhiambo/nyc-tlc-data-platform/great_expectations/schemas/{table_name}_schema.json"
            with open(schema_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def _save_current_schema(self, table_name: str, schema: Dict):
        """Save current schema for future comparisons"""
        import os
        schema_dir = "/home/steodhiambo/nyc-tlc-data-platform/great_expectations/schemas"
        os.makedirs(schema_dir, exist_ok=True)

        schema_file = f"{schema_dir}/{table_name}_schema.json"
        with open(schema_file, 'w') as f:
            json.dump(schema, f, indent=2, default=str)

    def _detect_anomalies(self, profile: Dict) -> List[Dict]:
        """Detect anomalies in the data profile"""
        anomalies = []

        # Check for significant changes in row count
        if profile['dataset_info']['row_count'] == 0:
            anomalies.append({
                'type': 'empty_dataset',
                'severity': 'critical',
                'message': 'Dataset contains no rows'
            })
        elif profile['dataset_info']['row_count'] < 10:  # Arbitrary threshold
            anomalies.append({
                'type': 'very_small_dataset',
                'severity': 'high',
                'message': f'Dataset contains only {profile["dataset_info"]["row_count"]} rows'
            })

        # Check for columns with too many nulls
        for col_name, col_profile in profile['columns'].items():
            total_count = col_profile['non_null_count'] + col_profile['null_count']
            if total_count > 0:
                null_percentage = col_profile['null_count'] / total_count
                if null_percentage > 0.9:  # More than 90% null
                    anomalies.append({
                        'type': 'high_null_percentage',
                        'severity': 'high',
                        'message': f'Column {col_name} has {null_percentage:.2%} null values',
                        'column': col_name
                    })

        # Check for columns with too many duplicates
        for col_name, col_profile in profile['columns'].items():
            total_count = col_profile['non_null_count'] + col_profile['null_count']
            if total_count > 0:
                duplicate_percentage = col_profile['duplicate_count'] / total_count
                if duplicate_percentage > 0.9:  # More than 90% duplicates
                    anomalies.append({
                        'type': 'high_duplicate_percentage',
                        'severity': 'medium',
                        'message': f'Column {col_name} has {duplicate_percentage:.2%} duplicate values',
                        'column': col_name
                    })

        # Check for unexpected data types
        expected_types = {
            'pickup_datetime': ['datetime'],
            'dropoff_datetime': ['datetime'],
            'fare_amount': ['numeric'],
            'trip_distance': ['numeric'],
            'passenger_count': ['numeric'],
            'pickup_location_id': ['numeric'],
            'dropoff_location_id': ['numeric']
        }

        for col_name, col_profile in profile['columns'].items():
            if col_name in expected_types:
                actual_type = col_profile.get('type')
                expected_type_list = expected_types[col_name]

                if actual_type not in expected_type_list:
                    anomalies.append({
                        'type': 'unexpected_data_type',
                        'severity': 'high',
                        'message': f'Column {col_name} has unexpected type {actual_type}, expected one of {expected_type_list}',
                        'column': col_name
                    })

        return anomalies


def run_data_profiling(context, table_name: str, batch_request: Dict):
    """
    Run data profiling on a table

    Args:
        context: Great Expectations context
        table_name: Name of the table to profile
        batch_request: Batch request configuration
    """
    profiler = DataProfiler(context)

    # Run profiling
    profile_results = profiler.profile_dataset(batch_request)

    # Log any detected anomalies
    anomalies = profile_results.get('anomalies', [])
    if anomalies:
        logger.warning(f"Detected {len(anomalies)} anomalies in {table_name}:")
        for anomaly in anomalies:
            logger.warning(f"  - {anomaly['message']} (Severity: {anomaly['severity']})")

    # Log any schema changes
    schema_changes = profile_results.get('schema_changes', {})
    change_count = sum(len(changes) for changes in schema_changes.values())
    if change_count > 0:
        logger.info(f"Detected {change_count} schema changes in {table_name}")
        for change_type, changes in schema_changes.items():
            if changes:
                logger.info(f"  {change_type}: {changes}")

    return profile_results