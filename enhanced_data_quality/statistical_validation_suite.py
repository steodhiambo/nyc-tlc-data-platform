"""
Statistical Validation Suite for NYC TLC Data Platform

This module implements advanced statistical validation techniques including:
- Outlier detection using multiple algorithms
- Distribution-based validation
- Historical pattern-based validation
"""

import pandas as pd
import numpy as np
from scipy import stats
import great_expectations as gx
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class StatisticalValidationSuite:
    """Advanced statistical validation suite for NYC TLC data"""

    def __init__(self, context):
        self.context = context

    def create_advanced_expectation_suite(self, table_name: str, suite_name: str = None):
        """
        Create an advanced expectation suite with statistical validation

        Args:
            table_name: Name of the table to validate
            suite_name: Name for the expectation suite
        """
        if suite_name is None:
            suite_name = f"{table_name}_statistical_suite"

        # Create expectation suite
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )

        # Add basic expectations
        self._add_basic_statistical_expectations(suite, table_name)

        # Add outlier detection expectations
        self._add_outlier_detection_expectations(suite)

        # Add distribution-based expectations
        self._add_distribution_based_expectations(suite)

        # Add historical pattern expectations
        self._add_historical_pattern_expectations(suite)

        return suite

    def _add_basic_statistical_expectations(self, suite, table_name: str):
        """Add basic statistical expectations"""
        # Table-level expectations
        suite.add_expectation(
            gx.expectations.ExpectTableRowcountToBeBetween(
                min_value=1
            )
        )

        # Column-level statistical expectations
        numeric_columns = ['fare_amount', 'trip_distance', 'passenger_count', 'total_amount']

        for col in numeric_columns:
            # Expect values to be within reasonable bounds based on historical data
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=0,
                    mostly=0.95  # At least 95% of values should meet this expectation
                )
            )

            # Expect most values to not be null
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=col,
                    mostly=0.98  # At least 98% of values should not be null
                )
            )

    def _add_outlier_detection_expectations(self, suite):
        """Add outlier detection expectations using multiple algorithms"""
        # Z-score based outlier detection
        numeric_columns = ['fare_amount', 'trip_distance', 'passenger_count', 'total_amount']

        for col in numeric_columns:
            # Using z-score method with threshold of 3 (standard for outlier detection)
            suite.add_expectation(
                gx.expectations.ExpectColumnValueZScoresToBeLessThan(
                    column=col,
                    threshold=3.0,
                    double_sided=True,
                    mostly=0.99  # Allow up to 1% of values to exceed threshold
                )
            )

        # IQR-based outlier detection
        for col in numeric_columns:
            # Custom expectation for IQR-based outlier detection
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    # These values would typically be calculated dynamically based on historical data
                    min_value=self._calculate_iqr_bounds(col)[0],
                    max_value=self._calculate_iqr_bounds(col)[1],
                    mostly=0.99
                )
            )

    def _add_distribution_based_expectations(self, suite):
        """Add distribution-based validation expectations"""
        # Expect fare amounts to follow a reasonable distribution
        suite.add_expectation(
            gx.expectations.ExpectColumnMostCommonValueToBeInSet(
                column="payment_type",
                value_set=[1, 2, 3, 4, 5, 6],
                mostly=0.99
            )
        )

        # Expect passenger count to be in reasonable range
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="passenger_count",
                min_value=1,
                max_value=6,
                mostly=0.99
            )
        )

        # Expect trip distance to have reasonable distribution
        suite.add_expectation(
            gx.expectations.ExpectColumnQuantileValuesToBeBetween(
                column="trip_distance",
                quantile_ranges={
                    "quantiles": [0.25, 0.5, 0.75, 0.95],
                    "value_ranges": [
                        [0, 1],      # 25th percentile: 0-1 miles
                        [0, 2],      # 50th percentile: 0-2 miles
                        [0, 5],      # 75th percentile: 0-5 miles
                        [0, 20]      # 95th percentile: 0-20 miles
                    ]
                }
            )
        )

    def _add_historical_pattern_expectations(self, suite):
        """Add expectations based on historical patterns"""
        # Expect datetime columns to have reasonable ranges
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="pickup_datetime",
                min_value="2009-01-01",
                max_value="2025-12-31",
                parse_strings_as_datetimes=True
            )
        )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="dropoff_datetime",
                min_value="2009-01-01",
                max_value="2025-12-31",
                parse_strings_as_datetimes=True
            )
        )

        # Expect fare amount to have reasonable correlation with trip distance
        suite.add_expectation(
            gx.expectations.ExpectColumnPairValuesAToBeGreaterThanOrEqualThanB(
                column_A="fare_amount",
                column_B="trip_distance",
                ignore_row_if="either_value_is_missing",
                mostly=0.8  # At least 80% of trips should have fare >= distance
            )
        )

    def _calculate_iqr_bounds(self, column: str) -> tuple:
        """
        Calculate IQR bounds for outlier detection
        Note: In practice, these would be calculated based on historical data
        """
        # Placeholder values - in real implementation, these would come from historical statistics
        if column == 'fare_amount':
            q1, q3 = 5.0, 25.0
        elif column == 'trip_distance':
            q1, q3 = 0.5, 3.0
        elif column == 'passenger_count':
            q1, q3 = 1.0, 2.0
        elif column == 'total_amount':
            q1, q3 = 6.0, 30.0
        else:
            q1, q3 = 0.0, 10.0

        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        return (max(0, lower_bound), upper_bound)  # Ensure non-negative bounds for monetary values


def create_dynamic_statistical_suite(context, table_name: str, historical_stats: Dict[str, Any] = None):
    """
    Create a dynamic statistical validation suite based on historical data patterns

    Args:
        context: Great Expectations context
        table_name: Name of the table to validate
        historical_stats: Historical statistics to use for dynamic validation thresholds
    """
    suite = StatisticalValidationSuite(context)

    # Create the basic statistical suite
    advanced_suite = suite.create_advanced_expectation_suite(table_name)

    # If historical stats provided, add dynamic expectations
    if historical_stats:
        add_dynamic_expectations(advanced_suite, historical_stats)

    return advanced_suite


def add_dynamic_expectations(suite, historical_stats: Dict[str, Any]):
    """
    Add expectations based on historical statistics

    Args:
        suite: The expectation suite to add to
        historical_stats: Dictionary containing historical statistics
    """
    # Add expectations based on historical mean and std dev
    for column, stats in historical_stats.items():
        if 'mean' in stats and 'std' in stats:
            # Calculate bounds based on historical mean +/- 3 std devs
            lower_bound = stats['mean'] - 3 * stats['std']
            upper_bound = stats['mean'] + 3 * stats['std']

            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=column,
                    min_value=lower_bound,
                    max_value=upper_bound,
                    mostly=0.995  # Allow for 0.5% of values to be outliers
                )
            )

        # Add expectations based on historical percentiles
        if 'percentiles' in stats:
            p25, p75 = stats['percentiles'].get(25), stats['percentiles'].get(75)
            if p25 and p75:
                iqr = p75 - p25
                lower_fence = p25 - 1.5 * iqr
                upper_fence = p75 + 1.5 * iqr

                suite.add_expectation(
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column,
                        min_value=lower_fence,
                        max_value=upper_fence,
                        mostly=0.99
                    )
                )


def run_statistical_validation(context, table_name: str, batch_request: Dict, historical_stats: Dict[str, Any] = None):
    """
    Run statistical validation on a table

    Args:
        context: Great Expectations context
        table_name: Name of the table to validate
        batch_request: Batch request configuration
        historical_stats: Historical statistics for dynamic validation
    """
    # Create the statistical validation suite
    suite = create_dynamic_statistical_suite(context, table_name, historical_stats)

    # Create checkpoint
    checkpoint_config = {
        "name": f"statistical_validation_{table_name}",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite.expectation_suite_name
            }
        ]
    }

    checkpoint = context.add_checkpoint(**checkpoint_config)

    # Run validation
    results = checkpoint.run()

    return results