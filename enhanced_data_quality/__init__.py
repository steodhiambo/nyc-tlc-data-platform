"""
Integration Module for Enhanced Data Quality Framework

This module integrates all the enhanced data quality components:
- Statistical validation
- Data profiling
- Data lineage tracking
- Caching layer (optional)
- ClickHouse integration (optional)
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd

from .statistical_validation_suite import run_statistical_validation, create_dynamic_statistical_suite
from .data_profiling import run_data_profiling
from .data_lineage import DataLineageTracker, create_lineage_decorator

logger = logging.getLogger(__name__)


class EnhancedDataQualityFramework:
    """Main class that integrates all enhanced data quality components"""

    def __init__(self):
        # Initialize required components
        self.lineage_tracker = DataLineageTracker()

        # Initialize optional components
        self.redis_cache = None
        self.dataset_cache = None
        self.query_cache = None
        self.clickhouse_client = None
        self.clickhouse_warehouse = None
        self.clickhouse_available = False

        # Try to initialize Redis cache (optional)
        try:
            from .cache_layer import RedisCache, DatasetCache, QueryResultCache
            self.redis_cache = RedisCache()
            self.dataset_cache = DatasetCache(self.redis_cache)
            self.query_cache = QueryResultCache(self.redis_cache)
            logger.info("Redis cache integration enabled")
        except ImportError:
            logger.warning("Redis not available, skipping cache initialization")

        # Initialize ClickHouse (will connect if available)
        try:
            from .clickhouse_integration import ClickHouseClient, ClickHouseDataWarehouse
            self.clickhouse_client = ClickHouseClient()
            self.clickhouse_warehouse = ClickHouseDataWarehouse(self.clickhouse_client)
            self.clickhouse_available = True
            logger.info("ClickHouse integration enabled")
        except ImportError:
            logger.warning("ClickHouse not available, skipping initialization")
        except Exception as e:
            logger.warning(f"ClickHouse not available: {e}")

    def run_enhanced_validation(
        self,
        context,
        table_name: str,
        batch_request: Dict,
        historical_stats: Dict[str, Any] = None,
        run_profiling: bool = True,
        track_lineage: bool = True
    ) -> Dict[str, Any]:
        """
        Run enhanced validation including statistical validation, profiling, and lineage tracking

        Args:
            context: Great Expectations context
            table_name: Name of the table to validate
            batch_request: Batch request configuration
            historical_stats: Historical statistics for dynamic validation
            run_profiling: Whether to run data profiling
            track_lineage: Whether to track lineage

        Returns:
            Dictionary containing all validation results
        """
        results = {
            'validation_results': None,
            'profiling_results': None,
            'lineage_recorded': None,
            'timestamp': datetime.utcnow().isoformat()
        }

        try:
            # Run statistical validation
            logger.info(f"Running statistical validation for {table_name}")
            validation_results = run_statistical_validation(
                context, table_name, batch_request, historical_stats
            )
            results['validation_results'] = validation_results

            # Run data profiling if requested
            if run_profiling:
                logger.info(f"Running data profiling for {table_name}")
                profiling_results = run_data_profiling(context, table_name, batch_request)
                results['profiling_results'] = profiling_results

            # Record lineage if requested
            if track_lineage:
                lineage_event_id = self.lineage_tracker.record_lineage_event(
                    event_type=self.lineage_tracker.LineageEventType.DATASET_READ,
                    dataset_name=table_name,
                    operation_details={
                        "operation": "enhanced_validation",
                        "validation_results": {
                            "success": validation_results.success if validation_results else False
                        }
                    },
                    job_id=batch_request.get('data_asset_name', 'unknown')
                )
                results['lineage_recorded'] = lineage_event_id

            logger.info(f"Enhanced validation completed for {table_name}")
            return results

        except Exception as e:
            logger.error(f"Error during enhanced validation for {table_name}: {e}")
            raise

    def cache_dataset(self, dataset_name: str, data: Any, expiration: int = 7200) -> bool:
        """
        Cache a dataset using the Redis cache

        Args:
            dataset_name: Name of the dataset
            data: Dataset to cache
            expiration: Cache expiration in seconds

        Returns:
            True if successful, False otherwise
        """
        if self.dataset_cache:
            return self.dataset_cache.cache_dataset(dataset_name, data, expiration)
        else:
            logger.warning("Redis cache not available, skipping dataset cache")
            return False

    def get_cached_dataset(self, dataset_name: str, default: Any = None):
        """
        Get a cached dataset

        Args:
            dataset_name: Name of the dataset
            default: Default value if not found

        Returns:
            Cached dataset or default
        """
        if self.dataset_cache:
            return self.dataset_cache.get_cached_dataset(dataset_name, default)
        else:
            logger.warning("Redis cache not available, returning default")
            return default

    def cache_query_result(self, query_hash: str, result: Any, expiration: int = 3600) -> bool:
        """
        Cache a query result

        Args:
            query_hash: Unique hash of the query
            result: Query result to cache
            expiration: Cache expiration in seconds

        Returns:
            True if successful, False otherwise
        """
        if self.query_cache:
            return self.query_cache.cache_query_result(query_hash, result, expiration)
        else:
            logger.warning("Redis cache not available, skipping query result cache")
            return False

    def get_cached_query_result(self, query_hash: str, default: Any = None):
        """
        Get a cached query result

        Args:
            query_hash: Unique hash of the query
            default: Default value if not found

        Returns:
            Cached query result or default
        """
        if self.query_cache:
            return self.query_cache.get_cached_query_result(query_hash, default)
        else:
            logger.warning("Redis cache not available, returning default")
            return default

    def load_data_to_clickhouse(self, df: pd.DataFrame, table_name: str) -> bool:
        """
        Load data to ClickHouse if available

        Args:
            df: DataFrame to load
            table_name: Target table name

        Returns:
            True if successful, False otherwise
        """
        if not self.clickhouse_available:
            logger.warning("ClickHouse not available, skipping data load")
            return False

        try:
            success = self.clickhouse_warehouse.load_trip_data(df, table_name)
            if success:
                # Record lineage event
                self.lineage_tracker.record_lineage_event(
                    event_type=self.lineage_tracker.LineageEventType.DATASET_WRITTEN,
                    dataset_name=table_name,
                    operation_details={
                        "operation": "load_to_clickhouse",
                        "rows_loaded": len(df)
                    },
                    upstream_datasets=[f"source_{table_name}"]
                )
            return success
        except Exception as e:
            logger.error(f"Error loading data to ClickHouse: {e}")
            return False

    def run_analytical_query(self, query: str, use_cache: bool = True) -> pd.DataFrame:
        """
        Run an analytical query with optional caching

        Args:
            query: SQL query to execute
            use_cache: Whether to use query result caching

        Returns:
            Query results as DataFrame
        """
        if not self.clickhouse_available:
            logger.warning("ClickHouse not available, returning empty DataFrame")
            return pd.DataFrame()

        import hashlib

        # Create query hash for caching
        query_hash = hashlib.md5(query.encode()).hexdigest()

        # Try to get from cache first
        if use_cache and self.query_cache:
            cached_result = self.get_cached_query_result(query_hash)
            if cached_result is not None:
                logger.debug("Retrieved query result from cache")
                return cached_result

        try:
            # Execute query
            result_df = self.clickhouse_warehouse.run_analytical_query(query)

            # Cache result if caching enabled
            if use_cache and self.query_cache and not result_df.empty:
                self.cache_query_result(query_hash, result_df)

            return result_df
        except Exception as e:
            logger.error(f"Error running analytical query: {e}")
            return pd.DataFrame()

    def get_lineage_info(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get lineage information for a dataset

        Args:
            dataset_name: Name of the dataset

        Returns:
            Lineage information
        """
        return self.lineage_tracker.get_lineage(dataset_name)

    def get_impact_analysis(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get impact analysis for a dataset

        Args:
            dataset_name: Name of the dataset

        Returns:
            Impact analysis results
        """
        return self.lineage_tracker.get_impact_analysis(dataset_name)

    def get_source_analysis(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get source analysis for a dataset

        Args:
            dataset_name: Name of the dataset

        Returns:
            Source analysis results
        """
        return self.lineage_tracker.get_source_analysis(dataset_name)

    def setup_nyc_tlc_schema(self) -> bool:
        """
        Set up NYC TLC schema in ClickHouse if available

        Returns:
            True if successful, False otherwise
        """
        if not self.clickhouse_available:
            logger.warning("ClickHouse not available, skipping schema setup")
            return False

        return self.clickhouse_warehouse.setup_nyc_tlc_schema()


def create_framework_from_env():
    """Create an EnhancedDataQualityFramework instance using environment variables"""
    return EnhancedDataQualityFramework()


# Decorator for automatically tracking lineage in data processing functions
def track_lineage(
    dataset_name: str,
    operation_type: str,
    upstream_datasets: List[str] = None,
    downstream_datasets: List[str] = None
):
    """
    Decorator to automatically track lineage for data processing functions

    Args:
        dataset_name: Name of the dataset being processed
        operation_type: Type of operation being performed
        upstream_datasets: List of upstream datasets
        downstream_datasets: List of downstream datasets
    """
    framework = EnhancedDataQualityFramework()
    lineage_decorator = create_lineage_decorator(framework.lineage_tracker)

    return lineage_decorator(
        dataset_name=dataset_name,
        operation_type=operation_type,
        upstream_datasets=upstream_datasets,
        downstream_datasets=downstream_datasets
    )


# Example usage functions
def process_nyc_tlc_data_with_enhanced_quality(df: pd.DataFrame, table_name: str):
    """
    Example function showing how to use the enhanced data quality framework

    Args:
        df: Input DataFrame
        table_name: Name of the table being processed
    """
    framework = EnhancedDataQualityFramework()

    # Cache the input dataset
    framework.cache_dataset(f"raw_{table_name}", df)

    # Load to ClickHouse for analytical processing
    if framework.clickhouse_available:
        framework.load_data_to_clickhouse(df, f"staging_{table_name}")

    # Perform analytical queries
    if framework.clickhouse_available:
        # Example: Get daily trip counts
        daily_counts = framework.run_analytical_query(
            f"""
            SELECT
                toDate(pickup_datetime) as date,
                count(*) as trip_count
            FROM staging_{table_name}
            GROUP BY date
            ORDER BY date
            LIMIT 10
            """
        )
        print("Daily trip counts:", daily_counts.head())

    # Record lineage
    framework.lineage_tracker.record_lineage_event(
        event_type=framework.lineage_tracker.LineageEventType.TRANSFORMATION_APPLIED,
        dataset_name=table_name,
        operation_details={
            "operation": "data_processing",
            "rows_processed": len(df),
            "columns": list(df.columns)
        },
        upstream_datasets=[f"raw_{table_name}"],
        downstream_datasets=[f"processed_{table_name}"]
    )

    return df  # Return processed data


def run_comprehensive_data_quality_check(table_name: str):
    """
    Run a comprehensive data quality check using all enhanced features

    Args:
        table_name: Name of the table to check
    """
    import great_expectations as gx

    # Get Great Expectations context
    context = gx.get_context(
        context_root_dir="/home/steodhiambo/nyc-tlc-data-platform/great_expectations"
    )

    # Create batch request
    batch_request = {
        "datasource_name": "nyc_tlc_postgres",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": table_name,
    }

    # Initialize framework
    framework = EnhancedDataQualityFramework()

    # Run enhanced validation
    results = framework.run_enhanced_validation(
        context=context,
        table_name=table_name,
        batch_request=batch_request,
        run_profiling=True,
        track_lineage=True
    )

    # Print results summary
    print(f"Comprehensive data quality check for {table_name}:")
    print(f"  - Validation success: {results['validation_results'].success if results['validation_results'] else 'N/A'}")
    print(f"  - Profiling completed: {results['profiling_results'] is not None}")
    print(f"  - Lineage recorded: {results['lineage_recorded'] is not None}")

    # Get lineage info
    lineage_info = framework.get_lineage_info(table_name)
    print(f"  - Upstream datasets: {len(lineage_info['upstream_datasets'])}")
    print(f"  - Downstream datasets: {len(lineage_info['downstream_datasets'])}")

    return results