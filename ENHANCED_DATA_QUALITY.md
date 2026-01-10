# Enhanced Data Quality Framework

This document describes the enhanced data quality framework implemented for the NYC TLC Data Platform.

## Overview

The enhanced data quality framework includes several advanced features to improve data validation, profiling, lineage tracking, and analytical capabilities:

1. **Advanced Statistical Validation**: Implements multiple outlier detection algorithms and distribution-based validation
2. **Automated Data Profiling**: Detects schema changes and anomalies automatically
3. **Comprehensive Data Lineage**: Tracks data flow through the pipeline
4. **Caching Layer**: Improves performance with Redis caching
5. **Columnar Storage**: Integrates ClickHouse for analytical queries

## Components

### 1. Statistical Validation Suite

The statistical validation suite implements multiple approaches to detect outliers and validate data:

- **Z-Score Method**: Identifies values that are more than 3 standard deviations from the mean
- **Interquartile Range (IQR)**: Detects outliers using the 1.5 * IQR rule
- **Distribution-Based Validation**: Ensures data follows expected distributions
- **Historical Pattern Validation**: Compares current data to historical statistics

### 2. Data Profiling

The data profiling component automatically:

- Detects schema changes (added/removed/modified columns)
- Identifies data type inconsistencies
- Flags columns with high null or duplicate percentages
- Generates comprehensive statistics for each column

### 3. Data Lineage Tracking

The lineage tracking system:

- Records events when datasets are created, read, written, or transformed
- Tracks upstream and downstream dependencies
- Provides impact analysis for changes
- Offers source analysis to understand data origins
- Generates visual lineage graphs

### 4. Caching Layer

The Redis-based caching layer:

- Caches frequently accessed datasets
- Stores query results to avoid recomputation
- Provides decorators for automatic caching
- Includes cache invalidation mechanisms

### 5. ClickHouse Integration

The ClickHouse integration:

- Provides columnar storage for analytical workloads
- Offers fast analytical queries on large datasets
- Includes NYC TLC-specific schema setup
- Enables complex analytical queries with aggregations

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│  Validation &   │───▶│   Data Warehouse│
│                 │    │   Profiling     │    │   (PostgreSQL)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Lineage Tracker │
                       │   (JSON Files)   │
                       └──────────────────┘
                              │
                              ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │   Cache Layer    │───▶│  Analytics DB  │
                       │    (Redis)       │    │  (ClickHouse)  │
                       └──────────────────┘    └─────────────────┘
```

## Usage

### Running Enhanced Data Quality Checks

The enhanced data quality framework is implemented as an Airflow DAG:

```bash
# The DAG runs daily and performs:
# 1. Statistical validation
# 2. Data profiling
# 3. Lineage tracking
# 4. Analytical queries
# 5. Quality reporting
```

### Manual Usage

You can also use the framework programmatically:

```python
from enhanced_data_quality import EnhancedDataQualityFramework

# Initialize the framework
framework = EnhancedDataQualityFramework()

# Run enhanced validation on a table
results = framework.run_enhanced_validation(
    context=ge_context,
    table_name="yellow_tripdata",
    batch_request=batch_request
)

# Get lineage information
lineage_info = framework.get_lineage_info("yellow_tripdata")

# Run analytical queries
analytics = framework.run_analytical_query("""
    SELECT 
        toMonth(pickup_datetime) as month,
        count(*) as trip_count
    FROM nyc_tlc_trips
    WHERE toYear(pickup_datetime) = 2023
    GROUP BY month
    ORDER BY month
""")
```

## Configuration

### Environment Variables

The framework uses the following environment variables:

```bash
# Redis configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=

# ClickHouse configuration
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
```

### Docker Services

The docker-compose.yml includes:

- Redis for caching
- ClickHouse for analytical queries
- Updated environment variable configuration

## Benefits

1. **Improved Data Quality**: Multiple validation layers catch more data issues
2. **Better Performance**: Caching layer speeds up repeated operations
3. **Enhanced Observability**: Comprehensive lineage tracking
4. **Scalable Analytics**: ClickHouse enables complex analytical queries
5. **Automated Monitoring**: Daily quality checks with alerts

## Future Enhancements

- Integration with OpenLineage for standardized lineage tracking
- Machine learning-based anomaly detection
- Real-time data quality monitoring
- Dynamic threshold adjustment based on historical patterns
- Integration with data catalog tools like Marquez or Amundsen