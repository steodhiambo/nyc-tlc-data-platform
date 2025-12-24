# Taxi Zone Enrichment

This document describes the taxi zone enrichment process in the NYC TLC Data Platform, which adds location context to taxi trip data.

## Overview

Taxi zone enrichment adds geographic and administrative context to taxi trip data by joining trip records with NYC taxi zone information. This enables location-based analysis and reporting.

## Taxi Zone Data

### Zone Lookup Table

The taxi zone data contains information about taxi zones in NYC:

```sql
CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Zone Categories

Taxi zones are categorized as:
- **Manhattan**: Central business district
- **Bronx**: Residential and commercial areas
- **Brooklyn**: Residential and commercial areas
- **Queens**: Residential and commercial areas
- **Staten Island**: Residential areas
- **EWR**: Newark Airport

## Enrichment Process

### Data Flow

```mermaid
graph LR
    A[Trip Data with Location IDs] --> B[Join with Zone Lookup]
    B --> C[Enriched Trip Data]
    C --> D[Dimensional Model]
    D --> E[Analytics Tables]
```

### Join Logic

The enrichment process joins trip data with zone information:

```python
def enrich_with_taxi_zones(df):
    """
    Enrich taxi data with taxi zone information
    """
    # Load taxi zone lookup
    zone_lookup = pd.read_sql("SELECT * FROM dim_location", connection)
    
    # Join pickup locations
    df = df.merge(
        zone_lookup.add_suffix('_pickup'),
        left_on='pickup_location_id',
        right_on='location_id_pickup',
        how='left'
    )
    
    # Join dropoff locations
    df = df.merge(
        zone_lookup.add_suffix('_dropoff'),
        left_on='dropoff_location_id',
        right_on='location_id_dropoff',
        how='left'
    )
    
    return df
```

## Enrichment Benefits

### Geographic Analysis

Enrichment enables geographic analysis:
- Trip patterns by borough
- Popular pickup/dropoff locations
- Distance analysis between zones
- Time-based location trends

### Business Intelligence

Enriched data supports business intelligence:
- Revenue by location
- Trip volume by zone
- Peak demand areas
- Service area analysis

## Implementation

### Dimension Model

The enriched data is stored in the dimension model:

```sql
-- Example of enriched fact table structure
CREATE TABLE fact_trips (
    trip_key VARCHAR(50) PRIMARY KEY,
    pickup_location_key INTEGER,
    dropoff_location_key INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(10,2),
    fare_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    -- Other trip attributes
    FOREIGN KEY (pickup_location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (dropoff_location_key) REFERENCES dim_location(location_key)
);
```

### dbt Model

The enrichment is implemented in dbt:

```sql
-- models/core/dim_location.sql
{{ config(materialized='table', schema='core') }}

SELECT
    location_id,
    borough,
    zone,
    service_zone,
    latitude,
    longitude,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ source('raw', 'taxi_zone_lookup') }}
```

## Quality Checks

### Enrichment Validation

Great Expectations validates the enrichment:

```json
{
  "expectation_suite_name": "taxi_zone_enrichment_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "pickup_borough",
        "value_set": ["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island", "EWR"]
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "pickup_location_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "zone",
        "regex_pattern": "^[A-Za-z\\s]+$"
      }
    }
  ]
}
```

## Use Cases

### Location-Based Analytics

The enriched data enables location-based analytics:
- **Heat maps**: Visualize trip density by zone
- **Route analysis**: Understand common trip patterns
- **Demand forecasting**: Predict demand by location
- **Performance metrics**: Track performance by service area

### Regulatory Compliance

Enrichment supports regulatory compliance:
- **Service area reporting**: Track trips by service zones
- **Accessibility metrics**: Analyze service to different areas
- **Equity analysis**: Ensure equitable service across boroughs

## Performance Considerations

### Indexing

The dimension table is indexed for performance:
```sql
CREATE INDEX idx_location_borough ON dim_location(borough);
CREATE INDEX idx_location_zone ON dim_location(zone);
CREATE INDEX idx_location_service_zone ON dim_location(service_zone);
```

### Partitioning

For large datasets, consider partitioning by geographic regions or time periods to improve query performance.