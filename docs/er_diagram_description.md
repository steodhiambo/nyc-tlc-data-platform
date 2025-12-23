# Entity Relationship Diagram for NYC TLC Data Warehouse

## Star Schema Design

### Fact Table
**trips_fact** (Central fact table)
- trip_key (PK) - Surrogate key
- vendor_key (FK) → vendor_dim
- pickup_location_key (FK) → location_dim
- dropoff_location_key (FK) → location_dim
- pickup_time_key (FK) → time_dim
- dropoff_time_key (FK) → time_dim
- payment_key (FK) → payment_dim
- rate_code_key (FK) → rate_code_dim
- trip_type_key (FK) → trip_type_dim
- [Measures: fare_amount, trip_distance, passenger_count, etc.]

### Dimension Tables

**location_dim**
- location_key (PK) - Surrogate key
- location_id (Natural Key) - Original TLC location ID
- borough
- zone
- service_zone
- latitude
- longitude

**time_dim** 
- time_key (PK) - Surrogate key
- full_date
- year, quarter, month, day_of_month
- day_of_week, hour_of_day, minute_of_hour
- is_weekend, is_holiday

**vendor_dim**
- vendor_key (PK) - Surrogate key
- vendor_id (Natural Key)
- vendor_name
- vendor_description

**payment_dim**
- payment_key (PK) - Surrogate key
- payment_type_id (Natural Key)
- payment_type_description
- payment_type_category

**rate_code_dim**
- rate_code_key (PK) - Surrogate key
- rate_code_id (Natural Key)
- rate_code_description

**trip_type_dim**
- trip_type_key (PK) - Surrogate key
- trip_type_id (Natural Key)
- trip_type_description

## Relationships

```
    location_dim (1) ← → (M) trips_fact ← → (M) location_dim
         ↑                    ↑                    ↑
    pickup FK           pickup_loc FK        dropoff_loc FK
                          ↓
                     time_dim (1) ← → (M) trips_fact ← → (M) time_dim
                          ↑                    ↑                    ↑
                      pickup_time FK      pickup_time FK      dropoff_time FK
                          ↓                    ↓                    ↓
                   vendor_dim (1) ← → (M) trips_fact ← → (M) payment_dim
                        ↑                      ↑                      ↑
                   vendor_key              vendor_key           payment_key
```

## Data Flow

Raw Data → Staging → Core Dimensional Model → Semantic Layer → Analytics

The star schema design allows for efficient querying of taxi trip data with dimensions for location, time, payment, vendor, and trip characteristics.