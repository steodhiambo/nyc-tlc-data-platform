-- models/core/fct_yellow_taxi_trips.sql
{{ config(
    materialized='table',
    schema='core'
) }}

-- Core fact table for yellow taxi trips with enhanced features computed during ELT
SELECT
    -- Surrogate key generation
    {{ dbt_utils.generate_surrogate_key(['pickup_datetime', 'dropoff_datetime', 'vendor_id']) }} AS trip_id,
    
    -- Original identifiers
    vendor_id,
    
    -- Time dimensions
    pickup_datetime,
    dropoff_datetime,
    EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    EXTRACT(DOW FROM pickup_datetime) AS pickup_day_of_week,
    
    -- Duration and timing
    trip_duration_minutes,
    CASE 
        WHEN trip_duration_minutes > 0 THEN trip_distance / (trip_duration_minutes / 60.0)
        ELSE NULL
    END AS avg_speed_mph,
    
    -- Location information
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    
    -- Trip characteristics
    passenger_count,
    trip_distance,
    trip_category,
    
    -- Financial information
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    
    -- Calculated financial metrics
    CASE 
        WHEN trip_distance > 0 THEN fare_amount / trip_distance
        ELSE NULL
    END AS fare_per_mile,
    CASE 
        WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
        ELSE NULL
    END AS tip_percentage,
    
    -- Payment information
    payment_type,
    store_and_fwd_flag,
    rate_code_id,
    
    -- Metadata
    year,
    month,
    loaded_at,
    source_file
FROM {{ ref('stg_yellow_tripdata_elaborated') }}
WHERE pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND pickup_datetime <= dropoff_datetime  -- Valid trip
  AND trip_distance >= 0
  AND fare_amount >= 0