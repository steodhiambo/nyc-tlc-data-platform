-- models/staging/stg_yellow_tripdata_elaborated.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

-- ELT-enhanced staging model that assumes data has already been cleaned in the database
WITH cleaned_data AS (
    SELECT
        vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        rate_code_id,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        year,
        month,
        loaded_at,
        source_file,
        -- Calculate derived fields that were computed during the ELT process
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60 AS trip_duration_minutes,
        -- Use the cleaned coordinates that were validated in the database
        CASE 
            WHEN pickup_longitude BETWEEN -74.27 AND -73.69 THEN pickup_longitude
            ELSE NULL
        END AS pickup_longitude_clean,
        CASE 
            WHEN pickup_latitude BETWEEN 40.49 AND 40.92 THEN pickup_latitude
            ELSE NULL
        END AS pickup_latitude_clean,
        CASE 
            WHEN dropoff_longitude BETWEEN -74.27 AND -73.69 THEN dropoff_longitude
            ELSE NULL
        END AS dropoff_longitude_clean,
        CASE 
            WHEN dropoff_latitude BETWEEN 40.49 AND 40.92 THEN dropoff_latitude
            ELSE NULL
        END AS dropoff_latitude_clean
    FROM {{ source('raw', 'yellow_tripdata') }}
    WHERE pickup_datetime >= '2020-01-01'  -- Filter for recent data
      AND pickup_longitude IS NOT NULL
      AND pickup_latitude IS NOT NULL
      AND dropoff_longitude IS NOT NULL
      AND dropoff_latitude IS NOT NULL
)

SELECT
    -- Original fields
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude_clean AS pickup_longitude,
    pickup_latitude_clean AS pickup_latitude,
    rate_code_id,
    store_and_fwd_flag,
    dropoff_longitude_clean AS dropoff_longitude,
    dropoff_latitude_clean AS dropoff_latitude,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    year,
    month,
    loaded_at,
    source_file,
    -- Derived fields
    trip_duration_minutes,
    -- Additional computed fields
    CASE 
        WHEN trip_distance > 0 THEN fare_amount / trip_distance
        ELSE NULL
    END AS fare_per_mile,
    CASE 
        WHEN trip_duration_minutes > 0 THEN trip_distance / (trip_duration_minutes / 60.0)
        ELSE NULL
    END AS avg_speed_mph,
    -- Trip categorization
    CASE 
        WHEN trip_distance < 1 THEN 'Short'
        WHEN trip_distance BETWEEN 1 AND 5 THEN 'Medium'
        WHEN trip_distance BETWEEN 5 AND 15 THEN 'Long'
        ELSE 'Very Long'
    END AS trip_category,
    -- Time-based features
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    EXTRACT(DOW FROM pickup_datetime) AS pickup_day_of_week,
    EXTRACT(MONTH FROM pickup_datetime) AS pickup_month
FROM cleaned_data
WHERE pickup_datetime >= '2020-01-01'  -- Additional filter for recent data