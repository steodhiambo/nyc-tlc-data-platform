-- models/staging/stg_green_tripdata.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    vendor_id,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
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
    source_file
FROM {{ source('raw', 'green_tripdata') }}
WHERE pickup_datetime >= '2020-01-01'  -- Filter for recent data