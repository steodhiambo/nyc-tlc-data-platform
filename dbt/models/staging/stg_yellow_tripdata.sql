-- models/staging/stg_yellow_tripdata.sql
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source_data AS (
    SELECT * 
    FROM {{ source('raw_data', 'yellow_tripdata') }}
    WHERE pickup_datetime IS NOT NULL 
      AND dropoff_datetime IS NOT NULL
),

renamed AS (
    SELECT
        -- identifiers
        {{ dbt_utils.surrogate_key(['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id']) }} AS trip_id,
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        rate_code_id,
        payment_type,
        
        -- timestamps
        pickup_datetime,
        dropoff_datetime,
        
        -- trip info
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        
        -- fare info
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        
        -- dates for partitioning
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
        
    FROM source_data
)

SELECT * FROM renamed