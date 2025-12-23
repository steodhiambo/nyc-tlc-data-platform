-- models/core/fact_trips.sql
{{ config(
    materialized='table',
    schema='core'
) }}

WITH trips AS (
    SELECT * FROM {{ ref('stg_yellow_tripdata') }}
    -- UNION ALL with green taxi data would go here in a complete implementation
),

locations AS (
    SELECT * FROM {{ ref('dim_location') }}
),

payment_types AS (
    SELECT * FROM {{ ref('dim_payment_type') }}
),

vendors AS (
    SELECT * FROM {{ ref('dim_vendor') }}
),

-- Create year_month for potential partitioning
trips_with_partition AS (
    SELECT *,
        CONCAT(
            LPAD(CAST(year AS STRING), 4, '0'),
            LPAD(CAST(month AS STRING), 2, '0')
        ) AS year_month
    FROM trips
),

-- Join with dimensions to create the fact table
fact_base AS (
    SELECT
        -- Surrogate key for the fact table
        {{ dbt_utils.surrogate_key([
            't.vendor_id',
            't.pickup_datetime',
            't.dropoff_datetime',
            't.pickup_location_id',
            't.dropoff_location_id'
        ]) }} AS trip_key,

        -- Dimension keys
        COALESCE(v.vendor_key, -1) AS vendor_key,
        COALESCE(pl.location_key, -1) AS pickup_location_key,
        COALESCE(dl.location_key, -1) AS dropoff_location_key,
        COALESCE(pt.payment_key, -1) AS payment_key,
        -1 AS rate_code_key,  -- Placeholder - would join with actual rate code dimension
        -1 AS trip_type_key,  -- Placeholder - would join with trip type dimension for green taxis

        -- Trip identifiers
        t.trip_id,

        -- Passenger and trip metrics
        t.passenger_count,
        t.trip_distance,
        EXTRACT(EPOCH FROM (t.dropoff_datetime - t.pickup_datetime))/60 AS trip_duration_minutes,

        -- Fare components
        t.fare_amount,
        t.extra AS extra_charges,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        COALESCE(t.congestion_surcharge, 0) AS congestion_surcharge,
        COALESCE(t.airport_fee, 0) AS airport_fee,
        t.total_amount,

        -- Additional metrics
        NULL AS pickup_latitude,  -- Would come from location dimension in full implementation
        NULL AS pickup_longitude,
        NULL AS dropoff_latitude,
        NULL AS dropoff_longitude,

        -- Flags
        CASE WHEN t.store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END AS store_and_fwd_flag,

        -- Timestamps
        t.pickup_datetime,
        t.dropoff_datetime,

        -- Date parts
        t.year,
        t.month,
        t.year_month,

        -- Audit fields
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM trips_with_partition t
    LEFT JOIN vendors v ON t.vendor_id = v.vendor_id
    LEFT JOIN locations pl ON t.pickup_location_id = pl.location_id
    LEFT JOIN locations dl ON t.dropoff_location_id = dl.location_id
    LEFT JOIN payment_types pt ON t.payment_type = pt.payment_type_id
)

SELECT * FROM fact_base