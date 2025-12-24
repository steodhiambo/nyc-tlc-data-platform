-- models/staging/stg_taxi_zone_lookup.sql
{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    location_id::INTEGER AS location_id,
    borough::VARCHAR(50) AS borough,
    zone::VARCHAR(100) AS zone,
    service_zone::VARCHAR(50) AS service_zone,
    loaded_at
FROM {{ source('raw', 'taxi_zone_lookup') }}