-- models/core/dimensions/dim_location.sql
{{ config(
    materialized='table',
    schema='core'
) }}

WITH source_locations AS (
    SELECT 
        location_id,
        borough,
        zone,
        service_zone
    FROM {{ source('raw_data', 'taxi_zone_lookup') }}
    WHERE location_id IS NOT NULL
),

-- Add surrogate key for dimension table
surrogate_key_added AS (
    SELECT
        {{ dbt_utils.surrogate_key(['location_id']) }} AS location_key,
        location_id,
        borough,
        zone,
        service_zone,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM source_locations
)

SELECT * FROM surrogate_key_added