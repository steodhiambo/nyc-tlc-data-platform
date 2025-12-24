-- models/staging/stg_weather_data.sql
{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    date::DATE AS date,
    hour::INTEGER AS hour,
    temperature::DECIMAL(5,2) AS temperature,
    precipitation::DECIMAL(5,2) AS precipitation,
    weather_condition::VARCHAR(50) AS weather_condition,
    location_borough::VARCHAR(50) AS location_borough,
    loaded_at
FROM {{ source('raw', 'weather_data') }}