-- models/core/dimensions/dim_time.sql
{{ config(
    materialized='table',
    schema='core'
) }}

-- This model creates a time dimension table with various time-related attributes
WITH time_range AS (
    -- Generate a series of dates for our time dimension
    SELECT generate_series(
        '2020-01-01'::date, 
        '2025-12-31'::date, 
        '1 day'::interval
    )::date AS full_date
),

time_attributes AS (
    SELECT
        {{ dbt_utils.surrogate_key(['full_date']) }} AS time_key,
        full_date,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(MONTH FROM full_date) AS month,
        TO_CHAR(full_date, 'Month') AS month_name,
        EXTRACT(DAY FROM full_date) AS day_of_month,
        EXTRACT(DOW FROM full_date) AS day_of_week,  -- 0=Sunday, 1=Monday, etc.
        TO_CHAR(full_date, 'Day') AS day_name,
        CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,  -- This would be updated with actual holiday data
        CURRENT_TIMESTAMP AS created_at
    FROM time_range
)

SELECT * FROM time_attributes