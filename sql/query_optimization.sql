-- Query Optimization Examples for NYC TLC Data Platform
-- Using WINDOW functions and other optimization techniques

-- 1. OPTIMIZED QUERIES USING WINDOW FUNCTIONS

-- Instead of using correlated subqueries or self-joins for ranking, use window functions
-- Example: Top 10 pickup zones by revenue for each month (with window function)
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_top_zones_by_month_window AS
WITH monthly_zone_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM tf.pickup_datetime) AS year,
        EXTRACT(MONTH FROM tf.pickup_datetime) AS month,
        ld.zone AS pickup_zone,
        SUM(tf.total_amount) AS total_revenue,
        COUNT(*) AS trip_count
    FROM nyc_taxi_dw.trips_fact tf
    JOIN nyc_taxi_dw.location_dim ld ON tf.pickup_location_key = ld.location_key
    GROUP BY 
        EXTRACT(YEAR FROM tf.pickup_datetime),
        EXTRACT(MONTH FROM tf.pickup_datetime),
        ld.zone
),
ranked_zones AS (
    SELECT 
        year,
        month,
        pickup_zone,
        total_revenue,
        trip_count,
        ROW_NUMBER() OVER (
            PARTITION BY year, month 
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM monthly_zone_revenue
)
SELECT 
    year,
    month,
    pickup_zone,
    total_revenue,
    trip_count,
    revenue_rank
FROM ranked_zones
WHERE revenue_rank <= 10
ORDER BY year, month, revenue_rank;

-- 2. OPTIMIZED QUERIES USING LAG/LEAD FOR TIME SERIES ANALYSIS

-- Example: Compare current month revenue with previous month
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_monthly_revenue_comparison AS
WITH monthly_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        SUM(total_amount) AS total_revenue,
        COUNT(*) AS trip_count
    FROM nyc_taxi_dw.trips_fact
    GROUP BY 
        EXTRACT(YEAR FROM pickup_datetime),
        EXTRACT(MONTH FROM pickup_datetime)
),
revenue_with_comparison AS (
    SELECT 
        year,
        month,
        total_revenue,
        trip_count,
        LAG(total_revenue) OVER (
            ORDER BY year, month
        ) AS prev_month_revenue,
        LAG(trip_count) OVER (
            ORDER BY year, month
        ) AS prev_month_trip_count
    FROM monthly_revenue
)
SELECT 
    year,
    month,
    total_revenue,
    trip_count,
    COALESCE(prev_month_revenue, 0) AS prev_month_revenue,
    COALESCE(prev_month_trip_count, 0) AS prev_month_trip_count,
    CASE 
        WHEN prev_month_revenue > 0 
        THEN ROUND(((total_revenue - prev_month_revenue) / prev_month_revenue * 100), 2)
        ELSE NULL 
    END AS revenue_change_pct
FROM revenue_with_comparison
ORDER BY year, month;

-- 3. OPTIMIZED QUERIES USING RANKING FUNCTIONS FOR PERFORMANCE ANALYSIS

-- Example: Driver performance ranking by month
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_driver_performance_ranking AS
WITH driver_monthly_stats AS (
    SELECT
        EXTRACT(YEAR FROM tf.pickup_datetime) AS year,
        EXTRACT(MONTH FROM tf.pickup_datetime) AS month,
        vd.vendor_name,
        COUNT(*) AS trip_count,
        SUM(tf.total_amount) AS total_revenue,
        AVG(tf.total_amount) AS avg_fare,
        AVG(tf.tip_amount) AS avg_tip,
        AVG(tf.trip_distance) AS avg_distance,
        AVG(tf.trip_duration_minutes) AS avg_duration
    FROM nyc_taxi_dw.trips_fact tf
    JOIN nyc_taxi_dw.vendor_dim vd ON tf.vendor_key = vd.vendor_key
    GROUP BY 
        EXTRACT(YEAR FROM tf.pickup_datetime),
        EXTRACT(MONTH FROM tf.pickup_datetime),
        vd.vendor_name
),
ranked_drivers AS (
    SELECT
        year,
        month,
        vendor_name,
        trip_count,
        total_revenue,
        avg_fare,
        avg_tip,
        avg_distance,
        avg_duration,
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY total_revenue DESC
        ) AS revenue_rank,
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY trip_count DESC
        ) AS trip_count_rank
    FROM driver_monthly_stats
)
SELECT 
    year,
    month,
    vendor_name,
    trip_count,
    total_revenue,
    avg_fare,
    avg_tip,
    avg_distance,
    avg_duration,
    revenue_rank,
    trip_count_rank
FROM ranked_drivers
WHERE revenue_rank <= 20  -- Top 20 performers
ORDER BY year, month, revenue_rank;

-- 4. OPTIMIZED QUERIES USING CTEs FOR COMPLEX CALCULATIONS

-- Example: Peak hour analysis with complex aggregations
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_peak_hour_analysis_optimized AS
WITH hourly_stats AS (
    SELECT
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        EXTRACT(DOW FROM pickup_datetime) AS day_of_week,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_fare,
        AVG(trip_distance) AS avg_distance,
        AVG(trip_duration_minutes) AS avg_duration,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount) AS median_fare
    FROM nyc_taxi_dw.trips_fact
    GROUP BY 
        EXTRACT(HOUR FROM pickup_datetime),
        EXTRACT(DOW FROM pickup_datetime)
),
hourly_rankings AS (
    SELECT
        pickup_hour,
        day_of_week,
        trip_count,
        total_revenue,
        avg_fare,
        avg_distance,
        avg_duration,
        median_fare,
        RANK() OVER (
            PARTITION BY day_of_week 
            ORDER BY trip_count DESC
        ) AS trip_count_rank,
        RANK() OVER (
            PARTITION BY day_of_week 
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM hourly_stats
)
SELECT 
    pickup_hour,
    day_of_week,
    TO_CHAR(MAKE_DATE(2023, 1, 1) + (day_of_week || ' day')::INTERVAL, 'Day') AS day_name,
    trip_count,
    total_revenue,
    avg_fare,
    avg_distance,
    avg_duration,
    median_fare,
    trip_count_rank,
    revenue_rank
FROM hourly_rankings
ORDER BY day_of_week, trip_count_rank;

-- 5. OPTIMIZED QUERIES USING FILTER CLAUSES IN AGGREGATIONS

-- Example: Payment type analysis with conditional aggregations
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_payment_analysis_optimized AS
SELECT 
    EXTRACT(YEAR FROM tf.pickup_datetime) AS year,
    EXTRACT(MONTH FROM tf.pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE tf.payment_key = 1) AS credit_card_trips,
    COUNT(*) FILTER (WHERE tf.payment_key = 2) AS cash_trips,
    COUNT(*) FILTER (WHERE tf.payment_key = 3) AS no_charge_trips,
    SUM(total_amount) AS total_revenue,
    SUM(total_amount) FILTER (WHERE tf.payment_key = 1) AS credit_revenue,
    SUM(total_amount) FILTER (WHERE tf.payment_key = 2) AS cash_revenue,
    AVG(tip_amount) AS avg_tip,
    AVG(tip_amount) FILTER (WHERE tf.payment_key = 1) AS credit_avg_tip,
    AVG(tip_amount) FILTER (WHERE tf.payment_key = 2) AS cash_avg_tip,
    SUM(tip_amount) / SUM(fare_amount) * 100 AS overall_tip_percentage
FROM nyc_taxi_dw.trips_fact tf
GROUP BY 
    EXTRACT(YEAR FROM tf.pickup_datetime),
    EXTRACT(MONTH FROM tf.pickup_datetime)
ORDER BY year, month;

-- 6. OPTIMIZED QUERIES FOR ZONE-TO-ZONE ANALYSIS

-- Example: Popular routes with distance and time efficiency
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_route_efficiency AS
WITH route_stats AS (
    SELECT
        pl.zone AS pickup_zone,
        pl.borough AS pickup_borough,
        dl.zone AS dropoff_zone,
        dl.borough AS dropoff_borough,
        COUNT(*) AS trip_count,
        AVG(trip_distance) AS avg_distance,
        AVG(trip_duration_minutes) AS avg_duration,
        AVG(CASE 
            WHEN trip_duration_minutes > 0 
            THEN trip_distance / trip_duration_minutes * 60  -- Speed in mph
            ELSE NULL 
        END) AS avg_speed,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_fare
    FROM nyc_taxi_dw.trips_fact tf
    JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key
    JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key
    GROUP BY 
        pl.zone, pl.borough,
        dl.zone, dl.borough
    HAVING COUNT(*) >= 10  -- Only routes with at least 10 trips
),
route_rankings AS (
    SELECT
        pickup_zone,
        pickup_borough,
        dropoff_zone,
        dropoff_borough,
        trip_count,
        avg_distance,
        avg_duration,
        avg_speed,
        total_revenue,
        avg_fare,
        RANK() OVER (ORDER BY trip_count DESC) AS popularity_rank,
        RANK() OVER (ORDER BY avg_speed DESC) AS efficiency_rank
    FROM route_stats
)
SELECT 
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    trip_count,
    avg_distance,
    avg_duration,
    ROUND(avg_speed, 2) AS avg_speed,
    total_revenue,
    ROUND(avg_fare, 2) AS avg_fare,
    popularity_rank,
    efficiency_rank
FROM route_rankings
WHERE popularity_rank <= 50  -- Top 50 popular routes
ORDER BY popularity_rank;

-- 7. OPTIMIZED QUERIES FOR TIME-BASED TRENDS

-- Example: Moving averages for trend analysis
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_trend_analysis AS
WITH daily_stats AS (
    SELECT
        pickup_datetime::DATE AS pickup_date,
        COUNT(*) AS daily_trips,
        SUM(total_amount) AS daily_revenue
    FROM nyc_taxi_dw.trips_fact
    GROUP BY pickup_datetime::DATE
),
trend_data AS (
    SELECT
        pickup_date,
        daily_trips,
        daily_revenue,
        AVG(daily_trips) OVER (
            ORDER BY pickup_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_avg_trips,
        AVG(daily_revenue) OVER (
            ORDER BY pickup_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_avg_revenue,
        AVG(daily_trips) OVER (
            ORDER BY pickup_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS monthly_avg_trips,
        AVG(daily_revenue) OVER (
            ORDER BY pickup_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS monthly_avg_revenue
    FROM daily_stats
)
SELECT 
    pickup_date,
    daily_trips,
    daily_revenue,
    ROUND(weekly_avg_trips, 2) AS weekly_avg_trips,
    ROUND(weekly_avg_revenue, 2) AS weekly_avg_revenue,
    ROUND(monthly_avg_trips, 2) AS monthly_avg_trips,
    ROUND(monthly_avg_revenue, 2) AS monthly_avg_revenue
FROM trend_data
ORDER BY pickup_date;

-- 8. PERFORMANCE OPTIMIZATION INDEXES

-- Create covering indexes for the most common queries
-- These should be created after analyzing query patterns

-- Index for date-based queries (most common)
CREATE INDEX CONCURRENTLY idx_trips_fact_pickup_date_btree ON nyc_taxi_dw.trips_fact (pickup_datetime);

-- Composite index for common analytical queries
CREATE INDEX CONCURRENTLY idx_trips_fact_date_location_btree ON nyc_taxi_dw.trips_fact (pickup_datetime, pickup_location_key);

-- Index for payment analysis
CREATE INDEX CONCURRENTLY idx_trips_fact_payment_date ON nyc_taxi_dw.trips_fact (payment_key, pickup_datetime);

-- Index for vendor analysis
CREATE INDEX CONCURRENTLY idx_trips_fact_vendor_date ON nyc_taxi_dw.trips_fact (vendor_key, pickup_datetime);

-- Index for distance and fare analysis
CREATE INDEX CONCURRENTLY idx_trips_fact_distance_fare ON nyc_taxi_dw.trips_fact (trip_distance, total_amount);

-- 9. MATERIALIZED VIEWS FOR FASTER REPORTING

-- Create materialized views for commonly accessed aggregations
CREATE MATERIALIZED VIEW nyc_taxi_dw.mv_monthly_summary AS
SELECT 
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare,
    SUM(trip_distance) AS total_distance,
    AVG(trip_distance) AS avg_distance,
    SUM(tip_amount) AS total_tips,
    AVG(tip_amount) AS avg_tip
FROM nyc_taxi_dw.trips_fact
GROUP BY 
    EXTRACT(YEAR FROM pickup_datetime),
    EXTRACT(MONTH FROM pickup_datetime);

-- Index for the materialized view
CREATE INDEX idx_mv_monthly_summary_year_month ON nyc_taxi_dw.mv_monthly_summary (year, month);

-- Refresh procedure for materialized views
CREATE OR REPLACE PROCEDURE nyc_taxi_dw.refresh_materialized_views()
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi_dw.mv_monthly_summary;
    -- Add other materialized views as needed
END;
$$ LANGUAGE plpgsql;

-- 10. QUERY OPTIMIZATION FUNCTIONS

-- Function to get trips with pagination using cursor-like approach for large result sets
CREATE OR REPLACE FUNCTION nyc_taxi_dw.get_trips_paginated(
    p_pickup_date DATE,
    p_offset INTEGER DEFAULT 0,
    p_limit INTEGER DEFAULT 1000
)
RETURNS TABLE (
    trip_key BIGINT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pickup_zone VARCHAR,
    dropoff_zone VARCHAR,
    total_amount DECIMAL,
    trip_distance DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        tf.trip_key,
        tf.pickup_datetime,
        tf.dropoff_datetime,
        pl.zone,
        dl.zone,
        tf.total_amount,
        tf.trip_distance
    FROM nyc_taxi_dw.trips_fact tf
    JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key
    JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key
    WHERE tf.pickup_datetime::DATE = p_pickup_date
    ORDER BY tf.pickup_datetime
    OFFSET p_offset LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;