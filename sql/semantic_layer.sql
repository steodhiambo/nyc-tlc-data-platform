-- Semantic Layer Views for NYC TLC Data Warehouse
-- These views provide business-friendly interfaces to the dimensional model

-- 1. Revenue by Zone View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_revenue_by_zone AS
SELECT 
    ld.zone AS pickup_zone,
    ld.borough AS pickup_borough,
    tf.year,
    tf.month,
    COUNT(*) AS trip_count,
    SUM(tf.total_amount) AS total_revenue,
    SUM(tf.fare_amount) AS fare_revenue,
    SUM(tf.tip_amount) AS tip_revenue,
    AVG(tf.total_amount) AS avg_revenue_per_trip,
    AVG(tf.trip_distance) AS avg_trip_distance,
    AVG(tf.trip_duration_minutes) AS avg_trip_duration
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.location_dim ld ON tf.pickup_location_key = ld.location_key
GROUP BY ld.zone, ld.borough, tf.year, tf.month
ORDER BY total_revenue DESC;

-- 2. Peak Hours Analysis View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_peak_hours_analysis AS
SELECT 
    EXTRACT(HOUR FROM tf.pickup_datetime) AS pickup_hour,
    EXTRACT(DOW FROM tf.pickup_datetime) AS day_of_week,
    TO_CHAR(tf.pickup_datetime, 'Day') AS day_name,
    COUNT(*) AS trip_count,
    SUM(tf.total_amount) AS total_revenue,
    AVG(tf.total_amount) AS avg_fare,
    AVG(tf.trip_distance) AS avg_distance,
    COUNT(*) FILTER (WHERE tf.payment_key = 1) AS credit_card_trips,
    COUNT(*) FILTER (WHERE tf.payment_key = 2) AS cash_trips
FROM nyc_taxi_dw.trips_fact tf
GROUP BY EXTRACT(HOUR FROM tf.pickup_datetime), EXTRACT(DOW FROM tf.pickup_datetime), TO_CHAR(tf.pickup_datetime, 'Day')
ORDER BY day_of_week, pickup_hour;

-- 3. Zone-to-Zone Trip Patterns View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_zone_to_zone_trips AS
SELECT 
    pl.zone AS pickup_zone,
    pl.borough AS pickup_borough,
    dl.zone AS dropoff_zone,
    dl.borough AS dropoff_borough,
    COUNT(*) AS trip_count,
    AVG(tf.trip_distance) AS avg_distance,
    AVG(tf.trip_duration_minutes) AS avg_duration,
    SUM(tf.total_amount) AS total_revenue,
    AVG(tf.total_amount) AS avg_revenue
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key
JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key
GROUP BY pl.zone, pl.borough, dl.zone, dl.borough
HAVING COUNT(*) >= 10  -- Only show routes with at least 10 trips
ORDER BY trip_count DESC
LIMIT 100;

-- 4. Payment Type Analysis View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_payment_type_analysis AS
SELECT 
    pt.payment_type_description,
    tf.year,
    tf.month,
    COUNT(*) AS trip_count,
    SUM(tf.total_amount) AS total_revenue,
    SUM(tf.tip_amount) AS total_tips,
    AVG(tf.tip_amount / NULLIF(tf.fare_amount, 0)) * 100 AS avg_tip_percentage,
    AVG(tf.total_amount) AS avg_total_fare
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.payment_dim pt ON tf.payment_key = pt.payment_key
WHERE tf.fare_amount > 0  -- Avoid division by zero
GROUP BY pt.payment_type_description, tf.year, tf.month
ORDER BY tf.year DESC, tf.month DESC, total_revenue DESC;

-- 5. Driver Performance View (by vendor)
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_driver_performance AS
SELECT 
    vd.vendor_name,
    tf.year,
    tf.month,
    COUNT(*) AS total_trips,
    SUM(tf.total_amount) AS total_revenue,
    SUM(tf.tip_amount) AS total_tips,
    AVG(tf.total_amount) AS avg_fare,
    AVG(tf.trip_distance) AS avg_distance,
    AVG(tf.trip_duration_minutes) AS avg_duration,
    AVG(tf.passenger_count) AS avg_passengers
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.vendor_dim vd ON tf.vendor_key = vd.vendor_key
GROUP BY vd.vendor_name, tf.year, tf.month
ORDER BY total_revenue DESC;

-- 6. Monthly Trends View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_monthly_trends AS
SELECT 
    tf.year,
    tf.month,
    COUNT(*) AS total_trips,
    SUM(tf.total_amount) AS total_revenue,
    SUM(tf.trip_distance) AS total_distance,
    AVG(tf.trip_distance) AS avg_distance,
    AVG(tf.trip_duration_minutes) AS avg_duration,
    AVG(tf.total_amount) AS avg_fare,
    AVG(tf.tip_amount) AS avg_tip,
    COUNT(*) FILTER (WHERE tf.trip_distance > 5) AS long_trips,  -- Trips over 5 miles
    COUNT(*) FILTER (WHERE tf.trip_distance <= 1) AS short_trips  -- Trips 1 mile or less
FROM nyc_taxi_dw.trips_fact tf
GROUP BY tf.year, tf.month
ORDER BY tf.year, tf.month;

-- 7. Time-based Aggregations View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_time_based_aggregations AS
SELECT 
    DATE_TRUNC('month', tf.pickup_datetime) AS month,
    DATE_TRUNC('week', tf.pickup_datetime) AS week,
    DATE_TRUNC('day', tf.pickup_datetime) AS day,
    EXTRACT(HOUR FROM tf.pickup_datetime) AS hour,
    COUNT(*) AS trip_count,
    SUM(tf.total_amount) AS revenue,
    AVG(tf.total_amount) AS avg_fare,
    SUM(tf.trip_distance) AS total_distance,
    AVG(tf.trip_distance) AS avg_distance,
    AVG(tf.trip_duration_minutes) AS avg_duration
FROM nyc_taxi_dw.trips_fact tf
GROUP BY 
    DATE_TRUNC('month', tf.pickup_datetime),
    DATE_TRUNC('week', tf.pickup_datetime),
    DATE_TRUNC('day', tf.pickup_datetime),
    EXTRACT(HOUR FROM tf.pickup_datetime)
ORDER BY month, week, day, hour;

-- 8. Location Performance View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_location_performance AS
SELECT 
    ld.zone,
    ld.borough,
    COUNT(*) AS total_pickups,
    COUNT(DISTINCT tf.dropoff_location_key) AS unique_dropoff_zones,
    SUM(tf.total_amount) AS total_pickup_revenue,
    AVG(tf.total_amount) AS avg_pickup_fare,
    AVG(tf.trip_distance) AS avg_trip_distance,
    AVG(tf.trip_duration_minutes) AS avg_trip_duration
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.location_dim ld ON tf.pickup_location_key = ld.location_key
GROUP BY ld.zone, ld.borough
HAVING COUNT(*) >= 50  -- Only locations with at least 50 trips
ORDER BY total_pickups DESC
LIMIT 50;

-- 9. Passenger Analysis View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_passenger_analysis AS
SELECT 
    tf.passenger_count,
    COUNT(*) AS trip_count,
    SUM(tf.total_amount) AS total_revenue,
    AVG(tf.total_amount) AS avg_fare,
    AVG(tf.trip_distance) AS avg_distance,
    AVG(tf.trip_duration_minutes) AS avg_duration,
    AVG(tf.tip_amount) AS avg_tip,
    SUM(tf.tip_amount) AS total_tips
FROM nyc_taxi_dw.trips_fact tf
WHERE tf.passenger_count IS NOT NULL
GROUP BY tf.passenger_count
ORDER BY passenger_count;

-- 10. Comprehensive Trip Summary View
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_trip_summary AS
SELECT 
    tf.trip_key,
    vd.vendor_name,
    pl.zone AS pickup_zone,
    pl.borough AS pickup_borough,
    dl.zone AS dropoff_zone,
    dl.borough AS dropoff_borough,
    tf.pickup_datetime,
    tf.dropoff_datetime,
    tf.trip_duration_minutes,
    tf.passenger_count,
    tf.trip_distance,
    tf.fare_amount,
    tf.tip_amount,
    tf.tolls_amount,
    tf.total_amount,
    pt.payment_type_description,
    CASE 
        WHEN tf.trip_distance > 0 THEN tf.total_amount / tf.trip_distance 
        ELSE NULL 
    END AS fare_per_mile
FROM nyc_taxi_dw.trips_fact tf
JOIN nyc_taxi_dw.vendor_dim vd ON tf.vendor_key = vd.vendor_key
JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key
JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key
LEFT JOIN nyc_taxi_dw.payment_dim pt ON tf.payment_key = pt.payment_key;

-- 11. Performance Metrics View for Dashboard
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_dashboard_metrics AS
SELECT 
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS average_fare,
    AVG(trip_distance) AS average_distance,
    AVG(trip_duration_minutes) AS average_duration,
    AVG(tip_amount) AS average_tip,
    SUM(tip_amount) / SUM(fare_amount) * 100 AS tip_percentage,
    COUNT(*) FILTER (WHERE store_and_fwd_flag = true) AS store_and_forward_trips,
    (COUNT(*) FILTER (WHERE store_and_fwd_flag = true)) * 100.0 / COUNT(*) AS store_and_forward_percentage
FROM nyc_taxi_dw.trips_fact tf;