-- Additional SQL functions and procedures for ELT transformations
-- These can be used in the improved ELT pipeline

-- Function to clean and validate coordinates
CREATE OR REPLACE FUNCTION clean_nyc_coordinates(
    lon DOUBLE PRECISION,
    lat DOUBLE PRECISION
)
RETURNS TABLE(clean_lon DOUBLE PRECISION, clean_lat DOUBLE PRECISION, is_valid BOOLEAN)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        CASE 
            WHEN lon BETWEEN -74.27 AND -73.69 THEN lon
            ELSE NULL
        END AS clean_lon,
        CASE 
            WHEN lat BETWEEN 40.49 AND 40.92 THEN lat
            ELSE NULL
        END AS clean_lat,
        (lon BETWEEN -74.27 AND -73.69 AND lat BETWEEN 40.49 AND 40.92) AS is_valid;
END;
$$ LANGUAGE plpgsql;


-- Function to calculate trip duration in minutes
CREATE OR REPLACE FUNCTION calculate_trip_duration(start_time TIMESTAMP, end_time TIMESTAMP)
RETURNS NUMERIC AS $$
BEGIN
    RETURN EXTRACT(EPOCH FROM (end_time - start_time))/60;
END;
$$ LANGUAGE plpgsql;


-- Function to validate trip data
CREATE OR REPLACE FUNCTION validate_trip_data(
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    fare_amount NUMERIC,
    passenger_count INTEGER,
    trip_distance NUMERIC
)
RETURNS BOOLEAN AS $$
DECLARE
    trip_duration NUMERIC;
BEGIN
    -- Check if required fields are not null
    IF pickup_datetime IS NULL OR dropoff_datetime IS NULL THEN
        RETURN FALSE;
    END IF;
    
    -- Check if fare amount is non-negative
    IF fare_amount < 0 THEN
        RETURN FALSE;
    END IF;
    
    -- Check if passenger count is non-negative
    IF passenger_count < 0 THEN
        RETURN FALSE;
    END IF;
    
    -- Check if trip distance is non-negative
    IF trip_distance < 0 THEN
        RETURN FALSE;
    END IF;
    
    -- Calculate trip duration
    trip_duration := EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60;
    
    -- Check if trip duration is between 1 minute and 24 hours
    IF trip_duration < 1 OR trip_duration > 1440 THEN
        RETURN FALSE;
    END IF;
    
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


-- Stored procedure for incremental ELT transformation
CREATE OR REPLACE PROCEDURE run_incremental_elt_transformation(
    p_year INTEGER,
    p_month INTEGER
)
AS $$
DECLARE
    records_processed INTEGER;
BEGIN
    -- Create temporary table for cleaned data
    CREATE TEMP TABLE temp_cleaned_data AS
    SELECT 
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
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
        p_year AS year,
        p_month AS month,
        NOW() AS loaded_at,
        'incremental_load' AS source_file,
        -- Calculate derived fields in SQL
        calculate_trip_duration(tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
        -- Clean coordinates to be within NYC bounds
        (SELECT clean_lon FROM clean_nyc_coordinates(pickup_longitude, pickup_latitude)) AS pickup_longitude_clean,
        (SELECT clean_lat FROM clean_nyc_coordinates(pickup_longitude, pickup_latitude)) AS pickup_latitude_clean,
        (SELECT clean_lon FROM clean_nyc_coordinates(dropoff_longitude, dropoff_latitude)) AS dropoff_longitude_clean,
        (SELECT clean_lat FROM clean_nyc_coordinates(dropoff_longitude, dropoff_latitude)) AS dropoff_latitude_clean
    FROM stg_yellow_tripdata
    WHERE year = p_year AND month = p_month
      AND validate_trip_data(tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount, passenger_count, trip_distance);

    -- Insert cleaned data into the final table
    INSERT INTO taxi_data.yellow_tripdata (
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
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
        created_at
    )
    SELECT 
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude_clean,
        pickup_latitude_clean,
        rate_code_id,
        store_and_fwd_flag,
        dropoff_longitude_clean,
        dropoff_latitude_clean,
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
        loaded_at
    FROM temp_cleaned_data
    WHERE pickup_longitude_clean IS NOT NULL
      AND pickup_latitude_clean IS NOT NULL
      AND dropoff_longitude_clean IS NOT NULL
      AND dropoff_latitude_clean IS NOT NULL;

    -- Get the count of processed records
    GET DIAGNOSTICS records_processed = ROW_COUNT;

    -- Log the number of records processed
    INSERT INTO taxi_data.pipeline_logs (
        pipeline_name, run_id, status, start_time, end_time,
        records_processed, records_failed
    )
    VALUES (
        'nyc_tlc_improved_elt_pipeline_procedure',
        'run_' || NOW()::TEXT,
        'SUCCESS',
        NOW() - INTERVAL '5 minutes',
        NOW(),
        records_processed,
        0
    );

    -- Clean up temporary table
    DROP TABLE temp_cleaned_data;
END;
$$ LANGUAGE plpgsql;


-- Function to create monthly partitions for better performance
CREATE OR REPLACE FUNCTION create_monthly_partition(
    table_name TEXT,
    partition_month DATE
)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
BEGIN
    partition_name := table_name || '_' || TO_CHAR(partition_month, 'YYYY_MM');
    start_date := TO_CHAR(partition_month, 'YYYY-MM-01');
    end_date := TO_CHAR(partition_month + INTERVAL '1 month', 'YYYY-MM-01');
    
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
        FOR VALUES FROM (''%s'') TO (''%s'');',
        partition_name, table_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;


-- Indexes for better query performance on staging tables
CREATE INDEX IF NOT EXISTS idx_stg_yellow_tripdata_year_month
ON stg_yellow_tripdata (year, month);

CREATE INDEX IF NOT EXISTS idx_stg_green_tripdata_year_month
ON stg_green_tripdata (year, month);

-- Materialized view for common aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_trip_summary AS
SELECT 
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    SUM(total_amount) AS total_revenue
FROM taxi_data.yellow_tripdata
GROUP BY DATE(pickup_datetime);

-- Refresh the materialized view
REFRESH MATERIALIZED VIEW mv_daily_trip_summary;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_daily_trip_summary;
    -- Add more materialized views as needed
END;
$$ LANGUAGE plpgsql;