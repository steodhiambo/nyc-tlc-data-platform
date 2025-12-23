-- Script to populate dimension tables from existing raw data
-- This script transforms the existing normalized schema into the dimensional star schema

-- First, populate vendor dimension from existing data
INSERT INTO nyc_taxi_dw.vendor_dim (vendor_id, vendor_name)
SELECT DISTINCT 
    vendor_id,
    CASE 
        WHEN vendor_id = 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN vendor_id = 2 THEN 'VeriFone Inc.'
        WHEN vendor_id = 4 THEN 'Square'
        ELSE 'Unknown Vendor'
    END AS vendor_name
FROM (
    SELECT DISTINCT vendor_id FROM taxi_data.yellow_tripdata WHERE vendor_id IS NOT NULL
    UNION
    SELECT DISTINCT vendor_id FROM taxi_data.green_tripdata WHERE vendor_id IS NOT NULL
) AS vendors
ON CONFLICT (vendor_id) DO NOTHING;

-- Populate location dimension from existing taxi zone lookup
-- (This should already be populated from the previous script, but included for completeness)
INSERT INTO nyc_taxi_dw.location_dim (location_id, borough, zone, service_zone)
SELECT 
    location_id,
    borough,
    zone,
    service_zone
FROM taxi_data.taxi_zone_lookup
ON CONFLICT (location_id) DO NOTHING;

-- Function to populate time dimension for a date range
CREATE OR REPLACE FUNCTION populate_time_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    current_date DATE := start_date;
    hour_val INTEGER;
    minute_val INTEGER;
BEGIN
    WHILE current_date <= end_date LOOP
        -- Create records for each hour of the day
        FOR hour_val IN 0..23 LOOP
            FOR minute_val IN ARRAY[0, 15, 30, 45] LOOP  -- Every 15 minutes to reduce data volume
                INSERT INTO nyc_taxi_dw.time_dim (
                    full_date, year, quarter, month, month_name, day_of_month,
                    day_of_week, day_name, hour_of_day, minute_of_hour, is_weekend
                )
                SELECT 
                    current_date,
                    EXTRACT(YEAR FROM current_date),
                    EXTRACT(QUARTER FROM current_date),
                    EXTRACT(MONTH FROM current_date),
                    TO_CHAR(current_date, 'Month'),
                    EXTRACT(DAY FROM current_date),
                    EXTRACT(DOW FROM current_date),
                    TO_CHAR(current_date, 'Day'),
                    hour_val,
                    minute_val,
                    CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END
                ON CONFLICT (full_date, hour_of_day, minute_of_hour) DO NOTHING;
            END LOOP;
        END LOOP;
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate time dimension for a reasonable range (adjust as needed)
SELECT populate_time_dimension('2020-01-01'::DATE, '2025-12-31'::DATE);

-- Function to populate fact table from yellow taxi data
CREATE OR REPLACE FUNCTION populate_yellow_trips_fact(start_date DATE, end_date DATE)
RETURNS INTEGER AS $$
DECLARE
    rows_inserted INTEGER := 0;
    pickup_time_key INTEGER;
    dropoff_time_key INTEGER;
    pickup_location_key INTEGER;
    dropoff_location_key INTEGER;
    vendor_key INTEGER;
    payment_key INTEGER;
    rate_code_key INTEGER;
BEGIN
    -- Process yellow taxi data in chunks to avoid memory issues
    FOR trip_record IN 
        SELECT * FROM taxi_data.yellow_tripdata 
        WHERE pickup_datetime::DATE BETWEEN start_date AND end_date
        AND pickup_location_id IS NOT NULL 
        AND dropoff_location_id IS NOT NULL
        AND vendor_id IS NOT NULL
        LIMIT 100000  -- Process in batches
    LOOP
        -- Get or create time dimension keys
        pickup_time_key := nyc_taxi_dw.get_or_create_time_dim(trip_record.pickup_datetime);
        dropoff_time_key := nyc_taxi_dw.get_or_create_time_dim(trip_record.dropoff_datetime);
        
        -- Get location keys (assuming they exist in dimension table)
        SELECT location_key INTO pickup_location_key 
        FROM nyc_taxi_dw.location_dim 
        WHERE location_id = trip_record.pickup_location_id;
        
        SELECT location_key INTO dropoff_location_key 
        FROM nyc_taxi_dw.location_dim 
        WHERE location_id = trip_record.dropoff_location_id;
        
        -- Get vendor key
        SELECT vendor_key INTO vendor_key 
        FROM nyc_taxi_dw.vendor_dim 
        WHERE vendor_id = trip_record.vendor_id;
        
        -- Get payment key
        SELECT payment_key INTO payment_key 
        FROM nyc_taxi_dw.payment_dim 
        WHERE payment_type_id = trip_record.payment_type;
        
        -- Get rate code key
        SELECT rate_code_key INTO rate_code_key 
        FROM nyc_taxi_dw.rate_code_dim 
        WHERE rate_code_id = trip_record.rate_code_id;
        
        -- Insert into fact table
        INSERT INTO nyc_taxi_dw.trips_fact (
            vendor_key, pickup_location_key, dropoff_location_key,
            pickup_time_key, dropoff_time_key, payment_key, rate_code_key,
            trip_id, passenger_count, trip_distance, trip_duration_minutes,
            fare_amount, extra_charges, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, congestion_surcharge, airport_fee, total_amount,
            pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude,
            store_and_fwd_flag, pickup_datetime, dropoff_datetime,
            year, month
        )
        VALUES (
            COALESCE(vendor_key, 1),  -- Default to first vendor if not found
            COALESCE(pickup_location_key, 1),  -- Default to first location if not found
            COALESCE(dropoff_location_key, 1),  -- Default to first location if not found
            pickup_time_key,
            dropoff_time_key,
            COALESCE(payment_key, 1),  -- Default to first payment type if not found
            COALESCE(rate_code_key, 1),  -- Default to first rate code if not found
            NULL,  -- trip_id - not in source but could be constructed
            trip_record.passenger_count,
            trip_record.trip_distance,
            EXTRACT(EPOCH FROM (trip_record.dropoff_datetime - trip_record.pickup_datetime))/60,
            trip_record.fare_amount,
            trip_record.extra,
            trip_record.mta_tax,
            trip_record.tip_amount,
            trip_record.tolls_amount,
            trip_record.improvement_surcharge,
            COALESCE(trip_record.congestion_surcharge, 0),
            COALESCE(trip_record.airport_fee, 0),
            trip_record.total_amount,
            NULL, NULL, NULL, NULL,  -- Lat/Long not in source but could be added
            CASE WHEN trip_record.store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END,
            trip_record.pickup_datetime,
            trip_record.dropoff_datetime,
            trip_record.year,
            trip_record.month
        );
        
        rows_inserted := rows_inserted + 1;
    END LOOP;
    
    RETURN rows_inserted;
END;
$$ LANGUAGE plpgsql;

-- Function to populate fact table from green taxi data
CREATE OR REPLACE FUNCTION populate_green_trips_fact(start_date DATE, end_date DATE)
RETURNS INTEGER AS $$
DECLARE
    rows_inserted INTEGER := 0;
    pickup_time_key INTEGER;
    dropoff_time_key INTEGER;
    pickup_location_key INTEGER;
    dropoff_location_key INTEGER;
    vendor_key INTEGER;
    payment_key INTEGER;
    rate_code_key INTEGER;
    trip_type_key INTEGER;
BEGIN
    -- Process green taxi data in chunks to avoid memory issues
    FOR trip_record IN 
        SELECT * FROM taxi_data.green_tripdata 
        WHERE pickup_datetime::DATE BETWEEN start_date AND end_date
        AND pickup_location_id IS NOT NULL 
        AND dropoff_location_id IS NOT NULL
        AND vendor_id IS NOT NULL
        LIMIT 100000  -- Process in batches
    LOOP
        -- Get or create time dimension keys
        pickup_time_key := nyc_taxi_dw.get_or_create_time_dim(trip_record.pickup_datetime);
        dropoff_time_key := nyc_taxi_dw.get_or_create_time_dim(trip_record.dropoff_datetime);
        
        -- Get location keys (assuming they exist in dimension table)
        SELECT location_key INTO pickup_location_key 
        FROM nyc_taxi_dw.location_dim 
        WHERE location_id = trip_record.pickup_location_id;
        
        SELECT location_key INTO dropoff_location_key 
        FROM nyc_taxi_dw.location_dim 
        WHERE location_id = trip_record.dropoff_location_id;
        
        -- Get vendor key
        SELECT vendor_key INTO vendor_key 
        FROM nyc_taxi_dw.vendor_dim 
        WHERE vendor_id = trip_record.vendor_id;
        
        -- Get payment key
        SELECT payment_key INTO payment_key 
        FROM nyc_taxi_dw.payment_dim 
        WHERE payment_type_id = trip_record.payment_type;
        
        -- Get rate code key
        SELECT rate_code_key INTO rate_code_key 
        FROM nyc_taxi_dw.rate_code_dim 
        WHERE rate_code_id = trip_record.rate_code_id;
        
        -- Get trip type key
        SELECT trip_type_key INTO trip_type_key 
        FROM nyc_taxi_dw.trip_type_dim 
        WHERE trip_type_id = trip_record.trip_type;
        
        -- Insert into fact table
        INSERT INTO nyc_taxi_dw.trips_fact (
            vendor_key, pickup_location_key, dropoff_location_key,
            pickup_time_key, dropoff_time_key, payment_key, rate_code_key, trip_type_key,
            trip_id, passenger_count, trip_distance, trip_duration_minutes,
            fare_amount, extra_charges, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, congestion_surcharge, airport_fee, total_amount,
            pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude,
            store_and_fwd_flag, pickup_datetime, dropoff_datetime,
            year, month
        )
        VALUES (
            COALESCE(vendor_key, 1),  -- Default to first vendor if not found
            COALESCE(pickup_location_key, 1),  -- Default to first location if not found
            COALESCE(dropoff_location_key, 1),  -- Default to first location if not found
            pickup_time_key,
            dropoff_time_key,
            COALESCE(payment_key, 1),  -- Default to first payment type if not found
            COALESCE(rate_code_key, 1),  -- Default to first rate code if not found
            COALESCE(trip_type_key, 1),  -- Default to first trip type if not found
            NULL,  -- trip_id - not in source but could be constructed
            trip_record.passenger_count,
            trip_record.trip_distance,
            EXTRACT(EPOCH FROM (trip_record.dropoff_datetime - trip_record.pickup_datetime))/60,
            trip_record.fare_amount,
            trip_record.extra,
            trip_record.mta_tax,
            trip_record.tip_amount,
            trip_record.tolls_amount,
            trip_record.improvement_surcharge,
            COALESCE(trip_record.congestion_surcharge, 0),
            COALESCE(trip_record.airport_fee, 0),
            trip_record.total_amount,
            NULL, NULL, NULL, NULL,  -- Lat/Long not in source but could be added
            CASE WHEN trip_record.store_and_fwd_flag = 'Y' THEN TRUE ELSE FALSE END,
            trip_record.pickup_datetime,
            trip_record.dropoff_datetime,
            trip_record.year,
            trip_record.month
        );
        
        rows_inserted := rows_inserted + 1;
    END LOOP;
    
    RETURN rows_inserted;
END;
$$ LANGUAGE plpgsql;

-- Create a procedure to populate the fact table incrementally
CREATE OR REPLACE PROCEDURE populate_fact_table_incremental(data_type VARCHAR, start_date DATE, end_date DATE)
AS $$
DECLARE
    rows_processed INTEGER;
BEGIN
    IF data_type = 'yellow' THEN
        RAISE NOTICE 'Populating fact table with yellow taxi data from % to %', start_date, end_date;
        SELECT populate_yellow_trips_fact(start_date, end_date) INTO rows_processed;
        RAISE NOTICE 'Inserted % rows from yellow taxi data', rows_processed;
    ELSIF data_type = 'green' THEN
        RAISE NOTICE 'Populating fact table with green taxi data from % to %', start_date, end_date;
        SELECT populate_green_trips_fact(start_date, end_date) INTO rows_processed;
        RAISE NOTICE 'Inserted % rows from green taxi data', rows_processed;
    ELSE
        RAISE EXCEPTION 'Invalid data type. Use ''yellow'' or ''green''';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- CALL populate_fact_table_incremental('yellow', '2023-01-01', '2023-01-31');
-- CALL populate_fact_table_incremental('green', '2023-01-01', '2023-01-31');