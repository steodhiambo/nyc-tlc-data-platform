-- Database Optimization Scripts for NYC TLC Data Warehouse
-- Includes partitioning, indexing, and maintenance procedures

-- 1. PARTITIONING: Create partitioned version of the fact table for better performance

-- Drop the original fact table if it exists (backup first!)
-- NOTE: In production, you would migrate data instead of dropping
DROP TABLE IF EXISTS nyc_taxi_dw.trips_fact CASCADE;

-- Recreate the fact table with partitioning by year and month
CREATE TABLE nyc_taxi_dw.trips_fact (
    trip_key BIGSERIAL,
    vendor_key INTEGER NOT NULL,
    pickup_location_key INTEGER NOT NULL,
    dropoff_location_key INTEGER NOT NULL,
    pickup_time_key INTEGER NOT NULL,
    dropoff_time_key INTEGER NOT NULL,
    payment_key INTEGER,
    rate_code_key INTEGER,
    trip_type_key INTEGER,
    
    -- Trip identifiers
    trip_id VARCHAR(50), -- Original trip ID from source
    
    -- Passenger and trip metrics
    passenger_count INTEGER,
    trip_distance DECIMAL(10,2),
    trip_duration_minutes DECIMAL(8,2),
    
    -- Fare components
    fare_amount DECIMAL(10,2),
    extra_charges DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    
    -- Additional metrics
    pickup_latitude DECIMAL(8,6),
    pickup_longitude DECIMAL(9,6),
    dropoff_latitude DECIMAL(8,6),
    dropoff_longitude DECIMAL(9,6),
    
    -- Flags
    store_and_fwd_flag BOOLEAN,
    
    -- Timestamps
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    
    -- Date parts for partitioning
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Primary key constraint will be defined in each partition
    -- Foreign key constraints will be defined in each partition
    CONSTRAINT fk_trips_vendor FOREIGN KEY (vendor_key) REFERENCES nyc_taxi_dw.vendor_dim(vendor_key),
    CONSTRAINT fk_trips_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES nyc_taxi_dw.location_dim(location_key),
    CONSTRAINT fk_trips_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES nyc_taxi_dw.location_dim(location_key),
    CONSTRAINT fk_trips_pickup_time FOREIGN KEY (pickup_time_key) REFERENCES nyc_taxi_dw.time_dim(time_key),
    CONSTRAINT fk_trips_dropoff_time FOREIGN KEY (dropoff_time_key) REFERENCES nyc_taxi_dw.time_dim(time_key),
    CONSTRAINT fk_trips_payment FOREIGN KEY (payment_key) REFERENCES nyc_taxi_dw.payment_dim(payment_key),
    CONSTRAINT fk_trips_rate_code FOREIGN KEY (rate_code_key) REFERENCES nyc_taxi_dw.rate_code_dim(rate_code_key),
    CONSTRAINT fk_trips_trip_type FOREIGN KEY (trip_type_key) REFERENCES nyc_taxi_dw.trip_type_dim(trip_type_key)
) PARTITION BY RANGE (year, month);

-- Create partitions for each year-month combination
-- We'll create partitions for 2020-2025, adjust as needed
DO $$
DECLARE
    year_val INTEGER;
    month_val INTEGER;
BEGIN
    FOR year_val IN 2020..2025 LOOP
        FOR month_val IN 1..12 LOOP
            EXECUTE format('
                CREATE TABLE nyc_taxi_dw.trips_fact_y%sm%s PARTITION OF nyc_taxi_dw.trips_fact
                FOR VALUES FROM (%s, %s) TO (%s, %s)',
                year_val, 
                lpad(month_val::text, 2, '0'),
                year_val, month_val,
                year_val, month_val + 1
            );
            
            -- Add primary key to each partition
            EXECUTE format('ALTER TABLE nyc_taxi_dw.trips_fact_y%sm%s ADD PRIMARY KEY (trip_key)', 
                          year_val, lpad(month_val::text, 2, '0'));
            
            -- Add indexes to each partition
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_pickup_datetime ON nyc_taxi_dw.trips_fact_y%sm%s (pickup_datetime)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_dropoff_datetime ON nyc_taxi_dw.trips_fact_y%sm%s (dropoff_datetime)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_pickup_location ON nyc_taxi_dw.trips_fact_y%sm%s (pickup_location_key)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_dropoff_location ON nyc_taxi_dw.trips_fact_y%sm%s (dropoff_location_key)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_payment_type ON nyc_taxi_dw.trips_fact_y%sm%s (payment_key)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
            EXECUTE format('CREATE INDEX idx_trips_fact_y%sm%s_vendor ON nyc_taxi_dw.trips_fact_y%sm%s (vendor_key)',
                          year_val, lpad(month_val::text, 2, '0'),
                          year_val, lpad(month_val::text, 2, '0'));
        END LOOP;
    END LOOP;
END $$;

-- 2. ADVANCED INDEXING STRATEGIES

-- Multi-column indexes for common analytical queries
CREATE INDEX idx_trips_fact_analytics_composite ON nyc_taxi_dw.trips_fact (year, month, pickup_datetime, pickup_location_key);
CREATE INDEX idx_trips_fact_revenue_analysis ON nyc_taxi_dw.trips_fact (year, month, pickup_datetime, fare_amount, total_amount);
CREATE INDEX idx_trips_fact_location_time ON nyc_taxi_dw.trips_fact (pickup_location_key, dropoff_location_key, pickup_datetime);

-- Partial indexes for frequently filtered data
CREATE INDEX idx_trips_fact_cash_payments_partial ON nyc_taxi_dw.trips_fact (trip_key) WHERE payment_key = 2; -- Cash payments
CREATE INDEX idx_trips_fact_credit_payments_partial ON nyc_taxi_dw.trips_fact (trip_key) WHERE payment_key = 1; -- Credit payments
CREATE INDEX idx_trips_fact_peak_hours_partial ON nyc_taxi_dw.trips_fact (trip_key) 
WHERE EXTRACT(HOUR FROM pickup_datetime) BETWEEN 7 AND 9 OR EXTRACT(HOUR FROM pickup_datetime) BETWEEN 17 AND 19;

-- BRIN indexes for large tables (good for time-series data)
CREATE INDEX idx_trips_fact_pickup_datetime_brin ON nyc_taxi_dw.trips_fact USING BRIN (pickup_datetime);
CREATE INDEX idx_trips_fact_year_brin ON nyc_taxi_dw.trips_fact USING BRIN (year);

-- 3. STATISTICS OPTIMIZATION
-- Update statistics for query planner
ANALYZE nyc_taxi_dw.vendor_dim;
ANALYZE nyc_taxi_dw.location_dim;
ANALYZE nyc_taxi_dw.time_dim;
ANALYZE nyc_taxi_dw.payment_dim;
ANALYZE nyc_taxi_dw.rate_code_dim;
ANALYZE nyc_taxi_dw.trip_type_dim;

-- 4. VACUUM AND MAINTENANCE PROCEDURES

-- Function to perform regular maintenance on fact table partitions
CREATE OR REPLACE FUNCTION nyc_taxi_dw.maintain_partitions(months_back INTEGER DEFAULT 3)
RETURNS TABLE(partition_name TEXT, operation TEXT, rows_affected BIGINT) AS $$
DECLARE
    partition_name TEXT;
    sql_cmd TEXT;
    result RECORD;
BEGIN
    -- Loop through recent partitions and perform maintenance
    FOR partition_name IN
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'nyc_taxi_dw' 
          AND tablename LIKE 'trips_fact_y%sm%'
          AND substring(tablename, 15, 4)::INTEGER >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 year')
        ORDER BY tablename DESC
        LIMIT months_back * 12
    LOOP
        -- Analyze the partition
        sql_cmd := format('ANALYZE nyc_taxi_dw.%I', partition_name);
        EXECUTE sql_cmd;
        
        RETURN QUERY SELECT partition_name, 'ANALYZE'::TEXT, NULL::BIGINT;
        
        -- Optionally VACUUM the partition (use VACUUM ANALYZE for both)
        sql_cmd := format('VACUUM ANALYZE nyc_taxi_dw.%I', partition_name);
        EXECUTE sql_cmd;
        
        RETURN QUERY SELECT partition_name, 'VACUUM ANALYZE'::TEXT, NULL::BIGINT;
    END LOOP;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- 5. TABLE CLUSTERING FOR PERFORMANCE
-- Cluster fact table by pickup_datetime to improve sequential access
-- Note: This is typically done after initial data load and periodically thereafter
CREATE OR REPLACE FUNCTION nyc_taxi_dw.cluster_recent_partitions()
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    sql_cmd TEXT;
BEGIN
    -- Get the most recent partitions and cluster them
    FOR partition_name IN
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = 'nyc_taxi_dw' 
          AND tablename LIKE 'trips_fact_y%sm%'
          AND substring(tablename, 15, 4)::INTEGER >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
        ORDER BY tablename DESC
        LIMIT 6  -- Last 6 months
    LOOP
        -- Cluster by pickup_datetime for time-series queries
        sql_cmd := format('CLUSTER nyc_taxi_dw.%I USING idx_trips_fact_%I_pickup_datetime', 
                         partition_name, partition_name);
        EXECUTE sql_cmd;
        
        -- Analyze after clustering
        sql_cmd := format('ANALYZE nyc_taxi_dw.%I', partition_name);
        EXECUTE sql_cmd;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 6. COMPRESSION FOR SPACE OPTIMIZATION
-- Create a compressed table for archived data (example for data older than 1 year)
CREATE OR REPLACE FUNCTION nyc_taxi_dw.create_compressed_archive_table()
RETURNS VOID AS $$
DECLARE
    year_val INTEGER;
    month_val INTEGER;
BEGIN
    -- Create archive partitions with compression
    FOR year_val IN 2020..EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 year')::INTEGER LOOP
        FOR month_val IN 1..12 LOOP
            EXECUTE format('
                CREATE TABLE nyc_taxi_dw.trips_fact_archive_y%sm%s (
                    LIKE nyc_taxi_dw.trips_fact INCLUDING ALL
                ) WITH (OIDS=FALSE);
                
                ALTER TABLE nyc_taxi_dw.trips_fact_archive_y%sm%s ADD PRIMARY KEY (trip_key);
                
                -- Add indexes
                CREATE INDEX idx_trips_fact_archive_y%sm%s_pickup_datetime ON nyc_taxi_dw.trips_fact_archive_y%sm%s (pickup_datetime);
                CREATE INDEX idx_trips_fact_archive_y%sm%s_dropoff_datetime ON nyc_taxi_dw.trips_fact_archive_y%sm%s (dropoff_datetime);
                ',
                year_val, lpad(month_val::text, 2, '0'),
                year_val, lpad(month_val::text, 2, '0'),
                year_val, lpad(month_val::text, 2, '0'),
                year_val, lpad(month_val::text, 2, '0'),
                year_val, lpad(month_val::text, 2, '0'),
                year_val, lpad(month_val::text, 2, '0')
            );
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 7. MAINTENANCE SCHEDULING
-- Create a procedure to run regular maintenance tasks
CREATE OR REPLACE PROCEDURE nyc_taxi_dw.run_daily_maintenance()
AS $$
BEGIN
    -- Update statistics
    ANALYZE nyc_taxi_dw.trips_fact;
    ANALYZE nyc_taxi_dw.location_dim;
    ANALYZE nyc_taxi_dw.time_dim;
    
    -- Perform maintenance on recent partitions
    PERFORM nyc_taxi_dw.maintain_partitions(3);
    
    -- Refresh materialized views
    REFRESH MATERIALIZED VIEW CONCURRENTLY nyc_taxi_dw.location_lookup_mv;
    
    -- Log maintenance activity
    INSERT INTO taxi_data.pipeline_logs (
        pipeline_name, run_id, status, start_time, end_time, 
        records_processed, records_failed
    ) VALUES (
        'daily_maintenance', 
        'maint_' || CURRENT_DATE::TEXT, 
        'SUCCESS', 
        NOW() - INTERVAL '5 minutes', 
        NOW(), 
        0, 0
    );
END;
$$ LANGUAGE plpgsql;

-- 8. PERFORMANCE OPTIMIZATION SETTINGS
-- These should be set in postgresql.conf but provided here as ALTER SYSTEM commands
-- Uncomment and run these in production as needed (requires superuser privileges)

/*
-- Shared buffers (typically 25% of available RAM)
ALTER SYSTEM SET shared_buffers = '4GB';

-- Effective cache size (typically 50-75% of available RAM)  
ALTER SYSTEM SET effective_cache_size = '12GB';

-- Maintenance work memory (for VACUUM, CREATE INDEX, etc.)
ALTER SYSTEM SET maintenance_work_mem = '1GB';

-- Work memory (for sorting, hashing operations)
ALTER SYSTEM SET work_mem = '32MB';

-- Checkpoint settings for better I/O performance
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET checkpoint_warning = 30;
ALTER SYSTEM SET wal_buffers = '16MB';

-- Random page cost (adjust based on storage type - 1.0 for SSD, 2.0-4.0 for HDD)
ALTER SYSTEM SET random_page_cost = 1.1;
*/

-- 9. QUERY OPTIMIZATION HINTS
-- Function to provide EXPLAIN ANALYZE template for performance testing
CREATE OR REPLACE FUNCTION nyc_taxi_dw.explain_query_template()
RETURNS TABLE(query_name TEXT, query_text TEXT) AS $$
BEGIN
    RETURN QUERY VALUES 
    ('Daily Trip Count by Zone', 
     'EXPLAIN (ANALYZE, BUFFERS) SELECT ld.zone, COUNT(*) as trip_count FROM nyc_taxi_dw.trips_fact tf JOIN nyc_taxi_dw.location_dim ld ON tf.pickup_location_key = ld.location_key WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''1 day'' GROUP BY ld.zone ORDER BY trip_count DESC LIMIT 10;'),
    
    ('Revenue by Hour', 
     'EXPLAIN (ANALYZE, BUFFERS) SELECT EXTRACT(HOUR FROM tf.pickup_datetime) as hour, SUM(tf.total_amount) as revenue FROM nyc_taxi_dw.trips_fact tf WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''7 days'' GROUP BY hour ORDER BY hour;'),
    
    ('Peak Location Pairs', 
     'EXPLAIN (ANALYZE, BUFFERS) SELECT pl.zone as pickup_zone, dl.zone as dropoff_zone, COUNT(*) as trip_count FROM nyc_taxi_dw.trips_fact tf JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''30 days'' GROUP BY pl.zone, dl.zone ORDER BY trip_count DESC LIMIT 10;');
END;
$$ LANGUAGE plpgsql;

-- 10. MONITORING QUERIES
-- Useful queries for monitoring performance
CREATE OR REPLACE VIEW nyc_taxi_dw.performance_monitoring_v AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins,
    n_tup_upd, 
    n_tup_del,
    n_tup_hot_upd,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname = 'nyc_taxi_dw';

-- Index usage statistics
CREATE OR REPLACE VIEW nyc_taxi_dw.index_usage_v AS
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'nyc_taxi_dw';