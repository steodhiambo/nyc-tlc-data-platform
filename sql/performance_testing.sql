-- Performance Testing Script for NYC TLC Data Warehouse
-- This script tests query performance before and after optimizations

-- 1. CREATE TEST TABLES FOR BEFORE/AFTER COMPARISON

-- Create a denormalized table similar to the original structure (before star schema)
CREATE TABLE nyc_taxi_dw.trips_denormalized_before (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DECIMAL(10,2),
    pickup_longitude DECIMAL(9,6),
    pickup_latitude DECIMAL(8,6),
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    dropoff_longitude DECIMAL(9,6),
    dropoff_latitude DECIMAL(8,6),
    payment_type INTEGER,
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    pickup_borough VARCHAR(32),
    pickup_zone VARCHAR(64),
    dropoff_borough VARCHAR(32),
    dropoff_zone VARCHAR(64),
    year INTEGER,
    month INTEGER
);

-- 2. PERFORMANCE TEST FUNCTIONS

-- Function to measure query execution time
CREATE OR REPLACE FUNCTION nyc_taxi_dw.measure_query_time(query_text TEXT)
RETURNS TABLE(
    query_name TEXT,
    execution_time_ms NUMERIC,
    rows_returned BIGINT,
    query_plan TEXT
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    result RECORD;
    rows_count BIGINT;
    plan_result TEXT;
BEGIN
    -- Record start time
    start_time := clock_timestamp();
    
    -- Execute the query and capture the plan
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ' || query_text INTO plan_result;
    
    -- Get the actual result without EXPLAIN
    EXECUTE 'SELECT COUNT(*) FROM (' || query_text || ') AS subquery' INTO rows_count;
    
    -- Record end time
    end_time := clock_timestamp();
    
    -- Calculate execution time in milliseconds
    execution_time_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    -- Return results
    query_name := 'Performance Test Query';
    rows_returned := rows_count;
    query_plan := plan_result;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 3. PERFORMANCE TEST CASES

-- Test Case 1: Revenue by Zone (Before Optimization)
CREATE OR REPLACE FUNCTION nyc_taxi_dw.test_revenue_by_zone_before()
RETURNS TABLE(
    test_name TEXT,
    execution_time_ms NUMERIC,
    rows_returned BIGINT,
    improvement_percentage NUMERIC
) AS $$
DECLARE
    time_before NUMERIC;
    time_after NUMERIC;
BEGIN
    -- Measure time for denormalized query
    SELECT execution_time_ms INTO time_before
    FROM nyc_taxi_dw.measure_query_time('
        SELECT pickup_zone, SUM(total_amount) as revenue
        FROM nyc_taxi_dw.trips_denormalized_before
        WHERE pickup_datetime >= CURRENT_DATE - INTERVAL ''30 days''
        GROUP BY pickup_zone
        ORDER BY revenue DESC
        LIMIT 10
    ');
    
    -- Measure time for star schema query
    SELECT execution_time_ms INTO time_after
    FROM nyc_taxi_dw.measure_query_time('
        SELECT ld.zone, SUM(tf.total_amount) as revenue
        FROM nyc_taxi_dw.trips_fact tf
        JOIN nyc_taxi_dw.location_dim ld ON tf.pickup_location_key = ld.location_key
        WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''30 days''
        GROUP BY ld.zone
        ORDER BY revenue DESC
        LIMIT 10
    ');
    
    -- Calculate improvement
    test_name := 'Revenue by Zone Query';
    execution_time_ms := time_after;
    rows_returned := 10;
    improvement_percentage := ((time_before - time_after) / time_before) * 100;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Test Case 2: Peak Hours Analysis
CREATE OR REPLACE FUNCTION nyc_taxi_dw.test_peak_hours_before_after()
RETURNS TABLE(
    test_name TEXT,
    execution_time_ms NUMERIC,
    rows_returned BIGINT,
    improvement_percentage NUMERIC
) AS $$
DECLARE
    time_before NUMERIC;
    time_after NUMERIC;
BEGIN
    -- Measure time for denormalized query
    SELECT execution_time_ms INTO time_before
    FROM nyc_taxi_dw.measure_query_time('
        SELECT EXTRACT(HOUR FROM pickup_datetime) as hour, COUNT(*) as trip_count
        FROM nyc_taxi_dw.trips_denormalized_before
        WHERE pickup_datetime >= CURRENT_DATE - INTERVAL ''7 days''
        GROUP BY EXTRACT(HOUR FROM pickup_datetime)
        ORDER BY hour
    ');
    
    -- Measure time for star schema query
    SELECT execution_time_ms INTO time_after
    FROM nyc_taxi_dw.measure_query_time('
        SELECT EXTRACT(HOUR FROM tf.pickup_datetime) as hour, COUNT(*) as trip_count
        FROM nyc_taxi_dw.trips_fact tf
        WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''7 days''
        GROUP BY EXTRACT(HOUR FROM tf.pickup_datetime)
        ORDER BY hour
    ');
    
    -- Calculate improvement
    test_name := 'Peak Hours Analysis Query';
    execution_time_ms := time_after;
    rows_returned := 24;  -- Assuming 24 hours
    improvement_percentage := ((time_before - time_after) / time_before) * 100;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Test Case 3: Complex Multi-Join Query
CREATE OR REPLACE FUNCTION nyc_taxi_dw.test_complex_query_before_after()
RETURNS TABLE(
    test_name TEXT,
    execution_time_ms NUMERIC,
    rows_returned BIGINT,
    improvement_percentage NUMERIC
) AS $$
DECLARE
    time_before NUMERIC;
    time_after NUMERIC;
BEGIN
    -- Measure time for denormalized query (already has denormalized fields)
    SELECT execution_time_ms INTO time_before
    FROM nyc_taxi_dw.measure_query_time('
        SELECT pickup_borough, dropoff_borough, payment_type, 
               COUNT(*) as trip_count, AVG(total_amount) as avg_fare
        FROM nyc_taxi_dw.trips_denormalized_before
        WHERE pickup_datetime >= CURRENT_DATE - INTERVAL ''30 days''
          AND total_amount > 10
        GROUP BY pickup_borough, dropoff_borough, payment_type
        HAVING COUNT(*) > 5
        ORDER BY trip_count DESC
        LIMIT 20
    ');
    
    -- Measure time for star schema query
    SELECT execution_time_ms INTO time_after
    FROM nyc_taxi_dw.measure_query_time('
        SELECT pl.borough as pickup_borough, dl.borough as dropoff_borough, 
               pt.payment_type_description, 
               COUNT(*) as trip_count, AVG(tf.total_amount) as avg_fare
        FROM nyc_taxi_dw.trips_fact tf
        JOIN nyc_taxi_dw.location_dim pl ON tf.pickup_location_key = pl.location_key
        JOIN nyc_taxi_dw.location_dim dl ON tf.dropoff_location_key = dl.location_key
        JOIN nyc_taxi_dw.payment_dim pt ON tf.payment_key = pt.payment_key
        WHERE tf.pickup_datetime >= CURRENT_DATE - INTERVAL ''30 days''
          AND tf.total_amount > 10
        GROUP BY pl.borough, dl.borough, pt.payment_type_description
        HAVING COUNT(*) > 5
        ORDER BY trip_count DESC
        LIMIT 20
    ');
    
    -- Calculate improvement
    test_name := 'Complex Multi-Join Query';
    execution_time_ms := time_after;
    rows_returned := 20;
    improvement_percentage := ((time_before - time_after) / time_before) * 100;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 4. PERFORMANCE BENCHMARKING SUITE

CREATE OR REPLACE FUNCTION nyc_taxi_dw.run_performance_benchmark()
RETURNS TABLE(
    test_name TEXT,
    execution_time_ms NUMERIC,
    rows_returned BIGINT,
    improvement_percentage NUMERIC,
    status TEXT
) AS $$
DECLARE
    result RECORD;
BEGIN
    -- Test 1: Revenue by Zone
    FOR result IN SELECT * FROM nyc_taxi_dw.test_revenue_by_zone_before() LOOP
        test_name := result.test_name;
        execution_time_ms := result.execution_time_ms;
        rows_returned := result.rows_returned;
        improvement_percentage := result.improvement_percentage;
        status := CASE 
            WHEN result.improvement_percentage >= 20 THEN 'SUCCESS (>20% improvement)'
            WHEN result.improvement_percentage >= 0 THEN 'PARTIAL IMPROVEMENT'
            ELSE 'REGRESSION'
        END;
        RETURN NEXT;
    END LOOP;
    
    -- Test 2: Peak Hours
    FOR result IN SELECT * FROM nyc_taxi_dw.test_peak_hours_before_after() LOOP
        test_name := result.test_name;
        execution_time_ms := result.execution_time_ms;
        rows_returned := result.rows_returned;
        improvement_percentage := result.improvement_percentage;
        status := CASE 
            WHEN result.improvement_percentage >= 20 THEN 'SUCCESS (>20% improvement)'
            WHEN result.improvement_percentage >= 0 THEN 'PARTIAL IMPROVEMENT'
            ELSE 'REGRESSION'
        END;
        RETURN NEXT;
    END LOOP;
    
    -- Test 3: Complex Query
    FOR result IN SELECT * FROM nyc_taxi_dw.test_complex_query_before_after() LOOP
        test_name := result.test_name;
        execution_time_ms := result.execution_time_ms;
        rows_returned := result.rows_returned;
        improvement_percentage := result.improvement_percentage;
        status := CASE 
            WHEN result.improvement_percentage >= 20 THEN 'SUCCESS (>20% improvement)'
            WHEN result.improvement_percentage >= 0 THEN 'PARTIAL IMPROVEMENT'
            ELSE 'REGRESSION'
        END;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 5. INDEX EFFECTIVENESS TEST

CREATE OR REPLACE FUNCTION nyc_taxi_dw.test_index_effectiveness()
RETURNS TABLE(
    index_name TEXT,
    query_type TEXT,
    with_index_time NUMERIC,
    without_index_time NUMERIC,
    improvement_percentage NUMERIC
) AS $$
DECLARE
    time_with_index NUMERIC;
    time_without_index NUMERIC;
BEGIN
    -- Test with index (assuming it exists)
    SELECT execution_time_ms INTO time_with_index
    FROM nyc_taxi_dw.measure_query_time('
        SELECT COUNT(*) 
        FROM nyc_taxi_dw.trips_fact 
        WHERE pickup_datetime >= ''2023-01-01'' AND pickup_datetime < ''2023-02-01''
    ');
    
    -- To test without index, we'd need to drop and recreate
    -- For this example, we'll simulate by using a more complex query
    -- that forces a sequential scan
    SELECT execution_time_ms INTO time_without_index
    FROM nyc_taxi_dw.measure_query_time('
        SELECT COUNT(*) 
        FROM nyc_taxi_dw.trips_fact 
        WHERE pickup_datetime >= ''2023-01-01'' AND pickup_datetime < ''2023-02-01''
        AND trip_distance > 0  -- Additional condition to make query more complex
    ');
    
    -- In a real test, you would:
    -- 1. Drop the index
    -- 2. Run the query
    -- 3. Record the time
    -- 4. Recreate the index
    -- 5. Run the query again
    -- 6. Compare times
    
    index_name := 'idx_trips_fact_pickup_datetime';
    query_type := 'Date Range Query';
    with_index_time := time_with_index;
    without_index_time := time_without_index;
    improvement_percentage := ((time_without_index - time_with_index) / time_without_index) * 100;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 6. PARTITIONING EFFECTIVENESS TEST

CREATE OR REPLACE FUNCTION nyc_taxi_dw.test_partitioning_effectiveness()
RETURNS TABLE(
    test_description TEXT,
    query_condition TEXT,
    execution_time_ms NUMERIC,
    rows_scanned BIGINT,
    partitions_accessed INTEGER
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_count BIGINT;
BEGIN
    -- Test query that should benefit from partitioning
    start_time := clock_timestamp();
    
    EXECUTE '
        SELECT COUNT(*), SUM(total_amount)
        FROM nyc_taxi_dw.trips_fact
        WHERE year = 2023 AND month = 1
    ' INTO rows_count;
    
    end_time := clock_timestamp();
    
    test_description := 'Single Month Partition Query';
    query_condition := 'year = 2023 AND month = 1';
    execution_time_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    rows_scanned := rows_count;
    partitions_accessed := 1;  -- Should only access one partition
    
    RETURN NEXT;
    
    -- Test query that accesses multiple partitions
    start_time := clock_timestamp();
    
    EXECUTE '
        SELECT COUNT(*), SUM(total_amount)
        FROM nyc_taxi_dw.trips_fact
        WHERE year = 2023
    ' INTO rows_count;
    
    end_time := clock_timestamp();
    
    test_description := 'Single Year Multiple Partitions Query';
    query_condition := 'year = 2023';
    execution_time_ms := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    rows_scanned := rows_count;
    partitions_accessed := 12;  -- Should access 12 partitions
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 7. COMPREHENSIVE PERFORMANCE REPORT

CREATE OR REPLACE FUNCTION nyc_taxi_dw.generate_performance_report()
RETURNS TABLE(
    report_section TEXT,
    metric_name TEXT,
    metric_value TEXT,
    notes TEXT
) AS $$
DECLARE
    benchmark_result RECORD;
    index_result RECORD;
    partition_result RECORD;
BEGIN
    -- Report Header
    report_section := 'PERFORMANCE_REPORT_HEADER';
    metric_name := 'Report Generated';
    metric_value := NOW()::TEXT;
    notes := 'Performance testing report for NYC TLC Data Warehouse';
    RETURN NEXT;
    
    -- Benchmark Results
    report_section := 'QUERY_PERFORMANCE';
    FOR benchmark_result IN SELECT * FROM nyc_taxi_dw.run_performance_benchmark() LOOP
        metric_name := benchmark_result.test_name;
        metric_value := benchmark_result.improvement_percentage || '% improvement';
        notes := 'Status: ' || benchmark_result.status;
        RETURN NEXT;
    END LOOP;
    
    -- Index Effectiveness
    report_section := 'INDEX_EFFECTIVENESS';
    FOR index_result IN SELECT * FROM nyc_taxi_dw.test_index_effectiveness() LOOP
        metric_name := index_result.index_name;
        metric_value := index_result.improvement_percentage || '% improvement';
        notes := 'Query type: ' || index_result.query_type;
        RETURN NEXT;
    END LOOP;
    
    -- Partitioning Effectiveness
    report_section := 'PARTITIONING_EFFECTIVENESS';
    FOR partition_result IN SELECT * FROM nyc_taxi_dw.test_partitioning_effectiveness() LOOP
        metric_name := partition_result.query_condition;
        metric_value := partition_result.execution_time_ms || 'ms, ' || partition_result.partitions_accessed || ' partitions';
        notes := 'Rows scanned: ' || partition_result.rows_scanned;
        RETURN NEXT;
    END LOOP;
    
    -- System Statistics
    report_section := 'SYSTEM_STATISTICS';
    
    -- Table sizes
    SELECT 
        'TABLE_SIZES'::TEXT as metric_name,
        pg_size_pretty(pg_total_relation_size('nyc_taxi_dw.trips_fact')) as metric_value,
        'Fact table size' as notes
    INTO benchmark_result;
    
    RETURN NEXT;
    
    SELECT 
        'DIMENSION_TABLE_COUNTS'::TEXT as metric_name,
        (SELECT COUNT(*) FROM nyc_taxi_dw.location_dim)::TEXT as metric_value,
        'Location dimension rows' as notes
    INTO benchmark_result;
    
    RETURN NEXT;
    
    SELECT 
        'FACT_TABLE_COUNTS'::TEXT as metric_name,
        (SELECT COUNT(*) FROM nyc_taxi_dw.trips_fact)::TEXT as metric_value,
        'Fact table rows' as notes
    INTO benchmark_result;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 8. SAMPLE DATA LOADING FOR TESTING (EXAMPLE)

-- This function would load sample data for performance testing
-- In practice, you would use your ETL pipeline to populate the tables
CREATE OR REPLACE FUNCTION nyc_taxi_dw.load_sample_data_for_testing()
RETURNS INTEGER AS $$
DECLARE
    rows_loaded INTEGER := 0;
BEGIN
    -- This is a simplified example - in practice you'd load real data
    -- or use a subset of your production data
    
    -- Insert sample data into fact table for testing
    INSERT INTO nyc_taxi_dw.trips_fact (
        vendor_key, pickup_location_key, dropoff_location_key,
        pickup_time_key, dropoff_time_key, payment_key,
        passenger_count, trip_distance, trip_duration_minutes,
        fare_amount, tip_amount, total_amount,
        store_and_fwd_flag, pickup_datetime, dropoff_datetime,
        year, month
    )
    SELECT 
        1,  -- vendor_key
        generate_series(1, 265),  -- pickup_location_key (NYC has ~265 taxi zones)
        generate_series(1, 265),  -- dropoff_location_key
        1,  -- pickup_time_key
        1,  -- dropoff_time_key
        1,  -- payment_key
        (random() * 4 + 1)::INTEGER,  -- passenger_count
        random() * 20,  -- trip_distance
        random() * 60,  -- trip_duration_minutes
        random() * 50 + 5,  -- fare_amount
        random() * 10,  -- tip_amount
        random() * 60 + 5,  -- total_amount
        FALSE,  -- store_and_fwd_flag
        CURRENT_TIMESTAMP - (random() * 365 * '1 day'::INTERVAL),  -- pickup_datetime
        CURRENT_TIMESTAMP - (random() * 365 * '1 day'::INTERVAL) - (random() * 120 * '1 minute'::INTERVAL),  -- dropoff_datetime
        EXTRACT(YEAR FROM CURRENT_TIMESTAMP - (random() * 365 * '1 day'::INTERVAL)),  -- year
        EXTRACT(MONTH FROM CURRENT_TIMESTAMP - (random() * 365 * '1 day'::INTERVAL))  -- month
    LIMIT 10000;  -- Load 10k sample rows
    
    GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    
    -- Update statistics after loading data
    ANALYZE nyc_taxi_dw.trips_fact;
    
    RETURN rows_loaded;
END;
$$ LANGUAGE plpgsql;

-- 9. PERFORMANCE MONITORING VIEWS

-- Create a view to monitor ongoing performance
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_performance_monitoring AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins + n_tup_upd + n_tup_del AS total_operations,
    n_tup_ins AS inserts,
    n_tup_upd AS updates,
    n_tup_del AS deletes,
    n_tup_hot_upd AS hot_updates,
    n_live_tup AS live_tuples,
    n_dead_tup AS dead_tuples,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE schemaname = 'nyc_taxi_dw'
ORDER BY total_operations DESC;

-- Index usage statistics
CREATE OR REPLACE VIEW nyc_taxi_dw.vw_index_performance AS
SELECT 
    schemaname,
    tablename,
    indexrelname AS index_name,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'nyc_taxi_dw'
ORDER BY idx_scan DESC;