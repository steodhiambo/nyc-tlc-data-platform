-- Data Warehouse Schema for NYC TLC Taxi Data
-- Dimensional Modeling: Star Schema with Fact and Dimension Tables

-- Create schema for data warehouse
CREATE SCHEMA IF NOT EXISTS nyc_taxi_dw;

-- Time Dimension Table
CREATE TABLE nyc_taxi_dw.time_dim (
    time_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    hour_of_day INTEGER NOT NULL,
    minute_of_hour INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for time dimension
CREATE INDEX idx_time_dim_date ON nyc_taxi_dw.time_dim (full_date);
CREATE INDEX idx_time_dim_year_month ON nyc_taxi_dw.time_dim (year, month);

-- Location Dimension Table
CREATE TABLE nyc_taxi_dw.location_dim (
    location_key SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL, -- Original TLC location ID
    borough VARCHAR(32) NOT NULL,
    zone VARCHAR(64) NOT NULL,
    service_zone VARCHAR(32) NOT NULL,
    latitude DECIMAL(8,6),
    longitude DECIMAL(9,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for location dimension
CREATE INDEX idx_location_borough ON nyc_taxi_dw.location_dim (borough);
CREATE INDEX idx_location_zone ON nyc_taxi_dw.location_dim (zone);

-- Payment Type Dimension Table
CREATE TABLE nyc_taxi_dw.payment_dim (
    payment_key SERIAL PRIMARY KEY,
    payment_type_id INTEGER NOT NULL,
    payment_type_description VARCHAR(50) NOT NULL,
    payment_type_category VARCHAR(30) NOT NULL, -- cash, credit card, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rate Code Dimension Table
CREATE TABLE nyc_taxi_dw.rate_code_dim (
    rate_code_key SERIAL PRIMARY KEY,
    rate_code_id INTEGER NOT NULL,
    rate_code_description VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trip Type Dimension Table (for green taxi trips)
CREATE TABLE nyc_taxi_dw.trip_type_dim (
    trip_type_key SERIAL PRIMARY KEY,
    trip_type_id INTEGER NOT NULL,
    trip_type_description VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vendor Dimension Table
CREATE TABLE nyc_taxi_dw.vendor_dim (
    vendor_key SERIAL PRIMARY KEY,
    vendor_id INTEGER NOT NULL,
    vendor_name VARCHAR(100),
    vendor_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trips Fact Table (Star Schema)
CREATE TABLE nyc_taxi_dw.trips_fact (
    trip_key BIGSERIAL PRIMARY KEY,
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
    
    -- Foreign key constraints
    CONSTRAINT fk_trips_vendor FOREIGN KEY (vendor_key) REFERENCES nyc_taxi_dw.vendor_dim(vendor_key),
    CONSTRAINT fk_trips_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES nyc_taxi_dw.location_dim(location_key),
    CONSTRAINT fk_trips_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES nyc_taxi_dw.location_dim(location_key),
    CONSTRAINT fk_trips_pickup_time FOREIGN KEY (pickup_time_key) REFERENCES nyc_taxi_dw.time_dim(time_key),
    CONSTRAINT fk_trips_dropoff_time FOREIGN KEY (dropoff_time_key) REFERENCES nyc_taxi_dw.time_dim(time_key),
    CONSTRAINT fk_trips_payment FOREIGN KEY (payment_key) REFERENCES nyc_taxi_dw.payment_dim(payment_key),
    CONSTRAINT fk_trips_rate_code FOREIGN KEY (rate_code_key) REFERENCES nyc_taxi_dw.rate_code_dim(rate_code_key),
    CONSTRAINT fk_trips_trip_type FOREIGN KEY (trip_type_key) REFERENCES nyc_taxi_dw.trip_type_dim(trip_type_key)
);

-- Create indexes for fact table (critical for performance)
CREATE INDEX idx_trips_fact_pickup_datetime ON nyc_taxi_dw.trips_fact (pickup_datetime);
CREATE INDEX idx_trips_fact_dropoff_datetime ON nyc_taxi_dw.trips_fact (dropoff_datetime);
CREATE INDEX idx_trips_fact_pickup_location ON nyc_taxi_dw.trips_fact (pickup_location_key);
CREATE INDEX idx_trips_fact_dropoff_location ON nyc_taxi_dw.trips_fact (dropoff_location_key);
CREATE INDEX idx_trips_fact_year_month ON nyc_taxi_dw.trips_fact (year, month);
CREATE INDEX idx_trips_fact_payment_type ON nyc_taxi_dw.trips_fact (payment_key);
CREATE INDEX idx_trips_fact_vendor ON nyc_taxi_dw.trips_fact (vendor_key);

-- Create a composite index for common analytical queries
CREATE INDEX idx_trips_fact_analytics ON nyc_taxi_dw.trips_fact (pickup_datetime, pickup_location_key, dropoff_location_key, fare_amount);

-- Insert reference data for payment types
INSERT INTO nyc_taxi_dw.payment_dim (payment_type_id, payment_type_description, payment_type_category) VALUES
(1, 'Credit Card', 'Credit Card'),
(2, 'Cash', 'Cash'),
(3, 'No Charge', 'Other'),
(4, 'Dispute', 'Other'),
(5, 'Unknown', 'Other'),
(6, 'Voided Trip', 'Other');

-- Insert reference data for rate codes
INSERT INTO nyc_taxi_dw.rate_code_dim (rate_code_id, rate_code_description) VALUES
(1, 'Standard Rate'),
(2, 'JFK Airport'),
(3, 'Newark Airport'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated Fare'),
(6, 'Group Ride');

-- Insert reference data for trip types (green taxi)
INSERT INTO nyc_taxi_dw.trip_type_dim (trip_type_id, trip_type_description) VALUES
(1, 'Street Hail'),
(2, 'Dispatch');

-- Insert reference data for vendors
INSERT INTO nyc_taxi_dw.vendor_dim (vendor_id, vendor_name, vendor_description) VALUES
(1, 'Creative Mobile Technologies, LLC', 'Third-party vendor for TPEP'),
(2, 'VeriFone Inc.', 'Third-party vendor for TPEP'),
(4, 'Square', 'Third-party vendor for FHV');

-- Function to get or create time dimension record
CREATE OR REPLACE FUNCTION nyc_taxi_dw.get_or_create_time_dim(input_datetime TIMESTAMP)
RETURNS INTEGER AS $$
DECLARE
    time_key INTEGER;
    hour_val INTEGER;
    minute_val INTEGER;
BEGIN
    hour_val := EXTRACT(HOUR FROM input_datetime);
    minute_val := EXTRACT(MINUTE FROM input_datetime);
    
    -- Try to find existing record
    SELECT t.time_key INTO time_key
    FROM nyc_taxi_dw.time_dim t
    WHERE t.full_date = input_datetime::DATE
      AND t.hour_of_day = hour_val
      AND t.minute_of_hour = minute_val
    LIMIT 1;
    
    -- If not found, create new record
    IF time_key IS NULL THEN
        INSERT INTO nyc_taxi_dw.time_dim (
            full_date, year, quarter, month, month_name, day_of_month,
            day_of_week, day_name, hour_of_day, minute_of_hour, is_weekend
        )
        VALUES (
            input_datetime::DATE,
            EXTRACT(YEAR FROM input_datetime),
            EXTRACT(QUARTER FROM input_datetime),
            EXTRACT(MONTH FROM input_datetime),
            TO_CHAR(input_datetime, 'Month'),
            EXTRACT(DAY FROM input_datetime),
            EXTRACT(DOW FROM input_datetime),
            TO_CHAR(input_datetime, 'Day'),
            hour_val,
            minute_val,
            CASE WHEN EXTRACT(DOW FROM input_datetime) IN (0, 6) THEN TRUE ELSE FALSE END
        )
        RETURNING time_key INTO time_key;
    END IF;
    
    RETURN time_key;
END;
$$ LANGUAGE plpgsql;

-- Function to get or create location dimension record
CREATE OR REPLACE FUNCTION nyc_taxi_dw.get_or_create_location_dim(
    input_location_id INTEGER,
    input_borough VARCHAR,
    input_zone VARCHAR,
    input_service_zone VARCHAR
)
RETURNS INTEGER AS $$
DECLARE
    location_key INTEGER;
BEGIN
    -- Try to find existing record
    SELECT l.location_key INTO location_key
    FROM nyc_taxi_dw.location_dim l
    WHERE l.location_id = input_location_id
    LIMIT 1;
    
    -- If not found, create new record
    IF location_key IS NULL THEN
        INSERT INTO nyc_taxi_dw.location_dim (
            location_id, borough, zone, service_zone
        )
        VALUES (input_location_id, input_borough, input_zone, input_service_zone)
        RETURNING location_key INTO location_key;
    END IF;
    
    RETURN location_key;
END;
$$ LANGUAGE plpgsql;

-- Insert initial location data from the existing taxi zone lookup
INSERT INTO nyc_taxi_dw.location_dim (location_id, borough, zone, service_zone)
SELECT 
    location_id,
    borough,
    zone,
    service_zone
FROM taxi_data.taxi_zone_lookup
ON CONFLICT (location_id) DO NOTHING;

-- Create a materialized view for quick access to location data
CREATE MATERIALIZED VIEW nyc_taxi_dw.location_lookup_mv AS
SELECT 
    location_key,
    location_id,
    borough,
    zone,
    service_zone
FROM nyc_taxi_dw.location_dim;

CREATE INDEX idx_location_lookup_mv_location_id ON nyc_taxi_dw.location_lookup_mv (location_id);