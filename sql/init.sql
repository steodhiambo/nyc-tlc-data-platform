-- PostgreSQL Schema for NYC TLC Data Warehouse
-- This script creates all necessary tables for the processed taxi data

-- Create extension for UUID generation if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema for taxi data
CREATE SCHEMA IF NOT EXISTS taxi_data;

-- Table for Yellow Taxi Trip Records
CREATE TABLE IF NOT EXISTS taxi_data.yellow_tripdata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    passenger_count INTEGER,
    trip_distance NUMERIC(10,2),
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2),
    year INTEGER,
    month INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_pickup_datetime 
ON taxi_data.yellow_tripdata (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_dropoff_datetime 
ON taxi_data.yellow_tripdata (dropoff_datetime);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_pickup_location_id 
ON taxi_data.yellow_tripdata (pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_dropoff_location_id 
ON taxi_data.yellow_tripdata (dropoff_location_id);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_year_month 
ON taxi_data.yellow_tripdata (year, month);

-- Table for Green Taxi Trip Records
CREATE TABLE IF NOT EXISTS taxi_data.green_tripdata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    store_and_fwd_flag VARCHAR(1),
    rate_code_id INTEGER,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    passenger_count INTEGER,
    trip_distance NUMERIC(10,2),
    fare_amount NUMERIC(10,2),
    extra NUMERIC(10,2),
    mta_tax NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    tolls_amount NUMERIC(10,2),
    improvement_surcharge NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    payment_type INTEGER,
    trip_type INTEGER,
    congestion_surcharge NUMERIC(10,2),
    airport_fee NUMERIC(10,2),
    year INTEGER,
    month INTEGER,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create indexes for green taxi data
CREATE INDEX IF NOT EXISTS idx_green_tripdata_pickup_datetime 
ON taxi_data.green_tripdata (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_green_tripdata_dropoff_datetime 
ON taxi_data.green_tripdata (dropoff_datetime);

CREATE INDEX IF NOT EXISTS idx_green_tripdata_pickup_location_id 
ON taxi_data.green_tripdata (pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_green_tripdata_dropoff_location_id 
ON taxi_data.green_tripdata (dropoff_location_id);

CREATE INDEX IF NOT EXISTS idx_green_tripdata_year_month 
ON taxi_data.green_tripdata (year, month);

-- Table for Taxi Zone lookup data
CREATE TABLE IF NOT EXISTS taxi_data.taxi_zone_lookup (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(32),
    zone VARCHAR(64),
    service_zone VARCHAR(32),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Insert initial taxi zone lookup data
INSERT INTO taxi_data.taxi_zone_lookup (location_id, borough, zone, service_zone) VALUES
(1, 'EWR', 'Newark Airport', 'EWR'),
(2, 'Queens', 'Jamaica Bay', 'Boro Zone'),
(3, 'Bronx', 'Allerton/Pelham Gardens', 'Boro Zone'),
(4, 'Manhattan', 'Alphabet City', 'Yellow Zone'),
(5, 'Staten Island', 'Arden Heights', 'Boro Zone'),
(6, 'Staten Island', 'Arrochar/Fort Wadsworth', 'Boro Zone'),
(7, 'Queens', 'Astoria', 'Boro Zone'),
(8, 'Queens', 'Astoria Park', 'Boro Zone'),
(9, 'Queens', 'Auburndale', 'Boro Zone'),
(10, 'Queens', 'Baisley Park', 'Boro Zone'),
(11, 'Brooklyn', 'Bath Beach', 'Boro Zone'),
(12, 'Manhattan', 'Battery Park', 'Yellow Zone'),
(13, 'Manhattan', 'Battery Park City', 'Yellow Zone'),
(14, 'Brooklyn', 'Bay Ridge', 'Boro Zone'),
(15, 'Queens', 'Bay Terrace/Fort Totten', 'Boro Zone'),
(16, 'Queens', 'Bayside', 'Boro Zone'),
(17, 'Brooklyn', 'Bedford', 'Boro Zone'),
(18, 'Bronx', 'Bedford Park', 'Boro Zone'),
(19, 'Queens', 'Bellerose', 'Boro Zone'),
(20, 'Bronx', 'Belmont', 'Boro Zone'),
(21, 'Brooklyn', 'Bensonhurst East', 'Boro Zone'),
(22, 'Brooklyn', 'Bensonhurst West', 'Boro Zone'),
(23, 'Staten Island', 'Bloomfield/Emerson Hill', 'Boro Zone'),
(24, 'Manhattan', 'Bloomingdale', 'Yellow Zone'),
(25, 'Brooklyn', 'Boerum Hill', 'Boro Zone'),
(26, 'Brooklyn', 'Borough Park', 'Boro Zone'),
(27, 'Queens', 'Breezy Point/Fort Tilden/Riis Beach', 'Boro Zone'),
(28, 'Queens', 'Briarwood/Jamaica Hills', 'Boro Zone'),
(29, 'Brooklyn', 'Brighton Beach', 'Boro Zone'),
(30, 'Queens', 'Broad Channel', 'Boro Zone'),
(31, 'Bronx', 'Bronx Park', 'Boro Zone'),
(32, 'Bronx', 'Bronxdale', 'Boro Zone'),
(33, 'Brooklyn', 'Brooklyn Heights', 'Boro Zone'),
(34, 'Brooklyn', 'Brooklyn Navy Yard', 'Boro Zone'),
(35, 'Brooklyn', 'Brownsville', 'Boro Zone'),
(36, 'Brooklyn', 'Bushwick North', 'Boro Zone'),
(37, 'Brooklyn', 'Bushwick South', 'Boro Zone'),
(38, 'Queens', 'Cambria Heights', 'Boro Zone'),
(39, 'Brooklyn', 'Canarsie', 'Boro Zone'),
(40, 'Brooklyn', 'Carroll Gardens', 'Boro Zone'),
(41, 'Manhattan', 'Central Harlem', 'Boro Zone'),
(42, 'Manhattan', 'Central Harlem North', 'Boro Zone'),
(43, 'Manhattan', 'Central Park', 'Yellow Zone'),
(44, 'Staten Island', 'Charleston/Tottenville', 'Boro Zone'),
(45, 'Manhattan', 'Chinatown', 'Yellow Zone'),
(46, 'Bronx', 'City Island', 'Boro Zone'),
(47, 'Bronx', 'Claremont/Bathgate', 'Boro Zone'),
(48, 'Manhattan', 'Clinton East', 'Yellow Zone'),
(49, 'Brooklyn', 'Clinton Hill', 'Boro Zone'),
(50, 'Manhattan', 'Clinton West', 'Yellow Zone'),
(51, 'Bronx', 'Co-Op City', 'Boro Zone'),
(52, 'Brooklyn', 'Cobble Hill', 'Boro Zone'),
(53, 'Queens', 'College Point', 'Boro Zone'),
(54, 'Brooklyn', 'Columbia Street', 'Boro Zone'),
(55, 'Brooklyn', 'Coney Island', 'Boro Zone'),
(56, 'Queens', 'Corona', 'Boro Zone'),
(57, 'Queens', 'Corona', 'Boro Zone'),
(58, 'Bronx', 'Country Club', 'Boro Zone'),
(59, 'Bronx', 'Crotona Park', 'Boro Zone'),
(60, 'Manhattan', 'Crown Heights', 'Boro Zone'),
(61, 'Brooklyn', 'Crown Heights North', 'Boro Zone'),
(62, 'Brooklyn', 'Crown Heights South', 'Boro Zone'),
(63, 'Manhattan', 'Civic Center', 'Yellow Zone'),
(64, 'Manhattan', 'DUMBO/Vinegar Hill', 'Yellow Zone'),
(65, 'Brooklyn', 'Ditmas Park', 'Boro Zone'),
(66, 'Manhattan', 'Downtown Brooklyn/MetroTech', 'Boro Zone'),
(67, 'Manhattan', 'East Chelsea', 'Yellow Zone'),
(68, 'Bronx', 'East Concourse/Concourse Village', 'Boro Zone'),
(69, 'Queens', 'East Elmhurst', 'Boro Zone'),
(70, 'Brooklyn', 'East Flatbush/Farragut', 'Boro Zone'),
(71, 'Brooklyn', 'East Flatbush/Remsen Village', 'Boro Zone'),
(72, 'Queens', 'East Flushing', 'Boro Zone'),
(73, 'Manhattan', 'East Harlem North', 'Boro Zone'),
(74, 'Manhattan', 'East Harlem South', 'Boro Zone'),
(75, 'Brooklyn', 'East New York', 'Boro Zone'),
(76, 'Brooklyn', 'East New York/Pennsylvania Avenue', 'Boro Zone'),
(77, 'Bronx', 'East Tremont', 'Boro Zone'),
(78, 'Manhattan', 'East Village', 'Yellow Zone'),
(79, 'Bronx', 'Eastchester', 'Boro Zone'),
(80, 'Manhattan', 'Elmhurst', 'Boro Zone'),
(81, 'Queens', 'Elmhurst/Maspeth', 'Boro Zone'),
(82, 'Staten Island', 'Eltingville/Annadale/Prince''s Bay', 'Boro Zone'),
(83, 'Brooklyn', 'Erasmus', 'Boro Zone'),
(84, 'Queens', 'Far Rockaway', 'Boro Zone'),
(85, 'Manhattan', 'Financial District North', 'Yellow Zone'),
(86, 'Manhattan', 'Financial District South', 'Yellow Zone'),
(87, 'Brooklyn', 'Flatbush/Ditmas Park', 'Boro Zone'),
(88, 'Manhattan', 'Flatiron', 'Yellow Zone'),
(89, 'Brooklyn', 'Flatlands', 'Boro Zone'),
(90, 'Manhattan', 'Fordham South', 'Boro Zone'),
(91, 'Queens', 'Forest Hills', 'Boro Zone'),
(92, 'Queens', 'Forest Park/Highland Park', 'Boro Zone'),
(93, 'Bronx', 'Ft. Green/Prospect Heights', 'Boro Zone'),
(94, 'Bronx', 'Ferry Point/Pelham Bay', 'Boro Zone'),
(95, 'Bronx', 'Fieldston/Riverdale', 'Boro Zone'),
(96, 'Manhattan', 'Financial District South', 'Yellow Zone'),
(97, 'Queens', 'Flushing', 'Boro Zone'),
(98, 'Queens', 'Flushing Meadows-Corona Park', 'Boro Zone'),
(99, 'Manhattan', 'Gramercy', 'Yellow Zone'),
(100, 'Brooklyn', 'Gravesend', 'Boro Zone'),
(101, 'Staten Island', 'Great Kills', 'Boro Zone'),
(102, 'Staten Island', 'Great Kills Park', 'Boro Zone'),
(103, 'Brooklyn', 'Greenpoint', 'Boro Zone'),
(104, 'Manhattan', 'Greenwich Village North', 'Yellow Zone'),
(105, 'Manhattan', 'Greenwich Village South', 'Yellow Zone'),
(106, 'Staten Island', 'Grymes Hill/Clifton', 'Boro Zone'),
(107, 'Manhattan', 'Hamilton Heights', 'Boro Zone'),
(108, 'Queens', 'Hammels/Arverne', 'Boro Zone'),
(109, 'Staten Island', 'Heartland Village/Todt Hill', 'Boro Zone'),
(110, 'Queens', 'Hillcrest/Pomonok', 'Boro Zone'),
(111, 'Queens', 'Hollis', 'Boro Zone'),
(112, 'Brooklyn', 'Homecrest', 'Boro Zone'),
(113, 'Queens', 'Howard Beach', 'Boro Zone'),
(114, 'Manhattan', 'Hudson Sq', 'Yellow Zone'),
(115, 'Bronx', 'Hunts Point', 'Boro Zone'),
(116, 'Manhattan', 'Inwood', 'Boro Zone'),
(117, 'Manhattan', 'Inwood Hill Park', 'Boro Zone'),
(118, 'Queens', 'Jackson Heights', 'Boro Zone'),
(119, 'Queens', 'Jamaica', 'Boro Zone'),
(120, 'Queens', 'Jamaica Estates', 'Boro Zone'),
(121, 'Queens', 'JFK Airport', 'Airports'),
(122, 'Brooklyn', 'Kensington', 'Boro Zone'),
(123, 'Queens', 'Kew Gardens', 'Boro Zone'),
(124, 'Queens', 'Kew Gardens Hills', 'Boro Zone'),
(125, 'Bronx', 'Kingsbridge Heights', 'Boro Zone'),
(126, 'Brooklyn', 'Knapp Street', 'Boro Zone'),
(127, 'Brooklyn', 'LaGuardia Airport', 'Airports'),
(128, 'Queens', 'Laurelton', 'Boro Zone'),
(129, 'Manhattan', 'Lenox Hill East', 'Yellow Zone'),
(130, 'Manhattan', 'Lenox Hill West', 'Yellow Zone'),
(131, 'Manhattan', 'Lincoln Square East', 'Yellow Zone'),
(132, 'Manhattan', 'Lincoln Square West', 'Yellow Zone'),
(133, 'Manhattan', 'Little Italy/NoLiTa', 'Yellow Zone'),
(134, 'Queens', 'Long Island City/Hunters Point', 'Boro Zone'),
(135, 'Queens', 'Long Island City/Queens Plaza', 'Boro Zone'),
(136, 'Brooklyn', 'Longwood', 'Boro Zone'),
(137, 'Manhattan', 'Lower East Side', 'Yellow Zone'),
(138, 'Brooklyn', 'Madison', 'Boro Zone'),
(139, 'Brooklyn', 'Manhattan Beach', 'Boro Zone'),
(140, 'Manhattan', 'Manhattan Valley', 'Yellow Zone'),
(141, 'Manhattan', 'Manhattanville', 'Boro Zone'),
(142, 'Manhattan', 'Marble Hill', 'Boro Zone'),
(143, 'Brooklyn', 'Marine Park/Floyd Bennett Field', 'Boro Zone'),
(144, 'Brooklyn', 'Marine Park/Mill Basin', 'Boro Zone'),
(145, 'Staten Island', 'Mariners Harbor', 'Boro Zone'),
(146, 'Queens', 'Maspeth', 'Boro Zone'),
(147, 'Manhattan', 'Meatpacking/West Village West', 'Yellow Zone'),
(148, 'Bronx', 'Melrose South', 'Boro Zone'),
(149, 'Queens', 'Middle Village', 'Boro Zone'),
(150, 'Manhattan', 'Midtown Center', 'Yellow Zone'),
(151, 'Manhattan', 'Midtown East', 'Yellow Zone'),
(152, 'Manhattan', 'Midtown North', 'Yellow Zone'),
(153, 'Manhattan', 'Midtown South', 'Yellow Zone'),
(154, 'Brooklyn', 'Midwood', 'Boro Zone'),
(155, 'Manhattan', 'Morningside Heights', 'Boro Zone'),
(156, 'Bronx', 'Morrisania/Melrose', 'Boro Zone'),
(157, 'Bronx', 'Mott Haven/Port Morris', 'Boro Zone'),
(158, 'Bronx', 'Mount Hope', 'Boro Zone'),
(159, 'Manhattan', 'Murray Hill', 'Yellow Zone'),
(160, 'Queens', 'Murray Hill-Queens', 'Boro Zone'),
(161, 'Staten Island', 'New Dorp/Midland Beach', 'Boro Zone'),
(162, 'Queens', 'North Corona', 'Boro Zone'),
(163, 'Bronx', 'Norwood', 'Boro Zone'),
(164, 'Manhattan', 'Noho', 'Yellow Zone'),
(165, 'Queens', 'Oakland Gardens', 'Boro Zone'),
(166, 'Staten Island', 'Oakwood', 'Boro Zone'),
(167, 'Brooklyn', 'Ocean Hill', 'Boro Zone'),
(168, 'Brooklyn', 'Ocean Parkway South', 'Boro Zone'),
(169, 'Queens', 'Old Astoria', 'Boro Zone'),
(170, 'Queens', 'Ozone Park', 'Boro Zone'),
(171, 'Brooklyn', 'Park Slope', 'Boro Zone'),
(172, 'Bronx', 'Parkchester', 'Boro Zone'),
(173, 'Bronx', 'Pelham Bay', 'Boro Zone'),
(174, 'Bronx', 'Pelham Bay Park', 'Boro Zone'),
(175, 'Bronx', 'Pelham Parkway', 'Boro Zone'),
(176, 'Manhattan', 'Penn Station/Madison Sq West', 'Yellow Zone'),
(177, 'Staten Island', 'Port Richmond', 'Boro Zone'),
(178, 'Brooklyn', 'Prospect-Lefferts Gardens', 'Boro Zone'),
(179, 'Brooklyn', 'Prospect Heights', 'Boro Zone'),
(180, 'Brooklyn', 'Prospect Park', 'Boro Zone'),
(181, 'Queens', 'Queens Village', 'Boro Zone'),
(182, 'Queens', 'Queensboro Hill', 'Boro Zone'),
(183, 'Queens', 'Queensbridge/Ravenswood', 'Boro Zone'),
(184, 'Manhattan', 'Randalls Island', 'Yellow Zone'),
(185, 'Brooklyn', 'Red Hook', 'Boro Zone'),
(186, 'Queens', 'Rego Park', 'Boro Zone'),
(187, 'Queens', 'Richmond Hill', 'Boro Zone'),
(188, 'Queens', 'Ridgewood', 'Boro Zone'),
(189, 'Bronx', 'Rikers Island', 'Boro Zone'),
(190, 'Queens', 'Rosedale', 'Boro Zone'),
(191, 'Staten Island', 'Rossville/Woodrow', 'Boro Zone'),
(192, 'Manhattan', 'Roosevelt Island', 'Boro Zone'),
(193, 'Queens', 'Rosedale', 'Boro Zone'),
(194, 'Brooklyn', 'Stuyvesant Heights', 'Boro Zone'),
(195, 'Queens', 'Saint Albans', 'Boro Zone'),
(196, 'Staten Island', 'Saint George/New Brighton', 'Boro Zone'),
(197, 'Queens', 'Saint Michaels Cemetery/Woodside', 'Boro Zone'),
(198, 'Bronx', 'Schuylerville/Edgewater Park', 'Boro Zone'),
(199, 'Manhattan', 'South Chelsea', 'Yellow Zone'),
(200, 'Queens', 'Kew Gardens', 'Boro Zone'),
(201, 'Manhattan', 'Springfield Gardens North', 'Boro Zone'),
(202, 'Queens', 'Springfield Gardens South', 'Boro Zone'),
(203, 'Bronx', 'Spuyten Duyvil/Kingsbridge', 'Boro Zone'),
(204, 'Staten Island', 'Stapleton', 'Boro Zone'),
(205, 'Brooklyn', 'Starrett City', 'Boro Zone'),
(206, 'Manhattan', 'Stuy Town/Peter Cooper Village', 'Yellow Zone'),
(207, 'Manhattan', 'Sutton Place/Turtle Bay North', 'Yellow Zone'),
(208, 'Manhattan', 'Times Sq/Theatre District', 'Yellow Zone'),
(209, 'Manhattan', 'TriBeCa/Civic Center', 'Yellow Zone'),
(210, 'Manhattan', 'Two Bridges/Seward Park', 'Yellow Zone'),
(211, 'Manhattan', 'UN/Turtle Bay South', 'Yellow Zone'),
(212, 'Manhattan', 'Union Sq', 'Yellow Zone'),
(213, 'Bronx', 'University Heights/Morris Heights', 'Boro Zone'),
(214, 'Manhattan', 'Upper East Side North', 'Yellow Zone'),
(215, 'Manhattan', 'Upper East Side South', 'Yellow Zone'),
(216, 'Manhattan', 'Upper West Side North', 'Yellow Zone'),
(217, 'Manhattan', 'Upper West Side South', 'Yellow Zone'),
(218, 'Bronx', 'Van Cortlandt Park', 'Boro Zone'),
(219, 'Bronx', 'Van Cortlandt Village', 'Boro Zone'),
(220, 'Bronx', 'Van Nest/Morris Park', 'Boro Zone'),
(221, 'Manhattan', 'Washington Heights North', 'Boro Zone'),
(222, 'Manhattan', 'Washington Heights South', 'Boro Zone'),
(223, 'Staten Island', 'West Brighton', 'Boro Zone'),
(224, 'Manhattan', 'West Chelsea/Hudson Yards', 'Yellow Zone'),
(225, 'Bronx', 'West Concourse', 'Boro Zone'),
(226, 'Bronx', 'West Farms/Bronx River', 'Boro Zone'),
(227, 'Manhattan', 'West Village', 'Yellow Zone'),
(228, 'Bronx', 'Westchester Village/Unionport', 'Boro Zone'),
(229, 'Staten Island', 'Westerleigh', 'Boro Zone'),
(230, 'Queens', 'Whitestone', 'Boro Zone'),
(231, 'Queens', 'Willets Point', 'Boro Zone'),
(232, 'Brooklyn', 'Williamsburg (North Side)', 'Boro Zone'),
(233, 'Brooklyn', 'Williamsburg (South Side)', 'Boro Zone'),
(234, 'Brooklyn', 'Windsor Terrace', 'Boro Zone'),
(235, 'Queens', 'Woodhaven', 'Boro Zone'),
(236, 'Bronx', 'Woodlawn/Wakefield', 'Boro Zone'),
(237, 'Queens', 'Woodside', 'Boro Zone'),
(238, 'Manhattan', 'World Trade Center', 'Yellow Zone'),
(239, 'Manhattan', 'Yorkville East', 'Yellow Zone'),
(240, 'Manhattan', 'Yorkville West', 'Yellow Zone'),
(241, 'Unknown', 'NV', 'N/A'),
(242, 'Unknown', 'NA', 'N/A'),
(243, 'Queens', 'Airport', 'Airports'),
(244, 'Queens', 'Seaport', 'Yellow Zone'),
(245, 'Manhattan', 'Sheepshead Bay', 'Boro Zone'),
(246, 'Queens', 'Upper East Side South', 'Yellow Zone'),
(247, 'Manhattan', 'Upper West Side South', 'Yellow Zone'),
(248, 'Queens', 'Midtown', 'Yellow Zone'),
(249, 'Queens', 'Midtown South', 'Yellow Zone'),
(250, 'Manhattan', 'Midtown North', 'Yellow Zone');

-- Table for data quality metrics and monitoring
CREATE TABLE IF NOT EXISTS taxi_data.data_quality_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(64),
    metric_type VARCHAR(32),
    metric_name VARCHAR(64),
    metric_value NUMERIC(15,2),
    date DATE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Table for pipeline run logs
CREATE TABLE IF NOT EXISTS taxi_data.pipeline_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(128),
    run_id VARCHAR(128),
    status VARCHAR(32),
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    records_processed INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create indexes for pipeline logs
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_pipeline_name 
ON taxi_data.pipeline_logs (pipeline_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id 
ON taxi_data.pipeline_logs (run_id);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_status 
ON taxi_data.pipeline_logs (status);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_created_at 
ON taxi_data.pipeline_logs (created_at);

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update the updated_at column
CREATE TRIGGER update_taxi_yellow_tripdata_updated_at 
    BEFORE UPDATE ON taxi_data.yellow_tripdata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxi_green_tripdata_updated_at 
    BEFORE UPDATE ON taxi_data.green_tripdata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_taxi_zone_lookup_updated_at 
    BEFORE UPDATE ON taxi_data.taxi_zone_lookup 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to the application user
GRANT ALL PRIVILEGES ON SCHEMA taxi_data TO nyc_tlc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA taxi_data TO nyc_tlc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA taxi_data TO nyc_tlc_user;

-- Insert sample data for testing
INSERT INTO taxi_data.yellow_tripdata (
    vendor_id, pickup_datetime, dropoff_datetime, passenger_count, 
    trip_distance, rate_code_id, store_and_fwd_flag, pickup_location_id, 
    dropoff_location_id, payment_type, fare_amount, extra, mta_tax, 
    tip_amount, tolls_amount, improvement_surcharge, total_amount, 
    congestion_surcharge, airport_fee, year, month
) VALUES 
(1, '2023-01-01 00:00:01', '2023-01-01 00:15:01', 1, 2.3, 1, 'N', 234, 140, 1, 10.5, 0.5, 0.5, 2.0, 0.0, 0.3, 14.8, 2.5, 0.0, 2023, 1),
(2, '2023-01-01 00:30:00', '2023-01-01 00:45:00', 2, 1.8, 1, 'N', 161, 162, 2, 8.0, 0.5, 0.5, 0.0, 0.0, 0.3, 11.3, 2.5, 0.0, 2023, 1);

-- Insert sample green taxi data
INSERT INTO taxi_data.green_tripdata (
    vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd_flag, 
    rate_code_id, pickup_location_id, dropoff_location_id, passenger_count, 
    trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
    improvement_surcharge, total_amount, payment_type, trip_type, 
    congestion_surcharge, airport_fee, year, month
) VALUES 
(1, '2023-01-01 00:05:00', '2023-01-01 00:20:00', 'N', 1, 234, 140, 1, 2.1, 9.5, 0.5, 0.5, 1.5, 0.0, 0.3, 13.8, 1, 1, 2.5, 0.0, 2023, 1);

-- Insert sample pipeline log
INSERT INTO taxi_data.pipeline_logs (
    pipeline_name, run_id, status, start_time, end_time, 
    records_processed, records_failed
) VALUES 
('yellow_tripdata_etl', 'run_20230101_000000', 'SUCCESS', 
 '2023-01-01 00:00:00', '2023-01-01 00:05:00', 10000, 0);

-- Insert sample data quality metrics
INSERT INTO taxi_data.data_quality_metrics (
    table_name, metric_type, metric_name, metric_value, date
) VALUES 
('yellow_tripdata', 'COUNT', 'total_records', 10000, '2023-01-01'),
('yellow_tripdata', 'VALIDATION', 'null_pickup_datetime_pct', 0.0, '2023-01-01'),
('yellow_tripdata', 'VALIDATION', 'null_dropoff_datetime_pct', 0.0, '2023-01-01'),
('yellow_tripdata', 'VALIDATION', 'negative_fare_pct', 0.0, '2023-01-01');