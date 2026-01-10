"""
ClickHouse Integration for NYC TLC Data Platform

This module provides integration with ClickHouse for columnar storage
and analytical queries.
"""

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import Dict, List, Any, Optional, Union
import pandas as pd
import logging
from datetime import datetime, timedelta
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """ClickHouse client for the NYC TLC Data Platform"""
    
    def __init__(
        self, 
        host: str = None, 
        port: int = None, 
        username: str = None, 
        password: str = None, 
        database: str = None
    ):
        """
        Initialize ClickHouse client
        
        Args:
            host: ClickHouse host (defaults to CLICKHOUSE_HOST env var or localhost)
            port: ClickHouse port (defaults to CLICKHOUSE_PORT env var or 8123)
            username: ClickHouse username (defaults to CLICKHOUSE_USERNAME env var)
            password: ClickHouse password (defaults to CLICKHOUSE_PASSWORD env var)
            database: ClickHouse database (defaults to CLICKHOUSE_DATABASE env var or 'default')
        """
        self.host = host or os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = port or int(os.getenv('CLICKHOUSE_PORT', 8123))
        self.username = username or os.getenv('CLICKHOUSE_USERNAME', 'default')
        self.password = password or os.getenv('CLICKHOUSE_PASSWORD', '')
        self.database = database or os.getenv('CLICKHOUSE_DATABASE', 'default')
        
        # Connect to ClickHouse
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Execute a SELECT query and return results as list of dictionaries
        
        Args:
            query: SQL query to execute
            params: Query parameters (for parameterized queries)
            
        Returns:
            Query results as list of dictionaries
        """
        try:
            result = self.client.query(query, parameters=params)
            # Convert to list of dictionaries
            return [dict(zip(result.column_names, row)) for row in result.result_rows]
        except Exception as e:
            logger.error(f"Error executing query: {query[:100]}... Error: {e}")
            raise
    
    def execute_command(self, command: str, params: Dict = None) -> bool:
        """
        Execute a non-SELECT command (INSERT, UPDATE, CREATE, etc.)
        
        Args:
            command: SQL command to execute
            params: Command parameters (for parameterized commands)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.command(command, parameters=params)
            logger.debug(f"Executed command: {command[:100]}...")
            return True
        except Exception as e:
            logger.error(f"Error executing command: {command[:100]}... Error: {e}")
            return False
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame, database: str = None) -> bool:
        """
        Insert a pandas DataFrame into a ClickHouse table
        
        Args:
            table_name: Name of the target table
            df: DataFrame to insert
            database: Database name (uses default if not specified)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert DataFrame to list of tuples for insertion
            data = [tuple(row) for row in df.values]
            column_names = list(df.columns)
            
            # Insert data
            self.client.insert(table_name, data, column_names, database=database)
            logger.info(f"Inserted {len(df)} rows into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error inserting DataFrame into {table_name}: {e}")
            return False
    
    def create_table_from_dataframe(self, table_name: str, df: pd.DataFrame, engine: str = 'MergeTree') -> bool:
        """
        Create a ClickHouse table based on a DataFrame's schema
        
        Args:
            table_name: Name of the table to create
            df: DataFrame to infer schema from
            engine: ClickHouse engine to use (default: MergeTree)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Map pandas dtypes to ClickHouse types
            dtype_mapping = {
                'object': 'String',
                'int64': 'Int64',
                'int32': 'Int32',
                'float64': 'Float64',
                'float32': 'Float32',
                'bool': 'UInt8',
                'datetime64[ns]': 'DateTime'
            }
            
            # Generate column definitions
            columns = []
            for col_name, dtype in df.dtypes.items():
                ch_type = dtype_mapping.get(str(dtype), 'String')
                columns.append(f"`{col_name}` {ch_type}")
            
            # Create table
            columns_def = ', '.join(columns)
            create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def}) ENGINE = {engine}"
            
            if engine == 'MergeTree':
                # Add ORDER BY clause for MergeTree
                # Use the first column as the sorting key (adjust as needed)
                first_col = df.columns[0]
                create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def}) ENGINE = MergeTree ORDER BY `{first_col}`"
            
            success = self.execute_command(create_query)
            if success:
                logger.info(f"Created table {table_name} with schema based on DataFrame")
            return success
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str, database: str = None) -> bool:
        """
        Check if a table exists in ClickHouse
        
        Args:
            table_name: Name of the table to check
            database: Database name (uses default if not specified)
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            db_clause = f"'{database}'." if database else f"'{self.database}'."
            query = f"SELECT name FROM system.tables WHERE name = '{table_name}' AND database = {db_clause[:-1]}"
            result = self.execute_query(query)
            return len(result) > 0
        except Exception as e:
            logger.error(f"Error checking if table exists {table_name}: {e}")
            return False
    
    def get_table_schema(self, table_name: str, database: str = None) -> List[Dict]:
        """
        Get the schema of a ClickHouse table
        
        Args:
            table_name: Name of the table
            database: Database name (uses default if not specified)
            
        Returns:
            List of column information dictionaries
        """
        try:
            db_clause = f"'{database}'." if database else f"'{self.database}'."
            query = f"SELECT name, type, default_kind, default_expression FROM system.columns WHERE table = '{table_name}' AND database = {db_clause[:-1]}"
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {e}")
            return []
    
    def get_table_size(self, table_name: str, database: str = None) -> Dict:
        """
        Get size information for a ClickHouse table
        
        Args:
            table_name: Name of the table
            database: Database name (uses default if not specified)
            
        Returns:
            Dictionary with size information
        """
        try:
            db_clause = f"`{database}`." if database else f"`{self.database}`."
            query = f"""
            SELECT 
                count() AS row_count,
                sum(bytes_on_disk) AS bytes_on_disk,
                sum(data_uncompressed_bytes) AS data_uncompressed_bytes
            FROM system.parts 
            WHERE table = '{table_name}' AND database = {db_clause[:-1]}
            """
            result = self.execute_query(query)
            if result:
                return result[0]
            else:
                return {'row_count': 0, 'bytes_on_disk': 0, 'data_uncompressed_bytes': 0}
        except Exception as e:
            logger.error(f"Error getting size for table {table_name}: {e}")
            return {'row_count': 0, 'bytes_on_disk': 0, 'data_uncompressed_bytes': 0}


class ClickHouseDataWarehouse:
    """Data warehouse implementation using ClickHouse"""
    
    def __init__(self, ch_client: ClickHouseClient = None):
        self.ch_client = ch_client or ClickHouseClient()
    
    def setup_nyc_tlc_schema(self) -> bool:
        """Set up the NYC TLC data schema in ClickHouse"""
        try:
            # Create trips table
            trips_table_sql = """
            CREATE TABLE IF NOT EXISTS nyc_tlc_trips (
                vendor_id UInt8,
                pickup_datetime DateTime,
                dropoff_datetime DateTime,
                passenger_count UInt8,
                trip_distance Float32,
                pickup_longitude Float64,
                pickup_latitude Float64,
                rate_code_id UInt8,
                store_and_fwd_flag String,
                dropoff_longitude Float64,
                dropoff_latitude Float64,
                payment_type UInt8,
                fare_amount Float32,
                extra Float32,
                mta_tax Float32,
                tip_amount Float32,
                tolls_amount Float32,
                improvement_surcharge Float32,
                total_amount Float32,
                congestion_surcharge Float32,
                pickup_location_id UInt16,
                dropoff_location_id UInt16
            ) ENGINE = MergeTree
            ORDER BY (pickup_datetime, pickup_location_id)
            PARTITION BY toYYYYMM(pickup_datetime)
            """
            
            success = self.ch_client.execute_command(trips_table_sql)
            if not success:
                return False
            
            # Create lookup table for taxi zones
            zones_table_sql = """
            CREATE TABLE IF NOT EXISTS taxi_zones (
                location_id UInt16,
                borough String,
                zone String,
                service_zone String
            ) ENGINE = MergeTree
            ORDER BY location_id
            """
            
            success = self.ch_client.execute_command(zones_table_sql)
            if not success:
                return False
            
            logger.info("NYC TLC schema created successfully in ClickHouse")
            return True
        except Exception as e:
            logger.error(f"Error setting up NYC TLC schema: {e}")
            return False
    
    def load_trip_data(self, df: pd.DataFrame, table_name: str = 'nyc_tlc_trips') -> bool:
        """
        Load trip data into ClickHouse
        
        Args:
            df: DataFrame containing trip data
            table_name: Target table name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure the table exists
            if not self.ch_client.table_exists(table_name):
                self.ch_client.create_table_from_dataframe(table_name, df)
            
            # Insert the data
            success = self.ch_client.insert_dataframe(table_name, df)
            if success:
                logger.info(f"Loaded {len(df)} trip records into {table_name}")
            return success
        except Exception as e:
            logger.error(f"Error loading trip data: {e}")
            return False
    
    def run_analytical_query(self, query: str) -> pd.DataFrame:
        """
        Run an analytical query and return results as a DataFrame
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query results as a DataFrame
        """
        try:
            results = self.ch_client.execute_query(query)
            if results:
                return pd.DataFrame(results)
            else:
                return pd.DataFrame()  # Return empty DataFrame if no results
        except Exception as e:
            logger.error(f"Error running analytical query: {query[:100]}... Error: {e}")
            return pd.DataFrame()
    
    def get_monthly_aggregates(self, year: int, month: int) -> pd.DataFrame:
        """
        Get monthly aggregate statistics for NYC TLC data
        
        Args:
            year: Year to aggregate
            month: Month to aggregate
            
        Returns:
            DataFrame with monthly aggregates
        """
        query = f"""
        SELECT 
            vendor_id,
            count(*) as trip_count,
            avg(fare_amount) as avg_fare,
            avg(trip_distance) as avg_distance,
            avg(passenger_count) as avg_passenger_count,
            sum(total_amount) as total_revenue,
            min(pickup_datetime) as first_trip,
            max(dropoff_datetime) as last_trip
        FROM nyc_tlc_trips 
        WHERE toYear(pickup_datetime) = {year} 
            AND toMonth(pickup_datetime) = {month}
        GROUP BY vendor_id
        ORDER BY vendor_id
        """
        
        return self.run_analytical_query(query)
    
    def get_top_pickup_zones(self, limit: int = 10) -> pd.DataFrame:
        """
        Get top pickup zones by trip count
        
        Args:
            limit: Number of top zones to return
            
        Returns:
            DataFrame with top pickup zones
        """
        query = f"""
        SELECT 
            tz.zone as pickup_zone,
            tz.borough as pickup_borough,
            count(*) as trip_count
        FROM nyc_tlc_trips t
        JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        GROUP BY tz.zone, tz.borough
        ORDER BY trip_count DESC
        LIMIT {limit}
        """
        
        return self.run_analytical_query(query)
    
    def get_peak_hours(self, year: int, month: int) -> pd.DataFrame:
        """
        Get peak hours for trips in a given month
        
        Args:
            year: Year to analyze
            month: Month to analyze
            
        Returns:
            DataFrame with hourly trip counts
        """
        query = f"""
        SELECT 
            toHour(pickup_datetime) as hour_of_day,
            count(*) as trip_count
        FROM nyc_tlc_trips
        WHERE toYear(pickup_datetime) = {year}
            AND toMonth(pickup_datetime) = {month}
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """
        
        return self.run_analytical_query(query)


def create_clickhouse_connection_from_env():
    """Create a ClickHouse connection using environment variables"""
    return ClickHouseClient()


def migrate_data_to_clickhouse(source_client, target_client: ClickHouseClient, table_name: str, 
                              source_query: str = None) -> bool:
    """
    Migrate data from a source database to ClickHouse
    
    Args:
        source_client: Source database client (with execute_query method)
        target_client: ClickHouse client
        table_name: Target table name in ClickHouse
        source_query: Query to get data from source (if None, uses SELECT * FROM table_name)
        
    Returns:
        True if migration successful, False otherwise
    """
    try:
        # Get data from source
        if source_query is None:
            source_query = f"SELECT * FROM {table_name} LIMIT 10000"  # Limit for initial migration
        
        source_data = source_client.execute_query(source_query)
        if not source_data:
            logger.info("No data to migrate")
            return True
        
        # Convert to DataFrame
        df = pd.DataFrame(source_data)
        
        # Create table in ClickHouse if it doesn't exist
        if not target_client.table_exists(table_name):
            target_client.create_table_from_dataframe(table_name, df)
        
        # Insert data
        success = target_client.insert_dataframe(table_name, df)
        if success:
            logger.info(f"Migrated {len(df)} rows to ClickHouse table {table_name}")
        
        return success
    except Exception as e:
        logger.error(f"Error migrating data to ClickHouse: {e}")
        return False