"""
Data Governance Components for NYC TLC Data Platform

This module implements data governance features including:
- Data lineage tracking
- Metadata management
- IAM role definitions
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import os


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DataAsset:
    """Represents a data asset in the platform"""
    name: str
    type: str  # table, view, file, etc.
    location: str
    owner: str
    created_at: datetime
    updated_at: datetime
    description: str = ""
    tags: List[str] = None
    lineage: List[str] = None  # List of upstream assets


@dataclass
class DataLineage:
    """Represents data lineage between assets"""
    source_asset: str
    target_asset: str
    transformation: str
    created_at: datetime
    updated_at: datetime
    description: str = ""


@dataclass
class Metadata:
    """Metadata for a data asset"""
    asset_name: str
    schema: Dict
    sample_data: List[Dict] = None
    data_quality_metrics: Dict = None
    access_logs: List[Dict] = None
    created_at: datetime = None
    updated_at: datetime = None


class DataGovernanceManager:
    """Manages data governance components"""
    
    def __init__(self, metadata_db_path: str = "/home/steodhiambo/nyc-tlc-data-platform/metadata/"):
        self.metadata_db_path = metadata_db_path
        self.assets_file = os.path.join(metadata_db_path, "data_assets.json")
        self.lineage_file = os.path.join(metadata_db_path, "data_lineage.json")
        self.metadata_file = os.path.join(metadata_db_path, "metadata.json")
        
        # Create metadata directory if it doesn't exist
        os.makedirs(metadata_db_path, exist_ok=True)
        
        # Initialize metadata files if they don't exist
        self._init_metadata_files()
    
    def _init_metadata_files(self):
        """Initialize metadata files if they don't exist"""
        for file_path in [self.assets_file, self.lineage_file, self.metadata_file]:
            if not os.path.exists(file_path):
                with open(file_path, 'w') as f:
                    json.dump([], f)
    
    def register_data_asset(self, asset: DataAsset) -> bool:
        """Register a new data asset"""
        try:
            with open(self.assets_file, 'r') as f:
                assets = json.load(f)
            
            # Convert asset to dict and add to list
            asset_dict = asdict(asset)
            asset_dict['created_at'] = asset_dict['created_at'].isoformat()
            asset_dict['updated_at'] = asset_dict['updated_at'].isoformat()
            
            # Check if asset already exists
            for existing_asset in assets:
                if existing_asset['name'] == asset.name:
                    logger.warning(f"Asset {asset.name} already exists, updating...")
                    existing_asset.update(asset_dict)
                    break
            else:
                assets.append(asset_dict)
            
            # Write back to file
            with open(self.assets_file, 'w') as f:
                json.dump(assets, f, indent=2)
            
            logger.info(f"Registered data asset: {asset.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error registering data asset {asset.name}: {e}")
            return False
    
    def get_data_asset(self, asset_name: str) -> Optional[DataAsset]:
        """Get a data asset by name"""
        try:
            with open(self.assets_file, 'r') as f:
                assets = json.load(f)
            
            for asset_dict in assets:
                if asset_dict['name'] == asset_name:
                    # Convert back to DataAsset
                    asset_dict['created_at'] = datetime.fromisoformat(asset_dict['created_at'])
                    asset_dict['updated_at'] = datetime.fromisoformat(asset_dict['updated_at'])
                    return DataAsset(**asset_dict)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting data asset {asset_name}: {e}")
            return None
    
    def create_lineage(self, lineage: DataLineage) -> bool:
        """Create a data lineage relationship"""
        try:
            with open(self.lineage_file, 'r') as f:
                lineages = json.load(f)
            
            # Convert lineage to dict and add to list
            lineage_dict = asdict(lineage)
            lineage_dict['created_at'] = lineage_dict['created_at'].isoformat()
            lineage_dict['updated_at'] = lineage_dict['updated_at'].isoformat()
            
            # Check if lineage already exists
            for existing_lineage in lineages:
                if (existing_lineage['source_asset'] == lineage.source_asset and
                    existing_lineage['target_asset'] == lineage.target_asset):
                    logger.warning(f"Lineage from {lineage.source_asset} to {lineage.target_asset} already exists, updating...")
                    existing_lineage.update(lineage_dict)
                    break
            else:
                lineages.append(lineage_dict)
            
            # Write back to file
            with open(self.lineage_file, 'w') as f:
                json.dump(lineages, f, indent=2)
            
            logger.info(f"Created lineage from {lineage.source_asset} to {lineage.target_asset}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating lineage: {e}")
            return False
    
    def get_lineage(self, asset_name: str) -> Dict:
        """Get lineage information for an asset"""
        try:
            with open(self.lineage_file, 'r') as f:
                lineages = json.load(f)
            
            upstream = []
            downstream = []
            
            for lineage_dict in lineages:
                if lineage_dict['target_asset'] == asset_name:
                    upstream.append(lineage_dict['source_asset'])
                elif lineage_dict['source_asset'] == asset_name:
                    downstream.append(lineage_dict['target_asset'])
            
            return {
                'asset': asset_name,
                'upstream': upstream,
                'downstream': downstream
            }
            
        except Exception as e:
            logger.error(f"Error getting lineage for {asset_name}: {e}")
            return {'asset': asset_name, 'upstream': [], 'downstream': []}
    
    def store_metadata(self, metadata: Metadata) -> bool:
        """Store metadata for a data asset"""
        try:
            with open(self.metadata_file, 'r') as f:
                metadata_list = json.load(f)
            
            # Convert metadata to dict
            metadata_dict = asdict(metadata)
            if metadata_dict['created_at']:
                metadata_dict['created_at'] = metadata_dict['created_at'].isoformat()
            if metadata_dict['updated_at']:
                metadata_dict['updated_at'] = metadata_dict['updated_at'].isoformat()
            
            # Check if metadata already exists
            for existing_metadata in metadata_list:
                if existing_metadata['asset_name'] == metadata.asset_name:
                    logger.warning(f"Metadata for {metadata.asset_name} already exists, updating...")
                    existing_metadata.update(metadata_dict)
                    break
            else:
                metadata_list.append(metadata_dict)
            
            # Write back to file
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata_list, f, indent=2)
            
            logger.info(f"Stored metadata for asset: {metadata.asset_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing metadata for {metadata.asset_name}: {e}")
            return False
    
    def get_metadata(self, asset_name: str) -> Optional[Metadata]:
        """Get metadata for a data asset"""
        try:
            with open(self.metadata_file, 'r') as f:
                metadata_list = json.load(f)
            
            for metadata_dict in metadata_list:
                if metadata_dict['asset_name'] == asset_name:
                    # Convert back to Metadata
                    if metadata_dict['created_at']:
                        metadata_dict['created_at'] = datetime.fromisoformat(metadata_dict['created_at'])
                    if metadata_dict['updated_at']:
                        metadata_dict['updated_at'] = datetime.fromisoformat(metadata_dict['updated_at'])
                    return Metadata(**metadata_dict)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting metadata for {asset_name}: {e}")
            return None


def create_iam_roles():
    """
    Define IAM roles and policies for data access
    """
    roles = {
        "data_engineer": {
            "description": "Full access to data processing and transformation",
            "permissions": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "redshift:Get*", 
                "redshift:Create*",
                "redshift:Update*",
                "airflow:Get*",
                "airflow:Update*",
                "airflow:Create*"
            ]
        },
        "data_analyst": {
            "description": "Read-only access to processed data",
            "permissions": [
                "s3:GetObject",
                "s3:ListBucket",
                "glue:GetTable",
                "glue:GetDatabase",
                "redshift:Select*",
                "redshift:Describe*"
            ]
        },
        "data_scientist": {
            "description": "Read access to data with ability to run transformations",
            "permissions": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "glue:GetTable",
                "glue:GetDatabase",
                "redshift:Select*",
                "redshift:Create*",
                "redshift:Update*"
            ]
        },
        "data_admin": {
            "description": "Full administrative access to all data resources",
            "permissions": [
                "*"
            ]
        }
    }
    
    # Write IAM roles to file
    with open("/home/steodhiambo/nyc-tlc-data-platform/iam_roles.json", "w") as f:
        json.dump(roles, f, indent=2)
    
    logger.info("IAM roles created successfully")
    return roles


def setup_metadata_tables_in_db():
    """
    SQL script to create metadata tracking tables in the database
    """
    sql_script = """
    -- Create schema for metadata tracking
    CREATE SCHEMA IF NOT EXISTS data_governance;

    -- Table to track data assets
    CREATE TABLE IF NOT EXISTS data_governance.data_assets (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        type VARCHAR(100) NOT NULL,
        location VARCHAR(500),
        owner VARCHAR(255),
        description TEXT,
        tags JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Table to track data lineage
    CREATE TABLE IF NOT EXISTS data_governance.data_lineage (
        id SERIAL PRIMARY KEY,
        source_asset VARCHAR(255) REFERENCES data_governance.data_assets(name),
        target_asset VARCHAR(255) REFERENCES data_governance.data_assets(name),
        transformation TEXT,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Table to track data quality metrics
    CREATE TABLE IF NOT EXISTS data_governance.data_quality_metrics (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        metric_type VARCHAR(100) NOT NULL,
        metric_name VARCHAR(255) NOT NULL,
        metric_value NUMERIC,
        date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Table to track pipeline execution logs
    CREATE TABLE IF NOT EXISTS data_governance.pipeline_logs (
        id SERIAL PRIMARY KEY,
        pipeline_name VARCHAR(255) NOT NULL,
        run_id VARCHAR(255) NOT NULL,
        status VARCHAR(50) NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        records_processed INTEGER DEFAULT 0,
        records_failed INTEGER DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Table to track data access logs
    CREATE TABLE IF NOT EXISTS data_governance.access_logs (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(255),
        asset_name VARCHAR(255) REFERENCES data_governance.data_assets(name),
        access_type VARCHAR(50), -- read, write, delete
        ip_address INET,
        user_agent TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_data_assets_name ON data_governance.data_assets(name);
    CREATE INDEX IF NOT EXISTS idx_data_lineage_source ON data_governance.data_lineage(source_asset);
    CREATE INDEX IF NOT EXISTS idx_data_lineage_target ON data_governance.data_lineage(target_asset);
    CREATE INDEX IF NOT EXISTS idx_quality_metrics_table ON data_governance.data_quality_metrics(table_name);
    CREATE INDEX IF NOT EXISTS idx_quality_metrics_date ON data_governance.data_quality_metrics(date);
    CREATE INDEX IF NOT EXISTS idx_pipeline_logs_pipeline ON data_governance.pipeline_logs(pipeline_name);
    CREATE INDEX IF NOT EXISTS idx_pipeline_logs_status ON data_governance.pipeline_logs(status);
    """
    
    # Write SQL script to file
    with open("/home/steodhiambo/nyc-tlc-data-platform/sql/data_governance_tables.sql", "w") as f:
        f.write(sql_script)
    
    logger.info("Data governance SQL tables created successfully")
    return sql_script


def register_nyc_tlc_data_assets():
    """
    Register NYC TLC data assets in the governance system
    """
    governance_manager = DataGovernanceManager()
    
    # Define NYC TLC data assets
    assets = [
        DataAsset(
            name="raw_yellow_tripdata",
            type="parquet_file",
            location="s3://nyc-tlc-data-raw/taxi-data/yellow/",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Raw yellow taxi trip data in Parquet format",
            tags=["raw", "taxi", "yellow", "nyc-tlc"],
            lineage=[]
        ),
        DataAsset(
            name="raw_green_tripdata",
            type="parquet_file",
            location="s3://nyc-tlc-data-raw/taxi-data/green/",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Raw green taxi trip data in Parquet format",
            tags=["raw", "taxi", "green", "nyc-tlc"],
            lineage=[]
        ),
        DataAsset(
            name="staging_yellow_tripdata",
            type="table",
            location="postgres://warehouse/staging/yellow_tripdata",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Staging table for yellow taxi trip data after initial cleaning",
            tags=["staging", "taxi", "yellow", "nyc-tlc"],
            lineage=["raw_yellow_tripdata"]
        ),
        DataAsset(
            name="staging_green_tripdata",
            type="table",
            location="postgres://warehouse/staging/green_tripdata",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Staging table for green taxi trip data after initial cleaning",
            tags=["staging", "taxi", "green", "nyc-tlc"],
            lineage=["raw_green_tripdata"]
        ),
        DataAsset(
            name="fact_trips",
            type="table",
            location="postgres://warehouse/core/fact_trips",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Core fact table for taxi trips with all transformations applied",
            tags=["core", "fact", "taxi", "nyc-tlc"],
            lineage=["staging_yellow_tripdata", "staging_green_tripdata"]
        ),
        DataAsset(
            name="dim_location",
            type="table",
            location="postgres://warehouse/core/dim_location",
            owner="data-engineering-team",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Dimension table for taxi zones and locations",
            tags=["core", "dimension", "location", "nyc-tlc"],
            lineage=[]
        )
    ]
    
    # Register all assets
    for asset in assets:
        governance_manager.register_data_asset(asset)
    
    # Create lineage relationships
    lineage_relationships = [
        DataLineage(
            source_asset="raw_yellow_tripdata",
            target_asset="staging_yellow_tripdata",
            transformation="Initial data cleaning and validation",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Raw yellow taxi data transformed to staging table"
        ),
        DataLineage(
            source_asset="raw_green_tripdata",
            target_asset="staging_green_tripdata",
            transformation="Initial data cleaning and validation",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Raw green taxi data transformed to staging table"
        ),
        DataLineage(
            source_asset="staging_yellow_tripdata",
            target_asset="fact_trips",
            transformation="Dimensional modeling and aggregation",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Yellow taxi staging data transformed to fact table"
        ),
        DataLineage(
            source_asset="staging_green_tripdata",
            target_asset="fact_trips",
            transformation="Dimensional modeling and aggregation",
            created_at=datetime.now(),
            updated_at=datetime.now(),
            description="Green taxi staging data transformed to fact table"
        )
    ]
    
    for lineage in lineage_relationships:
        governance_manager.create_lineage(lineage)
    
    logger.info("NYC TLC data assets registered successfully")
    return assets


if __name__ == "__main__":
    # Setup data governance components
    print("Setting up data governance components...")
    
    # Create IAM roles
    iam_roles = create_iam_roles()
    print("IAM roles created")
    
    # Create metadata tables SQL
    sql_script = setup_metadata_tables_in_db()
    print("Metadata tables SQL script created")
    
    # Register NYC TLC data assets
    assets = register_nyc_tlc_data_assets()
    print(f"Registered {len(assets)} data assets")
    
    # Test governance manager
    governance_manager = DataGovernanceManager()
    
    # Get lineage for fact_trips
    lineage = governance_manager.get_lineage("fact_trips")
    print(f"Lineage for fact_trips: {lineage}")
    
    print("Data governance setup completed successfully!")