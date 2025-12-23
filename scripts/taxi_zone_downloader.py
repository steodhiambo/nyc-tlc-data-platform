#!/usr/bin/env python3
"""
Taxi Zone Downloader

This script downloads the NYC Taxi Zone lookup data and uploads it to S3.
The taxi zone data is needed to enrich trip data with location information.
"""

import os
import sys
import requests
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import tempfile


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('taxi_zone_download.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TaxiZoneDownloader:
    """
    Downloads NYC Taxi Zone lookup data and uploads to S3
    """
    
    def __init__(self, s3_bucket: str = None, aws_region: str = 'us-east-1'):
        """
        Initialize the downloader
        
        Args:
            s3_bucket: S3 bucket name to upload data to
            aws_region: AWS region for S3 operations
        """
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        self.s3_bucket = s3_bucket or os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Validate AWS credentials
        try:
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"Successfully connected to S3 bucket: {self.s3_bucket}")
        except ClientError as e:
            logger.error(f"Could not access S3 bucket {self.s3_bucket}: {e}")
            raise
    
    def download_zone_data(self, local_path: str) -> bool:
        """
        Download the taxi zone data from the official source
        
        Args:
            local_path: Local path to save the file
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            logger.info(f"Downloading taxi zone data from {self.base_url}")
            response = requests.get(self.base_url, timeout=60)
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Successfully downloaded taxi zone data to {local_path}")
            return True
        except requests.RequestException as e:
            logger.error(f"Download failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading taxi zone data: {e}")
            return False
    
    def validate_data(self, local_path: str) -> bool:
        """
        Validate the downloaded taxi zone data
        
        Args:
            local_path: Path to the downloaded CSV file
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            df = pd.read_csv(local_path)
            
            # Check required columns
            required_columns = ['LocationID', 'Borough', 'Zone', 'service_zone']
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"Missing required column: {col}")
                    return False
            
            # Check for null LocationIDs
            if df['LocationID'].isnull().any():
                logger.error("Found null LocationIDs in taxi zone data")
                return False
            
            # Check for duplicate LocationIDs
            if df['LocationID'].duplicated().any():
                logger.error("Found duplicate LocationIDs in taxi zone data")
                return False
            
            logger.info(f"Taxi zone data validation passed. Found {len(df)} records.")
            return True
        except Exception as e:
            logger.error(f"Error validating taxi zone data: {e}")
            return False
    
    def upload_to_s3(self, local_path: str, s3_key: str) -> bool:
        """
        Upload a local file to S3
        
        Args:
            local_path: Local file path
            s3_key: S3 object key
            
        Returns:
            True if upload was successful, False otherwise
        """
        try:
            logger.info(f"Uploading {local_path} to s3://{self.s3_bucket}/{s3_key}")
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            logger.info(f"Successfully uploaded to s3://{self.s3_bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Upload failed for {local_path}: {e}")
            return False
    
    def download_and_upload(self) -> bool:
        """
        Download taxi zone data and upload to S3
        
        Returns:
            True if operation was successful, False otherwise
        """
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # Download the file
            if not self.download_zone_data(tmp_path):
                logger.error("Failed to download taxi zone data")
                return False
            
            # Validate the data
            if not self.validate_data(tmp_path):
                logger.error("Taxi zone data validation failed")
                return False
            
            # Upload to S3 with appropriate key
            s3_key = f"taxi-zone-data/taxi_zone_lookup_{datetime.now().strftime('%Y%m%d')}.csv"
            
            if not self.upload_to_s3(tmp_path, s3_key):
                logger.error(f"Failed to upload {tmp_path} to S3")
                return False
            
            # Clean up temporary file
            os.unlink(tmp_path)
            
            return True
        except Exception as e:
            logger.error(f"Error in download and upload process: {e}")
            # Clean up temporary file if it exists
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            return False


def main():
    # Get S3 bucket from environment or use default
    s3_bucket = os.getenv('S3_RAW_BUCKET', 'nyc-tlc-raw-data-us-east-1')
    
    logger.info("Starting taxi zone data download process...")
    
    downloader = TaxiZoneDownloader(s3_bucket=s3_bucket)
    
    success = downloader.download_and_upload()
    
    if success:
        logger.info("Taxi zone data download and upload completed successfully!")
        sys.exit(0)
    else:
        logger.error("Taxi zone data download and upload failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()