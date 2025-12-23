#!/usr/bin/env python3
"""
TLC Data Downloader

This script downloads NYC TLC taxi trip data from the official source
and uploads it to S3 for processing. It supports both Yellow and Green
taxi data for multiple years and months.
"""

import os
import sys
import requests
import logging
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from typing import List, Optional
import argparse
import time
from urllib.parse import urlparse
import tempfile
import gzip
import shutil


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tlc_data_download.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TLCDataDownloader:
    """
    Downloads NYC TLC taxi trip data and uploads to S3
    """
    
    def __init__(self, s3_bucket: str = None, aws_region: str = 'us-east-1'):
        """
        Initialize the downloader
        
        Args:
            s3_bucket: S3 bucket name to upload data to
            aws_region: AWS region for S3 operations
        """
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
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
    
    def construct_filename(self, data_type: str, year: int, month: int) -> str:
        """
        Construct the filename for TLC data
        
        Args:
            data_type: 'yellow' or 'green' for taxi type
            year: Year of the data
            month: Month of the data (1-12)
            
        Returns:
            Filename string
        """
        # Format month with leading zero
        month_str = f"{month:02d}"
        
        if data_type.lower() == 'yellow':
            return f"{data_type}_tripdata_{year}-{month_str}.parquet"
        elif data_type.lower() == 'green':
            return f"{data_type}_tripdata_{year}-{month_str}.parquet"
        else:
            raise ValueError(f"Invalid data_type: {data_type}. Use 'yellow' or 'green'")
    
    def construct_url(self, data_type: str, year: int, month: int) -> str:
        """
        Construct the download URL for TLC data
        
        Args:
            data_type: 'yellow' or 'green' for taxi type
            year: Year of the data
            month: Month of the data (1-12)
            
        Returns:
            Download URL string
        """
        filename = self.construct_filename(data_type, year, month)
        return f"{self.base_url}/{filename}"
    
    def check_file_exists(self, url: str) -> bool:
        """
        Check if the file exists at the given URL
        
        Args:
            url: URL to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except requests.RequestException as e:
            logger.warning(f"Could not check URL {url}: {e}")
            return False
    
    def download_file(self, url: str, local_path: str) -> bool:
        """
        Download a file from URL to local path
        
        Args:
            url: URL to download from
            local_path: Local path to save the file
            
        Returns:
            True if download was successful, False otherwise
        """
        try:
            logger.info(f"Downloading {url}")
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Successfully downloaded to {local_path}")
            return True
        except requests.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
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
    
    def download_and_upload_single_file(self, data_type: str, year: int, month: int) -> bool:
        """
        Download a single file and upload to S3
        
        Args:
            data_type: 'yellow' or 'green' for taxi type
            year: Year of the data
            month: Month of the data (1-12)
            
        Returns:
            True if operation was successful, False otherwise
        """
        # Construct URL and paths
        url = self.construct_url(data_type, year, month)
        filename = self.construct_filename(data_type, year, month)
        
        # Check if file exists at URL
        if not self.check_file_exists(url):
            logger.warning(f"File does not exist at {url}")
            return False
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # Download the file
            if not self.download_file(url, tmp_path):
                logger.error(f"Failed to download {url}")
                return False
            
            # Upload to S3 with appropriate key
            s3_key = f"taxi-data/{data_type}/{year}/{filename}"
            
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
    
    def download_range(self, data_type: str, start_year: int, start_month: int, 
                      end_year: int, end_month: int, max_concurrent: int = 1) -> List[bool]:
        """
        Download a range of files
        
        Args:
            data_type: 'yellow' or 'green' for taxi type
            start_year: Starting year
            start_month: Starting month
            end_year: Ending year
            end_month: Ending month
            max_concurrent: Maximum number of concurrent downloads (not implemented in this version)
            
        Returns:
            List of boolean results for each download attempt
        """
        results = []
        
        # Convert to datetime for easier iteration
        start_date = datetime(start_year, start_month, 1)
        end_date = datetime(end_year, end_month, 1)
        
        current_date = start_date
        while current_date <= end_date:
            year, month = current_date.year, current_date.month
            logger.info(f"Processing {data_type} data for {year}-{month:02d}")
            
            success = self.download_and_upload_single_file(data_type, year, month)
            results.append(success)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return results
    
    def download_latest(self, data_type: str, months_back: int = 3) -> List[bool]:
        """
        Download the latest N months of data
        
        Args:
            data_type: 'yellow' or 'green' for taxi type
            months_back: Number of months back to download
            
        Returns:
            List of boolean results for each download attempt
        """
        # Calculate date range
        end_date = datetime.now().replace(day=1)  # First day of current month
        start_date = end_date
        for _ in range(months_back - 1):
            if start_date.month == 1:
                start_date = start_date.replace(year=start_date.year - 1, month=12)
            else:
                start_date = start_date.replace(month=start_date.month - 1)
        
        return self.download_range(
            data_type,
            start_date.year, start_date.month,
            end_date.year, end_date.month
        )


def main():
    parser = argparse.ArgumentParser(description='Download NYC TLC Taxi Data')
    parser.add_argument('--type', choices=['yellow', 'green'], required=True,
                        help='Type of taxi data to download')
    parser.add_argument('--start-year', type=int,
                        help='Start year for data download')
    parser.add_argument('--start-month', type=int,
                        help='Start month for data download')
    parser.add_argument('--end-year', type=int,
                        help='End year for data download')
    parser.add_argument('--end-month', type=int,
                        help='End month for data download')
    parser.add_argument('--latest', type=int, default=0,
                        help='Download latest N months of data')
    parser.add_argument('--s3-bucket', type=str,
                        help='S3 bucket to upload data to')
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = TLCDataDownloader(s3_bucket=args.s3_bucket)
    
    if args.latest > 0:
        # Download latest N months
        logger.info(f"Downloading latest {args.latest} months of {args.type} data")
        results = downloader.download_latest(args.type, args.latest)
    elif all(v is not None for v in [args.start_year, args.start_month, args.end_year, args.end_month]):
        # Download range
        logger.info(f"Downloading {args.type} data from {args.start_year}-{args.start_month:02d} "
                   f"to {args.end_year}-{args.end_month:02d}")
        results = downloader.download_range(
            args.type, args.start_year, args.start_month,
            args.end_year, args.end_month
        )
    else:
        logger.error("You must specify either --latest or all four date parameters (--start-year, --start-month, --end-year, --end-month)")
        sys.exit(1)
    
    # Print summary
    successful = sum(results)
    total = len(results)
    logger.info(f"Download completed: {successful}/{total} files successful")
    
    if successful == 0:
        logger.error("No files were successfully downloaded")
        sys.exit(1)
    elif successful < total:
        logger.warning(f"{total - successful} files failed to download")
        sys.exit(2)  # Exit with warning code
    else:
        logger.info("All files downloaded successfully!")


if __name__ == "__main__":
    main()