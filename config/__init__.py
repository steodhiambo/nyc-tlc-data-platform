"""
Configuration Management Module

This module provides centralized configuration management for the NYC TLC Data Platform
using the secure secrets manager.
"""

import os
from typing import Optional

# Import secrets manager functions to make them available at the module level
from config.secrets_manager import (
    SecretConfig,
    SecretBackend,
    initialize_secrets_manager,
    get_secret,
    get_required_secret,
    get_secret_as_int,
    get_secret_as_bool
)


class Config:
    """
    Configuration class that manages all application settings
    """

    def __init__(self):
        # Initialize secrets manager based on environment
        backend_str = os.getenv('SECRETS_BACKEND', 'environment')
        backend = SecretBackend(backend_str)

        config = SecretConfig(
            backend=backend,
            aws_region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            vault_url=os.getenv('VAULT_URL'),
            secrets_file_path=os.getenv('SECRETS_FILE_PATH')
        )

        initialize_secrets_manager(config)

        # Load configuration values
        self.aws_access_key_id = get_required_secret('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = get_required_secret('AWS_SECRET_ACCESS_KEY')
        self.aws_default_region = get_secret('AWS_DEFAULT_REGION', 'us-east-1')

        # Database configuration
        self.postgres_host = get_secret('POSTGRES_HOST', 'localhost')
        self.postgres_port = get_secret_as_int('POSTGRES_PORT', 5433)
        self.postgres_user = get_secret('POSTGRES_USER', 'nyc_tlc_user')
        self.postgres_password = get_required_secret('POSTGRES_PASSWORD')
        self.postgres_db = get_secret('POSTGRES_DB', 'nyc_tlc_dw')

        # S3 configuration
        self.s3_raw_bucket = get_secret('S3_RAW_BUCKET', f'nyc-tlc-raw-data-{self.aws_default_region}')
        self.s3_processed_bucket = get_secret('S3_PROCESSED_BUCKET', f'nyc-tlc-processed-data-{self.aws_default_region}')

        # Email configuration for alerts
        self.smtp_host = get_secret('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = get_secret_as_int('SMTP_PORT', 587)
        self.smtp_user = get_secret('SMTP_USER')
        self.smtp_password = get_secret('SMTP_PASSWORD')

        # Slack configuration for alerts
        self.slack_webhook_url = get_secret('SLACK_WEBHOOK_URL')

        # Airflow configuration
        self.airflow_uid = get_secret_as_int('AIRFLOW_UID', 50000)
        self.airflow_gid = get_secret_as_int('AIRFLOW_GID', 50000)

        # Application-specific settings
        self.data_retention_days = get_secret_as_int('DATA_RETENTION_DAYS', 365)
        self.max_concurrent_downloads = get_secret_as_int('MAX_CONCURRENT_DOWNLOADS', 1)
        self.enable_monitoring = get_secret_as_bool('ENABLE_MONITORING', True)
        self.enable_alerts = get_secret_as_bool('ENABLE_ALERTS', True)

    @property
    def postgres_connection_string(self) -> str:
        """Get the PostgreSQL connection string"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def s3_client_config(self) -> dict:
        """Get S3 client configuration"""
        return {
            'aws_access_key_id': self.aws_access_key_id,
            'aws_secret_access_key': self.aws_secret_access_key,
            'region_name': self.aws_default_region
        }


# Global configuration instance
_config: Optional[Config] = None


def get_config() -> Config:
    """
    Get the global configuration instance

    Returns:
        Config instance with all application settings
    """
    global _config
    if _config is None:
        _config = Config()
    return _config


# Convenience functions for accessing configuration values
def get_postgres_connection_string() -> str:
    """Get the PostgreSQL connection string"""
    return get_config().postgres_connection_string


def get_s3_client_config() -> dict:
    """Get S3 client configuration"""
    return get_config().s3_client_config


def get_s3_raw_bucket() -> str:
    """Get the raw S3 bucket name"""
    return get_config().s3_raw_bucket


def get_s3_processed_bucket() -> str:
    """Get the processed S3 bucket name"""
    return get_config().s3_processed_bucket


def get_smtp_config() -> dict:
    """Get SMTP configuration for email alerts"""
    config = get_config()
    return {
        'host': config.smtp_host,
        'port': config.smtp_port,
        'user': config.smtp_user,
        'password': config.smtp_password
    }


def get_slack_webhook_url() -> Optional[str]:
    """Get the Slack webhook URL"""
    return get_config().slack_webhook_url


if __name__ == "__main__":
    # Example usage
    config = get_config()
    print(f"AWS Region: {config.aws_default_region}")
    print(f"PostgreSQL Host: {config.postgres_host}")
    print(f"S3 Raw Bucket: {config.s3_raw_bucket}")
    print(f"Connection String: {config.postgres_connection_string[:50]}...")