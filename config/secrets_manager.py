"""
Secure Secrets Management Module

This module provides a centralized way to manage secrets and configuration
values with support for multiple backends including environment variables,
AWS Secrets Manager, and HashiCorp Vault.
"""

import os
import json
import logging
from typing import Optional, Union
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class SecretBackend(Enum):
    """Enumeration of supported secret backends"""
    ENVIRONMENT = "environment"
    AWS_SECRETS_MANAGER = "aws_secrets_manager"
    HASHICORP_VAULT = "hashicorp_vault"
    FILE = "file"


@dataclass
class SecretConfig:
    """Configuration for secret management"""
    backend: SecretBackend = SecretBackend.ENVIRONMENT
    aws_region: str = "us-east-1"
    vault_url: Optional[str] = None
    secrets_file_path: Optional[str] = None


class SecretsManager:
    """
    A secure secrets management class that supports multiple backends
    """
    
    def __init__(self, config: SecretConfig):
        self.config = config
        self._cache = {}
        
        # Initialize backend-specific clients if needed
        if self.config.backend == SecretBackend.AWS_SECRETS_MANAGER:
            try:
                import boto3
                from botocore.exceptions import ClientError
                self.boto3_client = boto3.client('secretsmanager', region_name=self.config.aws_region)
            except ImportError:
                raise ImportError("boto3 is required for AWS Secrets Manager backend")
        elif self.config.backend == SecretBackend.HASHICORP_VAULT:
            try:
                import hvac
                self.vault_client = hvac.Client(url=self.config.vault_url)
            except ImportError:
                raise ImportError("hvac is required for HashiCorp Vault backend")
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Retrieve a secret value from the configured backend
        
        Args:
            key: The secret key to retrieve
            default: Default value to return if secret is not found
            
        Returns:
            The secret value or default if not found
        """
        # Check cache first
        if key in self._cache:
            return self._cache[key]
        
        secret_value = None
        
        try:
            if self.config.backend == SecretBackend.ENVIRONMENT:
                secret_value = self._get_from_environment(key)
            elif self.config.backend == SecretBackend.AWS_SECRETS_MANAGER:
                secret_value = self._get_from_aws_secrets_manager(key)
            elif self.config.backend == SecretBackend.HASHICORP_VAULT:
                secret_value = self._get_from_hashicorp_vault(key)
            elif self.config.backend == SecretBackend.FILE:
                secret_value = self._get_from_file(key)
        except Exception as e:
            logger.warning(f"Failed to retrieve secret '{key}' from {self.config.backend.value}: {e}")
            secret_value = default
        
        # Cache the result (even if it's the default)
        self._cache[key] = secret_value
        return secret_value
    
    def _get_from_environment(self, key: str) -> Optional[str]:
        """Get secret from environment variables"""
        return os.getenv(key)
    
    def _get_from_aws_secrets_manager(self, key: str) -> Optional[str]:
        """Get secret from AWS Secrets Manager"""
        try:
            response = self.boto3_client.get_secret_value(SecretId=key)
            secret_string = response['SecretString']
            
            # Try to parse as JSON first (for structured secrets)
            try:
                secret_dict = json.loads(secret_string)
                # If the secret is a JSON object, we might need to handle it differently
                # For now, return the whole string or specific field if key contains path
                return secret_string
            except json.JSONDecodeError:
                # If not JSON, return as is
                return secret_string
        except self.boto3_client.exceptions.ResourceNotFoundException:
            logger.warning(f"Secret '{key}' not found in AWS Secrets Manager")
            return None
        except Exception as e:
            logger.error(f"Error retrieving secret '{key}' from AWS Secrets Manager: {e}")
            raise
    
    def _get_from_hashicorp_vault(self, key: str) -> Optional[str]:
        """Get secret from HashiCorp Vault"""
        try:
            # Split key to handle paths like 'secret/data/myapp/db_password'
            if '/' in key:
                path_parts = key.split('/')
                secret_path = '/'.join(path_parts[:-1])
                secret_key = path_parts[-1]
            else:
                secret_path = 'secret/data'
                secret_key = key
            
            response = self.vault_client.secrets.kv.v2.read_secret_version(path=secret_path)
            secret_data = response['data']['data']
            
            return secret_data.get(secret_key)
        except Exception as e:
            logger.error(f"Error retrieving secret '{key}' from HashiCorp Vault: {e}")
            raise
    
    def _get_from_file(self, key: str) -> Optional[str]:
        """Get secret from a file"""
        if not self.config.secrets_file_path:
            raise ValueError("secrets_file_path not configured")
        
        try:
            with open(self.config.secrets_file_path, 'r') as f:
                secrets = json.load(f)
                return secrets.get(key)
        except FileNotFoundError:
            logger.warning(f"Secrets file '{self.config.secrets_file_path}' not found")
            return None
        except json.JSONDecodeError:
            logger.error(f"Secrets file '{self.config.secrets_file_path}' is not valid JSON")
            return None
    
    def get_required_secret(self, key: str) -> str:
        """
        Retrieve a required secret value, raising an exception if not found
        
        Args:
            key: The secret key to retrieve
            
        Returns:
            The secret value
            
        Raises:
            ValueError: If the secret is not found
        """
        value = self.get_secret(key)
        if value is None:
            raise ValueError(f"Required secret '{key}' not found in {self.config.backend.value}")
        return value
    
    def get_secret_as_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        """Get a secret value as an integer"""
        value = self.get_secret(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            logger.error(f"Secret '{key}' is not a valid integer: {value}")
            return default
    
    def get_secret_as_bool(self, key: str, default: Optional[bool] = None) -> Optional[bool]:
        """Get a secret value as a boolean"""
        value = self.get_secret(key)
        if value is None:
            return default
        # Common truthy values
        return value.lower() in ('true', '1', 'yes', 'on')


# Global instance for easy access
_secrets_manager: Optional[SecretsManager] = None


def initialize_secrets_manager(config: SecretConfig) -> SecretsManager:
    """
    Initialize the global secrets manager instance
    
    Args:
        config: SecretConfig instance with configuration
        
    Returns:
        Initialized SecretsManager instance
    """
    global _secrets_manager
    _secrets_manager = SecretsManager(config)
    return _secrets_manager


def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get a secret value from the global secrets manager
    
    Args:
        key: The secret key to retrieve
        default: Default value to return if secret is not found
        
    Returns:
        The secret value or default if not found
    """
    if _secrets_manager is None:
        # Fallback to environment variables if not initialized
        return os.getenv(key, default)
    return _secrets_manager.get_secret(key, default)


def get_required_secret(key: str) -> str:
    """
    Get a required secret value from the global secrets manager
    
    Args:
        key: The secret key to retrieve
        
    Returns:
        The secret value
        
    Raises:
        ValueError: If the secret is not found
    """
    if _secrets_manager is None:
        # Fallback to environment variables if not initialized
        value = os.getenv(key)
        if value is None:
            raise ValueError(f"Required secret '{key}' not found in environment")
        return value
    return _secrets_manager.get_required_secret(key)


# Convenience functions for common secret types
def get_secret_as_int(key: str, default: Optional[int] = None) -> Optional[int]:
    """Get a secret value as an integer"""
    if _secrets_manager is None:
        value = os.getenv(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            logger.error(f"Secret '{key}' is not a valid integer: {value}")
            return default
    return _secrets_manager.get_secret_as_int(key, default)


def get_secret_as_bool(key: str, default: Optional[bool] = None) -> Optional[bool]:
    """Get a secret value as a boolean"""
    if _secrets_manager is None:
        value = os.getenv(key)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')
    return _secrets_manager.get_secret_as_bool(key, default)


# Example usage and initialization
if __name__ == "__main__":
    # Example of how to initialize and use the secrets manager
    import sys
    
    # Determine backend from environment or default to environment
    backend_str = os.getenv('SECRETS_BACKEND', 'environment')
    backend = SecretBackend(backend_str)
    
    config = SecretConfig(
        backend=backend,
        aws_region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
        vault_url=os.getenv('VAULT_URL'),
        secrets_file_path=os.getenv('SECRETS_FILE_PATH')
    )
    
    # Initialize the global secrets manager
    secrets_manager = initialize_secrets_manager(config)
    
    # Example usage
    try:
        db_password = get_required_secret('POSTGRES_PASSWORD')
        aws_access_key = get_secret('AWS_ACCESS_KEY_ID')
        print(f"DB Password length: {len(db_password) if db_password else 0}")
        print(f"AWS Access Key ID: {'***' if aws_access_key else 'Not found'}")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)