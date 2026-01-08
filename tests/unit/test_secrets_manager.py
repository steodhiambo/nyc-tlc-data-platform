"""
Unit tests for configuration utilities
"""
import unittest
import os
from unittest.mock import patch, Mock, MagicMock
from config.secrets_manager import (
    SecretBackend,
    SecretConfig,
    SecretsManager,
    initialize_secrets_manager,
    get_secret,
    get_required_secret,
    get_secret_as_int,
    get_secret_as_bool
)


class TestSecretBackend(unittest.TestCase):
    """Test cases for SecretBackend enum"""
    
    def test_secret_backend_values(self):
        """Test that SecretBackend enum has the expected values"""
        self.assertEqual(SecretBackend.ENVIRONMENT.value, "environment")
        self.assertEqual(SecretBackend.AWS_SECRETS_MANAGER.value, "aws_secrets_manager")
        self.assertEqual(SecretBackend.HASHICORP_VAULT.value, "hashicorp_vault")
        self.assertEqual(SecretBackend.FILE.value, "file")


class TestSecretConfig(unittest.TestCase):
    """Test cases for SecretConfig class"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = SecretConfig()
        
        self.assertEqual(config.backend, SecretBackend.ENVIRONMENT)
        self.assertEqual(config.aws_region, "us-east-1")
        self.assertIsNone(config.vault_url)
        self.assertIsNone(config.secrets_file_path)
    
    def test_custom_config(self):
        """Test custom configuration values"""
        config = SecretConfig(
            backend=SecretBackend.AWS_SECRETS_MANAGER,
            aws_region="us-west-2",
            vault_url="http://vault.example.com",
            secrets_file_path="/path/to/secrets.json"
        )
        
        self.assertEqual(config.backend, SecretBackend.AWS_SECRETS_MANAGER)
        self.assertEqual(config.aws_region, "us-west-2")
        self.assertEqual(config.vault_url, "http://vault.example.com")
        self.assertEqual(config.secrets_file_path, "/path/to/secrets.json")


class TestSecretsManager(unittest.TestCase):
    """Test cases for SecretsManager class"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.config = SecretConfig(backend=SecretBackend.ENVIRONMENT)
        self.secrets_manager = SecretsManager(self.config)
    
    def test_get_secret_from_environment(self):
        """Test getting secrets from environment variables"""
        # Set up environment variable
        os.environ['TEST_SECRET'] = 'test_value'
        
        try:
            result = self.secrets_manager.get_secret('TEST_SECRET')
            self.assertEqual(result, 'test_value')
        finally:
            # Clean up
            if 'TEST_SECRET' in os.environ:
                del os.environ['TEST_SECRET']
    
    def test_get_secret_with_default(self):
        """Test getting secrets with default value"""
        result = self.secrets_manager.get_secret('NONEXISTENT_SECRET', 'default_value')
        self.assertEqual(result, 'default_value')
    
    def test_get_secret_not_found(self):
        """Test getting non-existent secret without default"""
        result = self.secrets_manager.get_secret('NONEXISTENT_SECRET')
        self.assertIsNone(result)
    
    def test_get_required_secret_found(self):
        """Test getting required secret that exists"""
        # Set up environment variable
        os.environ['REQUIRED_SECRET'] = 'required_value'
        
        try:
            result = self.secrets_manager.get_required_secret('REQUIRED_SECRET')
            self.assertEqual(result, 'required_value')
        finally:
            # Clean up
            if 'REQUIRED_SECRET' in os.environ:
                del os.environ['REQUIRED_SECRET']
    
    def test_get_required_secret_not_found(self):
        """Test getting required secret that doesn't exist"""
        with self.assertRaises(ValueError) as context:
            self.secrets_manager.get_required_secret('NONEXISTENT_REQUIRED_SECRET')
        
        self.assertIn('Required secret', str(context.exception))
        self.assertIn('NONEXISTENT_REQUIRED_SECRET', str(context.exception))
    
    def test_get_secret_as_int(self):
        """Test getting secret as integer"""
        # Set up environment variable
        os.environ['INT_SECRET'] = '123'
        
        try:
            result = self.secrets_manager.get_secret_as_int('INT_SECRET')
            self.assertEqual(result, 123)
            
            # Test with default
            result = self.secrets_manager.get_secret_as_int('NONEXISTENT_INT', 456)
            self.assertEqual(result, 456)
            
            # Test invalid integer
            os.environ['INVALID_INT_SECRET'] = 'not_an_int'
            result = self.secrets_manager.get_secret_as_int('INVALID_INT_SECRET', 789)
            self.assertEqual(result, 789)
        finally:
            # Clean up
            for key in ['INT_SECRET', 'INVALID_INT_SECRET']:
                if key in os.environ:
                    del os.environ[key]
    
    def test_get_secret_as_bool(self):
        """Test getting secret as boolean"""
        test_cases = [
            ('true', True),
            ('1', True),
            ('yes', True),
            ('on', True),
            ('false', False),
            ('0', False),
            ('no', False),
            ('off', False),
        ]
        
        for value, expected in test_cases:
            os.environ['BOOL_SECRET'] = value
            try:
                result = self.secrets_manager.get_secret_as_bool('BOOL_SECRET')
                self.assertEqual(result, expected, f"Failed for value: {value}")
            finally:
                del os.environ['BOOL_SECRET']
        
        # Test with default
        result = self.secrets_manager.get_secret_as_bool('NONEXISTENT_BOOL', True)
        self.assertTrue(result)


class TestGlobalSecretsManager(unittest.TestCase):
    """Test cases for global secrets manager functions"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Clear the global instance for clean tests
        from config import secrets_manager as sm
        sm._secrets_manager = None
    
    @patch('config.secrets_manager.SecretsManager')
    def test_initialize_secrets_manager(self, mock_secrets_manager_class):
        """Test initializing the global secrets manager"""
        mock_instance = Mock()
        mock_secrets_manager_class.return_value = mock_instance
        
        config = SecretConfig()
        result = initialize_secrets_manager(config)
        
        self.assertEqual(result, mock_instance)
        mock_secrets_manager_class.assert_called_once_with(config)
    
    def test_get_secret_fallback(self):
        """Test get_secret falls back to environment when not initialized"""
        os.environ['FALLBACK_SECRET'] = 'fallback_value'
        
        try:
            result = get_secret('FALLBACK_SECRET')
            self.assertEqual(result, 'fallback_value')
        finally:
            if 'FALLBACK_SECRET' in os.environ:
                del os.environ['FALLBACK_SECRET']
    
    def test_get_required_secret_fallback(self):
        """Test get_required_secret falls back to environment when not initialized"""
        os.environ['REQUIRED_FALLBACK_SECRET'] = 'required_fallback_value'
        
        try:
            result = get_required_secret('REQUIRED_FALLBACK_SECRET')
            self.assertEqual(result, 'required_fallback_value')
        finally:
            if 'REQUIRED_FALLBACK_SECRET' in os.environ:
                del os.environ['REQUIRED_FALLBACK_SECRET']
    
    def test_get_required_secret_fallback_not_found(self):
        """Test get_required_secret raises error when not found in fallback"""
        with self.assertRaises(ValueError) as context:
            get_required_secret('NONEXISTENT_FALLBACK_SECRET')
        
        self.assertIn('Required secret', str(context.exception))
        self.assertIn('NONEXISTENT_FALLBACK_SECRET', str(context.exception))
    
    def test_get_secret_as_int_fallback(self):
        """Test get_secret_as_int falls back to environment when not initialized"""
        os.environ['INT_FALLBACK_SECRET'] = '999'
        
        try:
            result = get_secret_as_int('INT_FALLBACK_SECRET')
            self.assertEqual(result, 999)
            
            # Test default fallback
            result = get_secret_as_int('NONEXISTENT_INT_FALLBACK', 888)
            self.assertEqual(result, 888)
        finally:
            if 'INT_FALLBACK_SECRET' in os.environ:
                del os.environ['INT_FALLBACK_SECRET']


if __name__ == '__main__':
    unittest.main()