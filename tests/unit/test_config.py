"""
Unit tests for configuration module
"""
import unittest
from unittest.mock import patch, Mock
from config import (
    Config,
    get_config,
    get_postgres_connection_string,
    get_s3_client_config,
    get_s3_raw_bucket,
    get_s3_processed_bucket,
    get_smtp_config,
    get_slack_webhook_url
)


class TestConfig(unittest.TestCase):
    """Test cases for the Config class"""

    @patch('config.get_required_secret')
    @patch('config.get_secret')
    @patch('config.get_secret_as_int')
    @patch('config.get_secret_as_bool')
    def test_config_initialization(self, mock_get_bool, mock_get_int, mock_get_secret, mock_get_required):
        """Test Config class initialization"""
        # Set up mock return values
        mock_get_required.side_effect = [
            'test_access_key',    # AWS_ACCESS_KEY_ID
            'test_secret_key',    # AWS_SECRET_ACCESS_KEY
            'test_password'       # POSTGRES_PASSWORD (needed for Config initialization)
        ]
        mock_get_secret.side_effect = [
            'us-west-2',          # AWS_DEFAULT_REGION
            'localhost',          # POSTGRES_HOST
            'nyc_tlc_user',       # POSTGRES_USER
            'nyc_tlc_dw',         # POSTGRES_DB
            'raw-bucket',         # S3_RAW_BUCKET
            'processed-bucket',   # S3_PROCESSED_BUCKET
            'smtp.example.com',   # SMTP_HOST
            'user@example.com',   # SMTP_USER
            'smtp_password',      # SMTP_PASSWORD
            'https://hooks.slack.com/...'  # SLACK_WEBHOOK_URL
        ]
        mock_get_int.side_effect = [
            5433,  # POSTGRES_PORT
            587,   # SMTP_PORT
            50000, # AIRFLOW_UID
            50000, # AIRFLOW_GID
            365,   # DATA_RETENTION_DAYS
            1,     # MAX_CONCURRENT_DOWNLOADS
        ]
        mock_get_bool.side_effect = [
            True,  # ENABLE_MONITORING
            True   # ENABLE_ALERTS
        ]

        # Create config instance
        config = Config()

        # Assertions
        self.assertEqual(config.aws_access_key_id, 'test_access_key')
        self.assertEqual(config.aws_secret_access_key, 'test_secret_key')
        self.assertEqual(config.aws_default_region, 'us-west-2')
        self.assertEqual(config.postgres_host, 'localhost')
        self.assertEqual(config.postgres_port, 5433)
        self.assertEqual(config.postgres_user, 'nyc_tlc_user')
        self.assertEqual(config.postgres_password, 'test_password')  # From get_required_secret
        self.assertEqual(config.postgres_db, 'nyc_tlc_dw')
        self.assertEqual(config.s3_raw_bucket, 'raw-bucket')
        self.assertEqual(config.s3_processed_bucket, 'processed-bucket')
        self.assertEqual(config.smtp_host, 'smtp.example.com')
        self.assertEqual(config.smtp_port, 587)
        self.assertEqual(config.smtp_user, 'user@example.com')
        self.assertEqual(config.smtp_password, 'smtp_password')
        self.assertEqual(config.slack_webhook_url, 'https://hooks.slack.com/...')
        self.assertEqual(config.airflow_uid, 50000)
        self.assertEqual(config.airflow_gid, 50000)
        self.assertEqual(config.data_retention_days, 365)
        self.assertEqual(config.max_concurrent_downloads, 1)
        self.assertTrue(config.enable_monitoring)
        self.assertTrue(config.enable_alerts)

    @patch('config.get_required_secret')
    @patch('config.get_secret')
    @patch('config.get_secret_as_int')
    @patch('config.get_secret_as_bool')
    def test_postgres_connection_string(self, mock_get_bool, mock_get_int, mock_get_secret, mock_get_required):
        """Test postgres connection string property"""
        # Set up mocks
        mock_get_required.side_effect = ['test_key', 'test_secret', 'test_password']
        mock_get_secret.side_effect = [
            'us-east-1',          # AWS_DEFAULT_REGION
            'localhost',          # POSTGRES_HOST
            'test_user',          # POSTGRES_USER
            'test_db',            # POSTGRES_DB
            'raw-bucket',         # S3_RAW_BUCKET
            'processed-bucket',   # S3_PROCESSED_BUCKET
            'smtp.example.com',   # SMTP_HOST
            'user@example.com',   # SMTP_USER
            'smtp_password',      # SMTP_PASSWORD
            'https://hooks.slack.com/...'  # SLACK_WEBHOOK_URL
        ]
        mock_get_int.side_effect = [
            5432,  # POSTGRES_PORT
            587,   # SMTP_PORT
            50000, # AIRFLOW_UID
            50000, # AIRFLOW_GID
            365,   # DATA_RETENTION_DAYS
            1,     # MAX_CONCURRENT_DOWNLOADS
        ]
        mock_get_bool.side_effect = [
            True,  # ENABLE_MONITORING
            True   # ENABLE_ALERTS
        ]

        config = Config()

        expected = "postgresql://test_user:test_password@localhost:5432/test_db"
        self.assertEqual(config.postgres_connection_string, expected)


class TestConfigFunctions(unittest.TestCase):
    """Test cases for configuration utility functions"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Reset the global config instance
        import config
        config._config = None

    @patch('config.Config')
    def test_get_config(self, mock_config_class):
        """Test get_config function"""
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # Call the function twice
        result1 = get_config()
        result2 = get_config()

        # Both calls should return the same instance
        self.assertIs(result1, result2)
        self.assertIs(result1, mock_config)

        # Config should only be initialized once
        mock_config_class.assert_called_once()

    @patch('config.get_config')
    def test_get_postgres_connection_string(self, mock_get_config):
        """Test get_postgres_connection_string function"""
        mock_config = Mock()
        mock_config.postgres_connection_string = 'test_connection_string'
        mock_get_config.return_value = mock_config

        result = get_postgres_connection_string()

        self.assertEqual(result, 'test_connection_string')
        mock_get_config.assert_called_once()

    @patch('config.get_config')
    def test_get_s3_client_config(self, mock_get_config):
        """Test get_s3_client_config function"""
        expected_config = {
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'region_name': 'us-east-1'
        }
        mock_config = Mock()
        mock_config.s3_client_config = expected_config
        mock_get_config.return_value = mock_config

        result = get_s3_client_config()

        self.assertEqual(result, expected_config)
        mock_get_config.assert_called_once()

    @patch('config.get_config')
    def test_get_s3_raw_bucket(self, mock_get_config):
        """Test get_s3_raw_bucket function"""
        mock_config = Mock()
        mock_config.s3_raw_bucket = 'test-raw-bucket'
        mock_get_config.return_value = mock_config

        result = get_s3_raw_bucket()

        self.assertEqual(result, 'test-raw-bucket')
        mock_get_config.assert_called_once()

    @patch('config.get_config')
    def test_get_s3_processed_bucket(self, mock_get_config):
        """Test get_s3_processed_bucket function"""
        mock_config = Mock()
        mock_config.s3_processed_bucket = 'test-processed-bucket'
        mock_get_config.return_value = mock_config

        result = get_s3_processed_bucket()

        self.assertEqual(result, 'test-processed-bucket')
        mock_get_config.assert_called_once()

    @patch('config.get_config')
    def test_get_smtp_config(self, mock_get_config):
        """Test get_smtp_config function"""
        expected_config = {
            'host': 'smtp.example.com',
            'port': 587,
            'user': 'user@example.com',
            'password': 'password'
        }
        mock_config = Mock()
        mock_config.smtp_user = 'user@example.com'
        mock_config.smtp_password = 'password'
        # Mock the other properties
        mock_config.smtp_host = 'smtp.example.com'
        mock_config.smtp_port = 587
        mock_get_config.return_value = mock_config

        result = get_smtp_config()

        self.assertEqual(result, expected_config)
        mock_get_config.assert_called_once()

    @patch('config.get_config')
    def test_get_slack_webhook_url(self, mock_get_config):
        """Test get_slack_webhook_url function"""
        mock_config = Mock()
        mock_config.slack_webhook_url = 'https://hooks.slack.com/test'
        mock_get_config.return_value = mock_config

        result = get_slack_webhook_url()

        self.assertEqual(result, 'https://hooks.slack.com/test')
        mock_get_config.assert_called_once()


if __name__ == '__main__':
    unittest.main()