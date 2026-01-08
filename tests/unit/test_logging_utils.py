"""
Unit tests for logging utilities
"""
import unittest
import json
import logging
from unittest.mock import patch, MagicMock
from io import StringIO
from utils.logging_utils import (
    StructuredLogger,
    PipelineLogger,
    get_pipeline_logger,
    setup_logging
)


class TestStructuredLogger(unittest.TestCase):
    """Test cases for the StructuredLogger class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.logger = StructuredLogger("test_logger")
        # Capture logs to test output
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(logging.Formatter('%(message)s'))  # JSON formatter outputs message as JSON
        self.logger.logger.handlers.clear()
        self.logger.logger.addHandler(self.handler)
        self.logger.logger.setLevel(logging.DEBUG)

    def test_log_levels(self):
        """Test that different log levels work correctly"""
        # Test debug
        self.logger.debug("Debug message", extra_field="value")

        # Test info
        self.logger.info("Info message", user_id=123)

        # Test warning
        self.logger.warning("Warning message", task="test_task")

        # Test error
        self.logger.error("Error message", error_code=500)

        # Test critical
        self.logger.critical("Critical message", severity="high")

        output = self.log_stream.getvalue()
        lines = [line.strip() for line in output.split('\n') if line.strip()]

        # Check that we have 5 log entries
        self.assertEqual(len(lines), 5)

        # Parse each log entry and verify structure
        for line in lines:
            log_entry = json.loads(line)
            self.assertIn('timestamp', log_entry)
            self.assertIn('level', log_entry)
            self.assertIn('message', log_entry)
            self.assertIn('service', log_entry)
            self.assertIn('context', log_entry)
            self.assertIn('caller', log_entry)

    def test_log_with_context(self):
        """Test logging with additional context"""
        self.logger.info("Test message", user="test_user", action="test_action")

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Test message")
        self.assertEqual(log_entry['context']['user'], "test_user")
        self.assertEqual(log_entry['context']['action'], "test_action")


class TestPipelineLogger(unittest.TestCase):
    """Test cases for the PipelineLogger class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.pipeline_logger = PipelineLogger("test_pipeline")
        # Set up logging to capture output
        self.log_stream = StringIO()
        self.handler = logging.StreamHandler(self.log_stream)
        self.handler.setFormatter(logging.Formatter('%(message)s'))  # JSON formatter outputs message as JSON
        self.pipeline_logger.logger.logger.handlers.clear()
        self.pipeline_logger.logger.logger.addHandler(self.handler)
        self.pipeline_logger.logger.logger.setLevel(logging.DEBUG)

    def test_log_pipeline_start(self):
        """Test logging pipeline start"""
        self.pipeline_logger.log_pipeline_start("run_123", user="test_user")

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Pipeline test_pipeline started")
        self.assertEqual(log_entry['context']['pipeline_name'], "test_pipeline")
        self.assertEqual(log_entry['context']['run_id'], "run_123")
        self.assertEqual(log_entry['context']['event_type'], "pipeline_start")
        self.assertEqual(log_entry['context']['user'], "test_user")

    def test_log_pipeline_end(self):
        """Test logging pipeline end"""
        self.pipeline_logger.log_pipeline_end("run_123", "SUCCESS", 45.2, records_processed=1000)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Pipeline test_pipeline completed with status SUCCESS")
        self.assertEqual(log_entry['context']['pipeline_name'], "test_pipeline")
        self.assertEqual(log_entry['context']['run_id'], "run_123")
        self.assertEqual(log_entry['context']['event_type'], "pipeline_end")
        self.assertEqual(log_entry['context']['status'], "SUCCESS")
        self.assertEqual(log_entry['context']['duration_seconds'], 45.2)
        self.assertEqual(log_entry['context']['records_processed'], 1000)

    def test_log_task_start(self):
        """Test logging task start"""
        self.pipeline_logger.log_task_start("extract_data", "run_123", year=2023, month=1)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Task extract_data started")
        self.assertEqual(log_entry['context']['task_id'], "extract_data")
        self.assertEqual(log_entry['context']['run_id'], "run_123")
        self.assertEqual(log_entry['context']['event_type'], "task_start")
        self.assertEqual(log_entry['context']['year'], 2023)
        self.assertEqual(log_entry['context']['month'], 1)

    def test_log_task_end(self):
        """Test logging task end"""
        self.pipeline_logger.log_task_end("extract_data", "run_123", "SUCCESS", 10.5, file_count=5)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Task extract_data completed with status SUCCESS")
        self.assertEqual(log_entry['context']['task_id'], "extract_data")
        self.assertEqual(log_entry['context']['run_id'], "run_123")
        self.assertEqual(log_entry['context']['event_type'], "task_end")
        self.assertEqual(log_entry['context']['status'], "SUCCESS")
        self.assertEqual(log_entry['context']['duration_seconds'], 10.5)
        self.assertEqual(log_entry['context']['file_count'], 5)

    def test_log_data_processing(self):
        """Test logging data processing"""
        self.pipeline_logger.log_data_processing("extract", 1000, table="yellow_tripdata")

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Data processing: extract")
        self.assertEqual(log_entry['context']['operation'], "extract")
        self.assertEqual(log_entry['context']['record_count'], 1000)
        self.assertEqual(log_entry['context']['event_type'], "data_processing")
        self.assertEqual(log_entry['context']['table'], "yellow_tripdata")

    def test_log_data_validation(self):
        """Test logging data validation"""
        details = {"null_count": 0, "total_count": 1000}
        self.pipeline_logger.log_data_validation("not_null_check", True, details)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Data validation not_null_check passed")
        self.assertEqual(log_entry['context']['validation_name'], "not_null_check")
        self.assertEqual(log_entry['context']['passed'], True)
        self.assertEqual(log_entry['context']['details'], details)
        self.assertEqual(log_entry['context']['event_type'], "data_validation")

    def test_log_error(self):
        """Test logging errors"""
        self.pipeline_logger.log_error("ConnectionError", "Failed to connect", "extract_task", retry_count=3)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Error occurred: Failed to connect")
        self.assertEqual(log_entry['context']['error_type'], "ConnectionError")
        self.assertEqual(log_entry['context']['error_message'], "Failed to connect")
        self.assertEqual(log_entry['context']['task_id'], "extract_task")
        self.assertEqual(log_entry['context']['event_type'], "error")
        self.assertEqual(log_entry['context']['retry_count'], 3)

    def test_log_warning(self):
        """Test logging warnings"""
        self.pipeline_logger.log_warning("Large file detected", "transform_task", file_size_mb=500)

        output = self.log_stream.getvalue().strip()
        log_entry = json.loads(output.split('\n')[0])  # Get first (and only) log line

        self.assertEqual(log_entry['message'], "Warning: Large file detected")
        self.assertEqual(log_entry['context']['warning_message'], "Large file detected")
        self.assertEqual(log_entry['context']['task_id'], "transform_task")
        self.assertEqual(log_entry['context']['event_type'], "warning")
        self.assertEqual(log_entry['context']['file_size_mb'], 500)


class TestPipelineLoggerFactory(unittest.TestCase):
    """Test cases for the pipeline logger factory function"""

    def test_get_pipeline_logger(self):
        """Test getting a pipeline logger instance"""
        logger1 = get_pipeline_logger("test_pipeline")
        logger2 = get_pipeline_logger("test_pipeline")
        logger3 = get_pipeline_logger("another_pipeline")

        # Same pipeline name should return same instance
        self.assertIs(logger1, logger2)

        # Different pipeline name should return different instance
        self.assertIsNot(logger1, logger3)

        # Both should be PipelineLogger instances
        self.assertIsInstance(logger1, PipelineLogger)
        self.assertIsInstance(logger3, PipelineLogger)


class TestSetupLogging(unittest.TestCase):
    """Test cases for the setup_logging function"""

    def test_setup_logging_structured(self):
        """Test setting up structured logging"""
        # This should not raise an exception
        setup_logging(log_level="INFO", log_format="structured")

    def test_setup_logging_standard(self):
        """Test setting up standard logging"""
        # This should not raise an exception
        setup_logging(log_level="INFO", log_format="standard")


if __name__ == '__main__':
    unittest.main()