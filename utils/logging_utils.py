"""
Structured Logging Module

This module provides structured logging capabilities for the NYC TLC Data Platform
with support for different log formats and context enrichment.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from enum import Enum


class LogLevel(Enum):
    """Enumeration of supported log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class StructuredLogger:
    """
    A structured logger that outputs JSON-formatted logs with consistent schema
    """
    
    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(message)s')  # JSON formatter will handle formatting
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _log(self, level: LogLevel, message: str, **context):
        """
        Internal method to create structured log entries
        
        Args:
            level: Log level
            message: Log message
            **context: Additional context fields
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level.value,
            "message": message,
            "service": "nyc-tlc-data-platform",
            "context": context
        }
        
        # Add caller information if available
        import inspect
        frame = inspect.currentframe().f_back
        if frame:
            log_entry["caller"] = {
                "file": frame.f_code.co_filename.split('/')[-1],
                "function": frame.f_code.co_name,
                "line": frame.f_lineno
            }
        
        self.logger.log(getattr(logging, level.value), json.dumps(log_entry))
    
    def debug(self, message: str, **context):
        """Log a debug message with structured context"""
        self._log(LogLevel.DEBUG, message, **context)
    
    def info(self, message: str, **context):
        """Log an info message with structured context"""
        self._log(LogLevel.INFO, message, **context)
    
    def warning(self, message: str, **context):
        """Log a warning message with structured context"""
        self._log(LogLevel.WARNING, message, **context)
    
    def error(self, message: str, **context):
        """Log an error message with structured context"""
        self._log(LogLevel.ERROR, message, **context)
    
    def critical(self, message: str, **context):
        """Log a critical message with structured context"""
        self._log(LogLevel.CRITICAL, message, **context)


class PipelineLogger:
    """
    Specialized logger for data pipeline operations with pipeline-specific context
    """
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.logger = StructuredLogger(f"pipeline.{pipeline_name}")
    
    def log_pipeline_start(self, run_id: str, **context):
        """Log the start of a pipeline run"""
        self.logger.info(
            f"Pipeline {self.pipeline_name} started",
            pipeline_name=self.pipeline_name,
            run_id=run_id,
            event_type="pipeline_start",
            **context
        )
    
    def log_pipeline_end(self, run_id: str, status: str, duration: float, **context):
        """Log the end of a pipeline run"""
        self.logger.info(
            f"Pipeline {self.pipeline_name} completed with status {status}",
            pipeline_name=self.pipeline_name,
            run_id=run_id,
            event_type="pipeline_end",
            status=status,
            duration_seconds=duration,
            **context
        )
    
    def log_task_start(self, task_id: str, run_id: str, **context):
        """Log the start of a task"""
        self.logger.info(
            f"Task {task_id} started",
            pipeline_name=self.pipeline_name,
            task_id=task_id,
            run_id=run_id,
            event_type="task_start",
            **context
        )
    
    def log_task_end(self, task_id: str, run_id: str, status: str, duration: float, **context):
        """Log the end of a task"""
        self.logger.info(
            f"Task {task_id} completed with status {status}",
            pipeline_name=self.pipeline_name,
            task_id=task_id,
            run_id=run_id,
            event_type="task_end",
            status=status,
            duration_seconds=duration,
            **context
        )
    
    def log_data_processing(self, operation: str, record_count: int, **context):
        """Log data processing operations"""
        self.logger.info(
            f"Data processing: {operation}",
            pipeline_name=self.pipeline_name,
            operation=operation,
            record_count=record_count,
            event_type="data_processing",
            **context
        )
    
    def log_data_validation(self, validation_name: str, passed: bool, details: Dict[str, Any] = None, **context):
        """Log data validation results"""
        self.logger.info(
            f"Data validation {validation_name} {'passed' if passed else 'failed'}",
            pipeline_name=self.pipeline_name,
            validation_name=validation_name,
            passed=passed,
            details=details,
            event_type="data_validation",
            **context
        )
    
    def log_error(self, error_type: str, error_message: str, task_id: Optional[str] = None, **context):
        """Log an error with structured context"""
        self.logger.error(
            f"Error occurred: {error_message}",
            pipeline_name=self.pipeline_name,
            error_type=error_type,
            error_message=error_message,
            task_id=task_id,
            event_type="error",
            **context
        )
    
    def log_warning(self, warning_message: str, task_id: Optional[str] = None, **context):
        """Log a warning with structured context"""
        self.logger.warning(
            f"Warning: {warning_message}",
            pipeline_name=self.pipeline_name,
            warning_message=warning_message,
            task_id=task_id,
            event_type="warning",
            **context
        )


# Global logger instances
_pipeline_loggers: Dict[str, PipelineLogger] = {}


def get_pipeline_logger(pipeline_name: str) -> PipelineLogger:
    """
    Get or create a pipeline logger instance
    
    Args:
        pipeline_name: Name of the pipeline
        
    Returns:
        PipelineLogger instance
    """
    if pipeline_name not in _pipeline_loggers:
        _pipeline_loggers[pipeline_name] = PipelineLogger(pipeline_name)
    return _pipeline_loggers[pipeline_name]


def setup_logging(log_level: str = "INFO", log_format: str = "structured"):
    """
    Setup logging for the application
    
    Args:
        log_level: Minimum log level to output
        log_format: Format of logs ('structured' or 'standard')
    """
    if log_format == "structured":
        # Already configured for structured logging
        pass
    else:
        # Standard Python logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )


# Example usage
if __name__ == "__main__":
    # Example of how to use the structured logger
    logger = get_pipeline_logger("test_pipeline")
    
    logger.log_pipeline_start("run_123", user="data_engineer")
    logger.log_task_start("extract_data", "run_123")
    logger.log_data_processing("extract", 1000)
    logger.log_task_end("extract_data", "run_123", "SUCCESS", 10.5)
    logger.log_pipeline_end("run_123", "SUCCESS", 45.2)
    
    # Example of error logging
    logger.log_error("ConnectionError", "Failed to connect to S3", "extract_data", retry_count=3)