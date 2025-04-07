"""
Structured logging module for the AWS Data Lake Framework.
Provides consistent logging across all components with context information.
"""
import logging
import os
import sys
from typing import Any, Dict, Optional, Union

import structlog
from structlog.stdlib import BoundLogger

# Import the configuration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402


def configure_logging() -> None:
    """Configure the logging system based on the current configuration."""
    log_level_name = config.get("logging.level", "INFO")
    log_level = getattr(logging, log_level_name)
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        level=log_level,
        stream=sys.stdout,
    )
    
    # Configure structlog
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    # Add output formatter based on configuration
    log_format = config.get("logging.format", "json")
    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_context: Any) -> BoundLogger:
    """
    Get a structured logger with the given name and initial context.
    
    Args:
        name: The name of the logger
        **initial_context: Initial context values to bind to the logger
        
    Returns:
        A structured logger instance
    """
    # Add environment information to the context
    context = {
        "environment": config.get("environment", "development"),
        "service": "glue-etl",
    }
    context.update(initial_context)
    
    return structlog.get_logger(name).bind(**context)


class CloudWatchHandler(logging.Handler):
    """Logging handler that sends logs to AWS CloudWatch."""
    
    def __init__(self, log_group: str, log_stream: Optional[str] = None):
        """
        Initialize the CloudWatch logging handler.
        
        Args:
            log_group: CloudWatch log group name
            log_stream: CloudWatch log stream name (defaults to job name or timestamp)
        """
        super().__init__()
        self.log_group = log_group
        self.log_stream = log_stream or f"{config.get('aws.glue.job_name', 'local')}-{structlog.processors.TimeStamper(fmt='%Y-%m-%d-%H-%M-%S')}"
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of the CloudWatch Logs client."""
        if self._client is None:
            import boto3
            self._client = boto3.client("logs", region_name=config.get("aws.region"))
            
            # Create log group and stream if they don't exist
            try:
                self._client.create_log_group(logGroupName=self.log_group)
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass
                
            try:
                self._client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream
                )
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass
                
        return self._client
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a log record to CloudWatch.
        
        Args:
            record: The log record to emit
        """
        try:
            log_message = self.format(record)
            timestamp = int(record.created * 1000)
            
            self.client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[
                    {
                        "timestamp": timestamp,
                        "message": log_message
                    }
                ]
            )
        except Exception:
            self.handleError(record)


def setup_cloudwatch_logging() -> None:
    """Set up CloudWatch logging if configured."""
    if config.get("logging.destination") == "cloudwatch":
        log_group = config.get("logging.cloudwatch.log_group", "/aws/glue/jobs")
        handler = CloudWatchHandler(log_group=log_group)
        
        # Set the formatter
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        
        # Add the handler to the root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)


def setup_file_logging() -> None:
    """Set up file logging if configured."""
    if config.get("logging.destination") == "file":
        from logging.handlers import RotatingFileHandler
        
        log_dir = config.get("logging.local.log_dir", "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, "glue_etl.log")
        max_size = config.get("logging.local.max_size_mb", 10) * 1024 * 1024
        backup_count = config.get("logging.local.backup_count", 5)
        
        handler = RotatingFileHandler(
            log_file,
            maxBytes=max_size,
            backupCount=backup_count
        )
        
        # Set the formatter
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        
        # Add the handler to the root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)


# Initialize logging
configure_logging()

# Set up additional handlers based on configuration
if not config.get("local_dev.mock_aws", True):
    setup_cloudwatch_logging()

if config.get("logging.destination") == "file":
    setup_file_logging()

# Create a default logger
logger = get_logger(__name__)