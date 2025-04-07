"""
Error handling module for the AWS Data Lake Framework.
Provides custom exceptions and error handling utilities.
"""
from .exceptions import (
    AWSError,
    ConfigurationError,
    DataError,
    DependencyError,
    GlueError,
    GlueETLError,
    PipelineError,
    S3Error,
    TimeoutError,
    ValidationError,
)
from .handlers import (
    alert_on_failure,
    handle_aws_error,
    log_error,
    retry,
    safe_execute,
)

__all__ = [
    # Exceptions
    "AWSError",
    "ConfigurationError",
    "DataError",
    "DependencyError",
    "GlueError",
    "GlueETLError",
    "PipelineError",
    "S3Error",
    "TimeoutError",
    "ValidationError",
    
    # Handlers
    "alert_on_failure",
    "handle_aws_error",
    "log_error",
    "retry",
    "safe_execute",
]