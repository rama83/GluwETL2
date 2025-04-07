"""
Logging module for the AWS Data Lake Framework.
Provides structured logging with context information.
"""
from .logger import (
    configure_logging,
    get_logger,
    logger,
    CloudWatchHandler,
    setup_cloudwatch_logging,
    setup_file_logging,
)

__all__ = [
    "configure_logging",
    "get_logger",
    "logger",
    "CloudWatchHandler",
    "setup_cloudwatch_logging",
    "setup_file_logging",
]