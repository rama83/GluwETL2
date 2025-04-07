"""
Error handling utilities for the AWS Data Lake Framework.
Provides retry mechanisms, error logging, and decorators for error handling.
"""
import functools
import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, cast

import boto3
from botocore.exceptions import ClientError

# Import the configuration and logging modules
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402

# Import custom exceptions
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

# Type variables for function signatures
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

# Create a logger for this module
logger = get_logger(__name__)


def log_error(
    error: Exception,
    level: str = "error",
    include_traceback: bool = True,
    additional_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log an error with context information.
    
    Args:
        error: The exception to log
        level: Log level (debug, info, warning, error, critical)
        include_traceback: Whether to include the traceback in the log
        additional_context: Additional context information to include in the log
    """
    context = additional_context or {}
    
    # Add exception details to the context
    context["error_type"] = error.__class__.__name__
    context["error_message"] = str(error)
    
    # Add traceback if requested
    if include_traceback:
        context["traceback"] = traceback.format_exc()
    
    # Add structured error information if available
    if isinstance(error, GlueETLError):
        context.update(error.to_dict())
    
    # Log the error with the appropriate level
    log_method = getattr(logger, level)
    log_method("Error occurred", **context)


def retry(
    max_retries: Optional[int] = None,
    retry_delay: Optional[float] = None,
    retry_backoff: float = 2.0,
    retry_exceptions: Optional[List[Type[Exception]]] = None,
    retry_on_result: Optional[Callable[[Any], bool]] = None,
) -> Callable[[F], F]:
    """
    Decorator for retrying a function on specified exceptions or results.
    
    Args:
        max_retries: Maximum number of retries (default from config)
        retry_delay: Initial delay between retries in seconds (default from config)
        retry_backoff: Backoff multiplier for the delay
        retry_exceptions: List of exceptions to retry on
        retry_on_result: Function that takes the result and returns True if retry is needed
        
    Returns:
        Decorated function
    """
    # Get defaults from configuration if not specified
    if max_retries is None:
        max_retries = config.get("errors.max_retries", 3)
    
    if retry_delay is None:
        retry_delay = config.get("errors.retry_delay_seconds", 5)
    
    # Default exceptions to retry on
    if retry_exceptions is None:
        retry_exceptions = [
            ClientError,  # Boto3 client errors
            TimeoutError,  # Custom timeout errors
            ConnectionError,  # Network errors
        ]
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            retries = 0
            current_delay = retry_delay
            
            while True:
                try:
                    result = func(*args, **kwargs)
                    
                    # Check if we should retry based on the result
                    if retry_on_result and retry_on_result(result):
                        if retries >= max_retries:
                            logger.warning(
                                "Max retries reached with unsuccessful result",
                                function=func.__name__,
                                retries=retries,
                                result=result,
                            )
                            return result
                        
                        logger.info(
                            "Retrying due to unsuccessful result",
                            function=func.__name__,
                            attempt=retries + 1,
                            max_retries=max_retries,
                            delay=current_delay,
                        )
                    else:
                        return result
                
                except tuple(retry_exceptions) as e:
                    if retries >= max_retries:
                        logger.error(
                            "Max retries reached",
                            function=func.__name__,
                            retries=retries,
                            error=str(e),
                            error_type=e.__class__.__name__,
                        )
                        raise
                    
                    logger.warning(
                        "Retrying after exception",
                        function=func.__name__,
                        attempt=retries + 1,
                        max_retries=max_retries,
                        delay=current_delay,
                        error=str(e),
                        error_type=e.__class__.__name__,
                    )
                
                # Wait before retrying
                time.sleep(current_delay)
                retries += 1
                current_delay *= retry_backoff
        
        return cast(F, wrapper)
    
    return decorator


def handle_aws_error(
    func: Optional[F] = None,
    *,
    service: Optional[str] = None,
    operation: Optional[str] = None,
    reraise: bool = True,
) -> Union[Callable[[F], F], F]:
    """
    Decorator for handling AWS errors and converting them to custom exceptions.
    
    Args:
        func: The function to decorate
        service: The AWS service name
        operation: The operation name
        reraise: Whether to reraise the custom exception
        
    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except ClientError as e:
                # Extract AWS error information
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                error_message = e.response.get("Error", {}).get("Message", str(e))
                
                # Determine the service and operation if not provided
                service_name = service
                operation_name = operation
                
                if not service_name and hasattr(e, "operation_name"):
                    service_name = getattr(e, "service_name", None)
                
                if not operation_name and hasattr(e, "operation_name"):
                    operation_name = getattr(e, "operation_name", None)
                
                # Create the appropriate custom exception
                if service_name == "s3":
                    custom_error = S3Error(
                        message=error_message,
                        operation=operation_name,
                        aws_error_code=error_code,
                    )
                elif service_name == "glue":
                    custom_error = GlueError(
                        message=error_message,
                        operation=operation_name,
                        aws_error_code=error_code,
                    )
                else:
                    custom_error = AWSError(
                        message=error_message,
                        service=service_name,
                        operation=operation_name,
                        aws_error_code=error_code,
                    )
                
                # Log the error
                log_error(custom_error)
                
                # Reraise the custom exception if requested
                if reraise:
                    raise custom_error from e
                
                return None
        
        return cast(F, wrapper)
    
    # Allow the decorator to be used with or without arguments
    if func is None:
        return decorator
    
    return decorator(func)


def alert_on_failure(
    func: Optional[F] = None,
    *,
    alert_topic_arn: Optional[str] = None,
    include_traceback: bool = True,
) -> Union[Callable[[F], F], F]:
    """
    Decorator for sending an alert when a function fails.
    
    Args:
        func: The function to decorate
        alert_topic_arn: The SNS topic ARN to send the alert to
        include_traceback: Whether to include the traceback in the alert
        
    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Log the error
                log_error(e, include_traceback=include_traceback)
                
                # Send an alert if configured
                if config.get("errors.alert_on_failure", False):
                    topic_arn = alert_topic_arn or config.get("errors.sns_topic_arn")
                    
                    if topic_arn:
                        try:
                            # Create a message for the alert
                            message = {
                                "error_type": e.__class__.__name__,
                                "error_message": str(e),
                                "function": func.__name__,
                                "timestamp": time.time(),
                            }
                            
                            if include_traceback:
                                message["traceback"] = traceback.format_exc()
                            
                            # Add structured error information if available
                            if isinstance(e, GlueETLError):
                                message.update(e.to_dict())
                            
                            # Send the alert
                            sns = boto3.client("sns", region_name=config.get("aws.region"))
                            sns.publish(
                                TopicArn=topic_arn,
                                Subject=f"Error in {func.__name__}",
                                Message=str(message),
                            )
                            
                            logger.info(
                                "Alert sent",
                                topic_arn=topic_arn,
                                error_type=e.__class__.__name__,
                            )
                        except Exception as alert_error:
                            logger.error(
                                "Failed to send alert",
                                error=str(alert_error),
                                original_error=str(e),
                            )
                
                # Reraise the original exception
                raise
        
        return cast(F, wrapper)
    
    # Allow the decorator to be used with or without arguments
    if func is None:
        return decorator
    
    return decorator(func)


def safe_execute(
    func: Callable[..., T],
    *args: Any,
    default_value: Optional[T] = None,
    log_level: str = "error",
    **kwargs: Any,
) -> Optional[T]:
    """
    Execute a function safely, catching any exceptions.
    
    Args:
        func: The function to execute
        *args: Positional arguments for the function
        default_value: Value to return if an exception occurs
        log_level: Log level for exceptions
        **kwargs: Keyword arguments for the function
        
    Returns:
        The function result or the default value if an exception occurs
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        log_error(e, level=log_level)
        return default_value