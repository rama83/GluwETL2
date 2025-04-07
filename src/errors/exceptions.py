"""
Custom exception classes for the AWS Data Lake Framework.
Provides standardized exceptions for different types of errors.
"""
from typing import Any, Dict, Optional


class GlueETLError(Exception):
    """Base exception class for all Glue ETL errors."""
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            error_code: Optional error code for categorization
            details: Optional dictionary with additional error details
        """
        self.message = message
        self.error_code = error_code or "UNKNOWN_ERROR"
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the exception to a dictionary for logging or serialization.
        
        Returns:
            Dictionary representation of the exception
        """
        return {
            "error_type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
        }


# Configuration errors
class ConfigurationError(GlueETLError):
    """Exception raised for errors in the configuration."""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            config_key: The configuration key that caused the error
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if config_key:
            error_details["config_key"] = config_key
            
        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            details=error_details
        )


# AWS errors
class AWSError(GlueETLError):
    """Exception raised for errors in AWS service interactions."""
    
    def __init__(
        self,
        message: str,
        service: Optional[str] = None,
        operation: Optional[str] = None,
        aws_error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            service: The AWS service that raised the error (e.g., 's3', 'glue')
            operation: The operation that failed (e.g., 'get_object', 'start_job_run')
            aws_error_code: The AWS error code
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if service:
            error_details["aws_service"] = service
        if operation:
            error_details["aws_operation"] = operation
        if aws_error_code:
            error_details["aws_error_code"] = aws_error_code
            
        super().__init__(
            message=message,
            error_code=f"AWS_{service.upper()}_ERROR" if service else "AWS_ERROR",
            details=error_details
        )


# S3 specific errors
class S3Error(AWSError):
    """Exception raised for errors in S3 operations."""
    
    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        bucket: Optional[str] = None,
        key: Optional[str] = None,
        aws_error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            operation: The S3 operation that failed
            bucket: The S3 bucket name
            key: The S3 object key
            aws_error_code: The AWS error code
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if bucket:
            error_details["bucket"] = bucket
        if key:
            error_details["key"] = key
            
        super().__init__(
            message=message,
            service="s3",
            operation=operation,
            aws_error_code=aws_error_code,
            details=error_details
        )


# Glue specific errors
class GlueError(AWSError):
    """Exception raised for errors in Glue operations."""
    
    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        job_name: Optional[str] = None,
        job_run_id: Optional[str] = None,
        aws_error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            operation: The Glue operation that failed
            job_name: The Glue job name
            job_run_id: The Glue job run ID
            aws_error_code: The AWS error code
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if job_name:
            error_details["job_name"] = job_name
        if job_run_id:
            error_details["job_run_id"] = job_run_id
            
        super().__init__(
            message=message,
            service="glue",
            operation=operation,
            aws_error_code=aws_error_code,
            details=error_details
        )


# Data errors
class DataError(GlueETLError):
    """Exception raised for errors in data processing."""
    
    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        table: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            source: The data source (e.g., 's3', 'database')
            table: The table or dataset name
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if source:
            error_details["source"] = source
        if table:
            error_details["table"] = table
            
        super().__init__(
            message=message,
            error_code="DATA_ERROR",
            details=error_details
        )


# Validation errors
class ValidationError(DataError):
    """Exception raised for data validation errors."""
    
    def __init__(
        self,
        message: str,
        source: Optional[str] = None,
        table: Optional[str] = None,
        column: Optional[str] = None,
        validation_rule: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            source: The data source
            table: The table or dataset name
            column: The column that failed validation
            validation_rule: The validation rule that failed
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if column:
            error_details["column"] = column
        if validation_rule:
            error_details["validation_rule"] = validation_rule
            
        super().__init__(
            message=message,
            source=source,
            table=table,
            details=error_details
        )
        
        # Override the error code
        self.error_code = "VALIDATION_ERROR"


# Pipeline errors
class PipelineError(GlueETLError):
    """Exception raised for errors in the ETL pipeline."""
    
    def __init__(
        self,
        message: str,
        pipeline_name: Optional[str] = None,
        stage: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            pipeline_name: The name of the pipeline
            stage: The stage in the pipeline (e.g., 'extract', 'transform', 'load')
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if pipeline_name:
            error_details["pipeline_name"] = pipeline_name
        if stage:
            error_details["stage"] = stage
            
        super().__init__(
            message=message,
            error_code="PIPELINE_ERROR",
            details=error_details
        )


# Dependency errors
class DependencyError(GlueETLError):
    """Exception raised for errors in external dependencies."""
    
    def __init__(
        self,
        message: str,
        dependency: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            dependency: The name of the dependency
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if dependency:
            error_details["dependency"] = dependency
            
        super().__init__(
            message=message,
            error_code="DEPENDENCY_ERROR",
            details=error_details
        )


# Timeout errors
class TimeoutError(GlueETLError):
    """Exception raised for timeout errors."""
    
    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the exception.
        
        Args:
            message: Error message
            operation: The operation that timed out
            timeout_seconds: The timeout in seconds
            details: Optional dictionary with additional error details
        """
        error_details = details or {}
        if operation:
            error_details["operation"] = operation
        if timeout_seconds:
            error_details["timeout_seconds"] = timeout_seconds
            
        super().__init__(
            message=message,
            error_code="TIMEOUT_ERROR",
            details=error_details
        )