"""
AWS Glue utilities for the AWS Data Lake Framework.
Provides functions for working with AWS Glue ETL jobs.
"""
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3

# Import the configuration, logging, and error handling modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402
from errors import GlueError, handle_aws_error, retry  # noqa: E402

# Create a logger for this module
logger = get_logger(__name__)


def get_glue_client():
    """
    Get a Glue client with the configured AWS region.
    
    Returns:
        boto3 Glue client
    """
    return boto3.client("glue", region_name=config.get("aws.region"))


def get_default_job_args() -> Dict[str, str]:
    """
    Get the default job arguments for Glue jobs.
    
    Returns:
        Dictionary of default job arguments
    """
    return {
        "--job-language": "python",
        "--job-bookmark-option": config.get("aws.glue.job_bookmark", "job-bookmark-enable"),
        "--enable-metrics": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": f"s3://{config.get('s3.temp.bucket')}/{config.get('s3.temp.prefix')}spark-logs/",
    }


@handle_aws_error(service="glue")
def create_job(
    job_name: str,
    script_location: str,
    job_type: str = "glueetl",
    role: Optional[str] = None,
    description: Optional[str] = None,
    glue_version: Optional[str] = None,
    python_version: Optional[str] = None,
    max_capacity: Optional[float] = None,
    worker_type: Optional[str] = None,
    number_of_workers: Optional[int] = None,
    max_retries: Optional[int] = None,
    timeout: Optional[int] = None,
    default_arguments: Optional[Dict[str, str]] = None,
    connections: Optional[List[str]] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Create a Glue job.
    
    Args:
        job_name: Name of the job
        script_location: S3 location of the job script
        job_type: Type of job (glueetl or pythonshell)
        role: IAM role ARN
        description: Job description
        glue_version: Glue version
        python_version: Python version
        max_capacity: Maximum capacity (for pythonshell jobs)
        worker_type: Worker type (for glueetl jobs)
        number_of_workers: Number of workers (for glueetl jobs)
        max_retries: Maximum number of retries
        timeout: Timeout in minutes
        default_arguments: Default job arguments
        connections: List of connection names
        tags: Tags for the job
        
    Returns:
        Glue create_job response
    """
    glue = get_glue_client()
    
    # Set default values from configuration if not provided
    if role is None:
        role = config.get("aws.glue.role")
    
    if glue_version is None:
        glue_version = config.get("aws.glue.version", "5.0")
    
    if python_version is None:
        python_version = config.get("aws.glue.python_version", "3.9")
    
    if max_retries is None:
        max_retries = config.get("errors.max_retries", 3)
    
    if timeout is None:
        timeout = config.get("aws.glue.timeout_minutes", 60)
    
    # Prepare job arguments
    job_args = get_default_job_args()
    if default_arguments:
        job_args.update(default_arguments)
    
    # Prepare job parameters
    job_params = {
        "Name": job_name,
        "Role": role,
        "Command": {
            "Name": job_type,
            "ScriptLocation": script_location,
            "PythonVersion": python_version,
        },
        "DefaultArguments": job_args,
        "GlueVersion": glue_version,
        "MaxRetries": max_retries,
        "Timeout": timeout,
    }
    
    if description:
        job_params["Description"] = description
    
    if connections:
        job_params["Connections"] = {"Connections": connections}
    
    # Set worker configuration based on job type
    if job_type == "glueetl":
        if worker_type is None:
            worker_type = config.get("aws.glue.worker_type", "G.1X")
        
        if number_of_workers is None:
            number_of_workers = config.get("aws.glue.number_of_workers", 5)
        
        job_params["WorkerType"] = worker_type
        job_params["NumberOfWorkers"] = number_of_workers
    else:  # pythonshell
        if max_capacity is None:
            max_capacity = 0.0625  # Default for Python shell
        
        job_params["MaxCapacity"] = max_capacity
    
    # Add tags if provided
    if tags:
        job_params["Tags"] = tags
    
    # Create the job
    response = glue.create_job(**job_params)
    
    logger.info(
        "Glue job created",
        job_name=job_name,
        job_type=job_type,
        script_location=script_location,
    )
    
    return response


@handle_aws_error(service="glue")
def update_job(
    job_name: str,
    script_location: Optional[str] = None,
    role: Optional[str] = None,
    description: Optional[str] = None,
    glue_version: Optional[str] = None,
    python_version: Optional[str] = None,
    max_capacity: Optional[float] = None,
    worker_type: Optional[str] = None,
    number_of_workers: Optional[int] = None,
    max_retries: Optional[int] = None,
    timeout: Optional[int] = None,
    default_arguments: Optional[Dict[str, str]] = None,
    connections: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Update an existing Glue job.
    
    Args:
        job_name: Name of the job
        script_location: S3 location of the job script
        role: IAM role ARN
        description: Job description
        glue_version: Glue version
        python_version: Python version
        max_capacity: Maximum capacity (for pythonshell jobs)
        worker_type: Worker type (for glueetl jobs)
        number_of_workers: Number of workers (for glueetl jobs)
        max_retries: Maximum number of retries
        timeout: Timeout in minutes
        default_arguments: Default job arguments
        connections: List of connection names
        
    Returns:
        Glue update_job response
    """
    glue = get_glue_client()
    
    # Get the current job definition
    job_def = glue.get_job(JobName=job_name)["Job"]
    
    # Prepare job parameters
    job_params = {
        "JobName": job_name,
        "JobUpdate": {
            "Role": role or job_def["Role"],
            "Command": {
                "Name": job_def["Command"]["Name"],
                "ScriptLocation": script_location or job_def["Command"]["ScriptLocation"],
            },
            "DefaultArguments": default_arguments or job_def.get("DefaultArguments", {}),
            "MaxRetries": max_retries if max_retries is not None else job_def.get("MaxRetries", 3),
            "Timeout": timeout if timeout is not None else job_def.get("Timeout", 60),
        }
    }
    
    # Set optional parameters if provided
    if description:
        job_params["JobUpdate"]["Description"] = description
    
    if glue_version:
        job_params["JobUpdate"]["GlueVersion"] = glue_version
    
    if python_version:
        job_params["JobUpdate"]["Command"]["PythonVersion"] = python_version
    
    if connections:
        job_params["JobUpdate"]["Connections"] = {"Connections": connections}
    
    # Set worker configuration based on job type
    if job_def["Command"]["Name"] == "glueetl":
        if worker_type:
            job_params["JobUpdate"]["WorkerType"] = worker_type
        
        if number_of_workers:
            job_params["JobUpdate"]["NumberOfWorkers"] = number_of_workers
    else:  # pythonshell
        if max_capacity:
            job_params["JobUpdate"]["MaxCapacity"] = max_capacity
    
    # Update the job
    response = glue.update_job(**job_params)
    
    logger.info(
        "Glue job updated",
        job_name=job_name,
    )
    
    return response


@handle_aws_error(service="glue")
def delete_job(job_name: str) -> Dict[str, Any]:
    """
    Delete a Glue job.
    
    Args:
        job_name: Name of the job
        
    Returns:
        Glue delete_job response
    """
    glue = get_glue_client()
    
    response = glue.delete_job(JobName=job_name)
    
    logger.info(
        "Glue job deleted",
        job_name=job_name,
    )
    
    return response


@handle_aws_error(service="glue")
def start_job_run(
    job_name: str,
    arguments: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
) -> str:
    """
    Start a Glue job run.
    
    Args:
        job_name: Name of the job
        arguments: Job arguments
        timeout: Timeout in minutes
        
    Returns:
        Job run ID
    """
    glue = get_glue_client()
    
    # Prepare job run parameters
    job_run_params = {
        "JobName": job_name,
    }
    
    if arguments:
        job_run_params["Arguments"] = arguments
    
    if timeout:
        job_run_params["Timeout"] = timeout
    
    # Start the job run
    response = glue.start_job_run(**job_run_params)
    job_run_id = response["JobRunId"]
    
    logger.info(
        "Glue job run started",
        job_name=job_name,
        job_run_id=job_run_id,
    )
    
    return job_run_id


@handle_aws_error(service="glue")
def get_job_run(job_name: str, job_run_id: str) -> Dict[str, Any]:
    """
    Get information about a Glue job run.
    
    Args:
        job_name: Name of the job
        job_run_id: Job run ID
        
    Returns:
        Job run information
    """
    glue = get_glue_client()
    
    response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
    
    return response["JobRun"]


@handle_aws_error(service="glue")
def get_job_runs(job_name: str, max_results: int = 100) -> List[Dict[str, Any]]:
    """
    Get information about all runs of a Glue job.
    
    Args:
        job_name: Name of the job
        max_results: Maximum number of results to return
        
    Returns:
        List of job run information
    """
    glue = get_glue_client()
    
    paginator = glue.get_paginator("get_job_runs")
    job_runs = []
    
    for page in paginator.paginate(JobName=job_name, MaxResults=max_results):
        job_runs.extend(page["JobRuns"])
    
    return job_runs


@handle_aws_error(service="glue")
def wait_for_job_run(
    job_name: str,
    job_run_id: str,
    poll_interval: int = 30,
    timeout: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Wait for a Glue job run to complete.
    
    Args:
        job_name: Name of the job
        job_run_id: Job run ID
        poll_interval: Polling interval in seconds
        timeout: Timeout in seconds
        
    Returns:
        Final job run information
    """
    start_time = time.time()
    
    while True:
        # Check if timeout has been reached
        if timeout and time.time() - start_time > timeout:
            raise GlueError(
                message=f"Timeout waiting for job run {job_run_id} to complete",
                job_name=job_name,
                job_run_id=job_run_id,
            )
        
        # Get the job run status
        job_run = get_job_run(job_name, job_run_id)
        status = job_run["JobRunState"]
        
        # Log the status
        logger.info(
            "Glue job run status",
            job_name=job_name,
            job_run_id=job_run_id,
            status=status,
        )
        
        # Check if the job run has completed
        if status in ["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"]:
            return job_run
        
        # Wait before polling again
        time.sleep(poll_interval)


@handle_aws_error(service="glue")
def stop_job_run(job_name: str, job_run_id: str) -> Dict[str, Any]:
    """
    Stop a Glue job run.
    
    Args:
        job_name: Name of the job
        job_run_id: Job run ID
        
    Returns:
        Glue stop_job_run response
    """
    glue = get_glue_client()
    
    response = glue.stop_job_run(JobName=job_name, RunId=job_run_id)
    
    logger.info(
        "Glue job run stopped",
        job_name=job_name,
        job_run_id=job_run_id,
    )
    
    return response


@handle_aws_error(service="glue")
def reset_job_bookmark(job_name: str) -> Dict[str, Any]:
    """
    Reset the job bookmark for a Glue job.
    
    Args:
        job_name: Name of the job
        
    Returns:
        Glue reset_job_bookmark response
    """
    glue = get_glue_client()
    
    response = glue.reset_job_bookmark(JobName=job_name)
    
    logger.info(
        "Glue job bookmark reset",
        job_name=job_name,
    )
    
    return response


@handle_aws_error(service="glue")
def list_jobs(max_results: int = 100) -> List[Dict[str, Any]]:
    """
    List all Glue jobs.
    
    Args:
        max_results: Maximum number of results to return
        
    Returns:
        List of job information
    """
    glue = get_glue_client()
    
    paginator = glue.get_paginator("get_jobs")
    jobs = []
    
    for page in paginator.paginate(MaxResults=max_results):
        jobs.extend(page["Jobs"])
    
    return jobs


@handle_aws_error(service="glue")
def run_job_and_wait(
    job_name: str,
    arguments: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    poll_interval: int = 30,
) -> Dict[str, Any]:
    """
    Run a Glue job and wait for it to complete.
    
    Args:
        job_name: Name of the job
        arguments: Job arguments
        timeout: Timeout in seconds
        poll_interval: Polling interval in seconds
        
    Returns:
        Final job run information
    """
    # Start the job run
    job_run_id = start_job_run(job_name, arguments)
    
    # Wait for the job run to complete
    job_run = wait_for_job_run(job_name, job_run_id, poll_interval, timeout)
    
    # Check if the job run succeeded
    if job_run["JobRunState"] != "SUCCEEDED":
        error_message = f"Job run {job_run_id} failed with status {job_run['JobRunState']}"
        if "ErrorMessage" in job_run:
            error_message += f": {job_run['ErrorMessage']}"
        
        raise GlueError(
            message=error_message,
            job_name=job_name,
            job_run_id=job_run_id,
        )
    
    return job_run


@handle_aws_error(service="glue")
def get_job_metrics(job_name: str, job_run_id: str) -> Dict[str, Any]:
    """
    Get metrics for a Glue job run.
    
    Args:
        job_name: Name of the job
        job_run_id: Job run ID
        
    Returns:
        Dictionary of job metrics
    """
    # Get the job run information
    job_run = get_job_run(job_name, job_run_id)
    
    # Extract metrics
    metrics = {
        "job_name": job_name,
        "job_run_id": job_run_id,
        "status": job_run["JobRunState"],
        "start_time": job_run.get("StartedOn"),
        "end_time": job_run.get("CompletedOn"),
    }
    
    # Calculate duration if available
    if "StartedOn" in job_run and "CompletedOn" in job_run:
        duration = (job_run["CompletedOn"] - job_run["StartedOn"]).total_seconds()
        metrics["duration_seconds"] = duration
    
    # Add execution details if available
    if "ExecutionTime" in job_run:
        metrics["execution_time"] = job_run["ExecutionTime"]
    
    if "Attempt" in job_run:
        metrics["attempt"] = job_run["Attempt"]
    
    # Add error information if available
    if "ErrorMessage" in job_run:
        metrics["error_message"] = job_run["ErrorMessage"]
    
    # Add resource usage if available
    if "AllocatedCapacity" in job_run:
        metrics["allocated_capacity"] = job_run["AllocatedCapacity"]
    
    if "MaxCapacity" in job_run:
        metrics["max_capacity"] = job_run["MaxCapacity"]
    
    if "WorkerType" in job_run:
        metrics["worker_type"] = job_run["WorkerType"]
    
    if "NumberOfWorkers" in job_run:
        metrics["number_of_workers"] = job_run["NumberOfWorkers"]
    
    return metrics


@handle_aws_error(service="glue")
def upload_script(
    script_path: str,
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
) -> str:
    """
    Upload a Glue script to S3.
    
    Args:
        script_path: Local path to the script
        bucket: S3 bucket name (default: temp bucket)
        prefix: S3 prefix (default: temp prefix + scripts/)
        
    Returns:
        S3 location of the uploaded script
    """
    # Get the bucket and prefix
    bucket = bucket or config.get("s3.temp.bucket")
    prefix = prefix or f"{config.get('s3.temp.prefix')}scripts/"
    
    # Get the script name
    script_name = os.path.basename(script_path)
    
    # Upload the script to S3
    s3 = boto3.client("s3", region_name=config.get("aws.region"))
    
    with open(script_path, "rb") as f:
        s3.upload_fileobj(f, bucket, f"{prefix}{script_name}")
    
    # Return the S3 location
    s3_location = f"s3://{bucket}/{prefix}{script_name}"
    
    logger.info(
        "Glue script uploaded",
        script_path=script_path,
        s3_location=s3_location,
    )
    
    return s3_location


def create_spark_job(
    job_name: str,
    script_location: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a Glue Spark ETL job.
    
    Args:
        job_name: Name of the job
        script_location: S3 location of the job script
        **kwargs: Additional arguments for create_job
        
    Returns:
        Glue create_job response
    """
    return create_job(
        job_name=job_name,
        script_location=script_location,
        job_type="glueetl",
        **kwargs,
    )


def create_python_shell_job(
    job_name: str,
    script_location: str,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Create a Glue Python shell job.
    
    Args:
        job_name: Name of the job
        script_location: S3 location of the job script
        **kwargs: Additional arguments for create_job
        
    Returns:
        Glue create_job response
    """
    return create_job(
        job_name=job_name,
        script_location=script_location,
        job_type="pythonshell",
        **kwargs,
    )


def get_job_parameter(
    job_name: str,
    parameter_name: str,
    default_value: Optional[Any] = None,
) -> Any:
    """
    Get a parameter value for a Glue job.
    
    Args:
        job_name: Name of the job
        parameter_name: Name of the parameter
        default_value: Default value if the parameter is not found
        
    Returns:
        Parameter value
    """
    glue = get_glue_client()
    
    try:
        # Get the job definition
        job_def = glue.get_job(JobName=job_name)["Job"]
        
        # Get the parameter value from the job arguments
        if "DefaultArguments" in job_def:
            args = job_def["DefaultArguments"]
            
            # Check if the parameter exists
            if f"--{parameter_name}" in args:
                value = args[f"--{parameter_name}"]
                
                # Try to parse as JSON if it looks like a JSON string
                if value.startswith("{") or value.startswith("["):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return value
                
                return value
    except Exception as e:
        logger.warning(
            "Error getting job parameter",
            job_name=job_name,
            parameter_name=parameter_name,
            error=str(e),
        )
    
    return default_value


def set_job_parameter(
    job_name: str,
    parameter_name: str,
    parameter_value: Any,
) -> Dict[str, Any]:
    """
    Set a parameter value for a Glue job.
    
    Args:
        job_name: Name of the job
        parameter_name: Name of the parameter
        parameter_value: Value of the parameter
        
    Returns:
        Glue update_job response
    """
    glue = get_glue_client()
    
    # Get the current job definition
    job_def = glue.get_job(JobName=job_name)["Job"]
    
    # Get the current arguments
    args = job_def.get("DefaultArguments", {}).copy()
    
    # Convert the parameter value to a string if it's not already
    if not isinstance(parameter_value, str):
        if isinstance(parameter_value, (dict, list)):
            parameter_value = json.dumps(parameter_value)
        else:
            parameter_value = str(parameter_value)
    
    # Set the parameter value
    args[f"--{parameter_name}"] = parameter_value
    
    # Update the job
    return update_job(
        job_name=job_name,
        default_arguments=args,
    )