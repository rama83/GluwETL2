"""
S3 utilities for the AWS Data Lake Framework.
Provides functions for working with S3 buckets and objects in the Bronze layer.
"""
import io
import json
import os
from typing import Any, Dict, Iterator, List, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Import the configuration, logging, and error handling modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402
from errors import S3Error, handle_aws_error, retry  # noqa: E402

# Create a logger for this module
logger = get_logger(__name__)


def get_s3_client():
    """
    Get an S3 client with the configured AWS region.
    
    Returns:
        boto3 S3 client
    """
    return boto3.client("s3", region_name=config.get("aws.region"))


def get_s3_resource():
    """
    Get an S3 resource with the configured AWS region.
    
    Returns:
        boto3 S3 resource
    """
    return boto3.resource("s3", region_name=config.get("aws.region"))


def get_bronze_bucket() -> str:
    """
    Get the configured Bronze layer S3 bucket name.
    
    Returns:
        S3 bucket name for the Bronze layer
    """
    return config.get("s3.bronze.bucket")


def get_bronze_prefix() -> str:
    """
    Get the configured Bronze layer S3 prefix.
    
    Returns:
        S3 prefix for the Bronze layer
    """
    return config.get("s3.bronze.prefix", "raw/")


@handle_aws_error(service="s3")
def list_objects(
    bucket: Optional[str] = None,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    max_keys: int = 1000,
) -> List[Dict[str, Any]]:
    """
    List objects in an S3 bucket with the given prefix and suffix.
    
    Args:
        bucket: S3 bucket name (default: Bronze layer bucket)
        prefix: S3 prefix (default: Bronze layer prefix)
        suffix: Filter objects by suffix (e.g., '.csv')
        max_keys: Maximum number of keys to return
        
    Returns:
        List of object metadata dictionaries
    """
    bucket = bucket or get_bronze_bucket()
    prefix = prefix or get_bronze_prefix()
    
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys):
        if "Contents" in page:
            for obj in page["Contents"]:
                if suffix is None or obj["Key"].endswith(suffix):
                    objects.append(obj)
    
    return objects


@handle_aws_error(service="s3")
def object_exists(
    key: str,
    bucket: Optional[str] = None,
) -> bool:
    """
    Check if an object exists in S3.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        
    Returns:
        True if the object exists, False otherwise
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


@handle_aws_error(service="s3")
def read_object(
    key: str,
    bucket: Optional[str] = None,
) -> bytes:
    """
    Read an object from S3.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        
    Returns:
        Object content as bytes
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


@handle_aws_error(service="s3")
def write_object(
    key: str,
    data: Union[bytes, str, io.IOBase],
    bucket: Optional[str] = None,
    content_type: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Write an object to S3.
    
    Args:
        key: S3 object key
        data: Object content (bytes, string, or file-like object)
        bucket: S3 bucket name (default: Bronze layer bucket)
        content_type: Content type of the object
        metadata: Object metadata
        
    Returns:
        S3 put_object response
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    # Convert string to bytes if necessary
    if isinstance(data, str):
        data = data.encode("utf-8")
    
    # Prepare arguments for put_object
    args = {
        "Bucket": bucket,
        "Key": key,
        "Body": data,
    }
    
    if content_type:
        args["ContentType"] = content_type
    
    if metadata:
        args["Metadata"] = metadata
    
    response = s3.put_object(**args)
    
    logger.info(
        "Object written to S3",
        bucket=bucket,
        key=key,
        size=len(data) if isinstance(data, bytes) else "unknown",
    )
    
    return response


@handle_aws_error(service="s3")
def delete_object(
    key: str,
    bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Delete an object from S3.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        
    Returns:
        S3 delete_object response
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    response = s3.delete_object(Bucket=bucket, Key=key)
    
    logger.info(
        "Object deleted from S3",
        bucket=bucket,
        key=key,
    )
    
    return response


@handle_aws_error(service="s3")
def copy_object(
    source_key: str,
    dest_key: str,
    source_bucket: Optional[str] = None,
    dest_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Copy an object within S3.
    
    Args:
        source_key: Source S3 object key
        dest_key: Destination S3 object key
        source_bucket: Source S3 bucket name (default: Bronze layer bucket)
        dest_bucket: Destination S3 bucket name (default: source_bucket)
        
    Returns:
        S3 copy_object response
    """
    source_bucket = source_bucket or get_bronze_bucket()
    dest_bucket = dest_bucket or source_bucket
    s3 = get_s3_client()
    
    response = s3.copy_object(
        CopySource={"Bucket": source_bucket, "Key": source_key},
        Bucket=dest_bucket,
        Key=dest_key,
    )
    
    logger.info(
        "Object copied in S3",
        source_bucket=source_bucket,
        source_key=source_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )
    
    return response


@handle_aws_error(service="s3")
def move_object(
    source_key: str,
    dest_key: str,
    source_bucket: Optional[str] = None,
    dest_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Move an object within S3 (copy and delete).
    
    Args:
        source_key: Source S3 object key
        dest_key: Destination S3 object key
        source_bucket: Source S3 bucket name (default: Bronze layer bucket)
        dest_bucket: Destination S3 bucket name (default: source_bucket)
        
    Returns:
        S3 copy_object response
    """
    copy_response = copy_object(
        source_key=source_key,
        dest_key=dest_key,
        source_bucket=source_bucket,
        dest_bucket=dest_bucket,
    )
    
    delete_object(
        key=source_key,
        bucket=source_bucket,
    )
    
    return copy_response


@handle_aws_error(service="s3")
def list_prefixes(
    prefix: Optional[str] = None,
    delimiter: str = "/",
    bucket: Optional[str] = None,
) -> List[str]:
    """
    List prefixes (directories) in an S3 bucket.
    
    Args:
        prefix: S3 prefix (default: Bronze layer prefix)
        delimiter: Delimiter for prefixes
        bucket: S3 bucket name (default: Bronze layer bucket)
        
    Returns:
        List of prefixes
    """
    bucket = bucket or get_bronze_bucket()
    prefix = prefix or get_bronze_prefix()
    s3 = get_s3_client()
    
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter):
        if "CommonPrefixes" in page:
            for common_prefix in page["CommonPrefixes"]:
                prefixes.append(common_prefix["Prefix"])
    
    return prefixes


@handle_aws_error(service="s3")
def read_csv(
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> pd.DataFrame:
    """
    Read a CSV file from S3 into a pandas DataFrame.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for pandas.read_csv
        
    Returns:
        pandas DataFrame
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"], **pandas_kwargs)


@handle_aws_error(service="s3")
def write_csv(
    df: pd.DataFrame,
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Dict[str, Any]:
    """
    Write a pandas DataFrame to a CSV file in S3.
    
    Args:
        df: pandas DataFrame
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for DataFrame.to_csv
        
    Returns:
        S3 put_object response
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    # Write DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, **pandas_kwargs)
    
    # Upload to S3
    response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    
    logger.info(
        "CSV written to S3",
        bucket=bucket,
        key=key,
        rows=len(df),
        columns=list(df.columns),
    )
    
    return response


@handle_aws_error(service="s3")
def read_json(
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Read a JSON file from S3.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for pandas.read_json
        
    Returns:
        pandas DataFrame or dictionary
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    
    # If pandas_kwargs are provided, use pandas.read_json
    if pandas_kwargs:
        return pd.read_json(content, **pandas_kwargs)
    
    # Otherwise, return a dictionary
    return json.loads(content)


@handle_aws_error(service="s3")
def write_json(
    data: Union[pd.DataFrame, Dict[str, Any], List[Any]],
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Dict[str, Any]:
    """
    Write data to a JSON file in S3.
    
    Args:
        data: Data to write (DataFrame, dictionary, or list)
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for DataFrame.to_json
        
    Returns:
        S3 put_object response
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    # Convert data to JSON
    if isinstance(data, pd.DataFrame):
        json_data = data.to_json(**pandas_kwargs)
    else:
        json_data = json.dumps(data)
    
    # Upload to S3
    response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_data,
        ContentType="application/json",
    )
    
    logger.info(
        "JSON written to S3",
        bucket=bucket,
        key=key,
    )
    
    return response


@handle_aws_error(service="s3")
def read_parquet(
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> pd.DataFrame:
    """
    Read a Parquet file from S3 into a pandas DataFrame.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for pandas.read_parquet
        
    Returns:
        pandas DataFrame
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), **pandas_kwargs)


@handle_aws_error(service="s3")
def write_parquet(
    df: pd.DataFrame,
    key: str,
    bucket: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Dict[str, Any]:
    """
    Write a pandas DataFrame to a Parquet file in S3.
    
    Args:
        df: pandas DataFrame
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        **pandas_kwargs: Additional arguments for DataFrame.to_parquet
        
    Returns:
        S3 put_object response
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    # Write DataFrame to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, **pandas_kwargs)
    parquet_buffer.seek(0)
    
    # Upload to S3
    response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    
    logger.info(
        "Parquet written to S3",
        bucket=bucket,
        key=key,
        rows=len(df),
        columns=list(df.columns),
    )
    
    return response


@retry(max_retries=3)
@handle_aws_error(service="s3")
def create_bucket(
    bucket: str,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an S3 bucket.
    
    Args:
        bucket: S3 bucket name
        region: AWS region (default: configured region)
        
    Returns:
        S3 create_bucket response
    """
    region = region or config.get("aws.region")
    s3 = get_s3_client()
    
    # Create the bucket
    if region == "us-east-1":
        response = s3.create_bucket(Bucket=bucket)
    else:
        response = s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    
    logger.info(
        "S3 bucket created",
        bucket=bucket,
        region=region,
    )
    
    return response


@handle_aws_error(service="s3")
def get_bucket_location(
    bucket: Optional[str] = None,
) -> str:
    """
    Get the location (region) of an S3 bucket.
    
    Args:
        bucket: S3 bucket name (default: Bronze layer bucket)
        
    Returns:
        AWS region
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    response = s3.get_bucket_location(Bucket=bucket)
    location = response.get("LocationConstraint")
    
    # AWS returns None for us-east-1
    if location is None:
        location = "us-east-1"
    
    return location


@handle_aws_error(service="s3")
def generate_presigned_url(
    key: str,
    bucket: Optional[str] = None,
    expiration: int = 3600,
) -> str:
    """
    Generate a presigned URL for an S3 object.
    
    Args:
        key: S3 object key
        bucket: S3 bucket name (default: Bronze layer bucket)
        expiration: URL expiration time in seconds
        
    Returns:
        Presigned URL
    """
    bucket = bucket or get_bronze_bucket()
    s3 = get_s3_client()
    
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expiration,
    )
    
    return url