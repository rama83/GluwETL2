"""
Python shell script for ingesting data into the Bronze layer.
This script reads data from a source, performs basic transformations using pandas,
and writes the data to the Bronze layer in S3.
"""
import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Union

import boto3
import pandas as pd

# Add the parent directory to the path to import the utility modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402
from utils.s3_utils import (  # noqa: E402
    get_bronze_bucket,
    get_bronze_prefix,
    read_csv,
    read_json,
    read_parquet,
    write_csv,
    write_json,
    write_parquet,
)

# Create a logger for this script
logger = get_logger(__name__)


def parse_args() -> Dict[str, str]:
    """
    Parse command line arguments.
    
    Returns:
        Dictionary of arguments
    """
    parser = argparse.ArgumentParser(description="Ingest data into the Bronze layer")
    parser.add_argument("--source-type", required=True, help="Type of source (csv, json, parquet)")
    parser.add_argument("--source-path", required=True, help="Path to the source data")
    parser.add_argument("--target-key", required=True, help="Key for the target data in S3")
    parser.add_argument("--file-format", required=True, help="Format to write the data in")
    parser.add_argument("--partition-cols", help="Columns to partition by (comma-separated)")
    
    args = parser.parse_args()
    
    return {
        "source_type": args.source_type,
        "source_path": args.source_path,
        "target_key": args.target_key,
        "file_format": args.file_format,
        "partition_cols": args.partition_cols.split(",") if args.partition_cols else [],
    }


def read_data(
    source_type: str,
    source_path: str,
    **kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """
    Read data from the source.
    
    Args:
        source_type: Type of source (csv, json, parquet)
        source_path: Path to the source data
        **kwargs: Additional options for reading the data
        
    Returns:
        DataFrame with the source data
    """
    if source_path.startswith("s3://"):
        # Extract bucket and key from S3 path
        s3_path = source_path[5:]  # Remove "s3://"
        bucket, key = s3_path.split("/", 1)
        
        if source_type == "csv":
            return read_csv(key, bucket, **kwargs)
        elif source_type == "json":
            return read_json(key, bucket, **kwargs)
        elif source_type == "parquet":
            return read_parquet(key, bucket, **kwargs)
        else:
            raise ValueError(f"Unsupported source type for S3: {source_type}")
    else:
        # Local file
        if source_type == "csv":
            return pd.read_csv(source_path, **kwargs)
        elif source_type == "json":
            return pd.read_json(source_path, **kwargs)
        elif source_type == "parquet":
            return pd.read_parquet(source_path, **kwargs)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations to the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Add metadata columns
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H-%M-%S")
    
    df["bronze_ingest_timestamp"] = datetime.now()
    df["bronze_ingest_date"] = current_date
    df["bronze_ingest_time"] = current_time
    
    # Add additional transformations here
    
    return df


def write_data(
    df: pd.DataFrame,
    target_key: str,
    file_format: str,
    partition_cols: Optional[List[str]] = None,
    **kwargs: Dict[str, Any],
) -> None:
    """
    Write data to the target in S3.
    
    Args:
        df: DataFrame to write
        target_key: Key to write the data to in S3
        file_format: Format to write the data in
        partition_cols: Columns to partition by
        **kwargs: Additional options for writing the data
    """
    bucket = get_bronze_bucket()
    
    if file_format == "csv":
        write_csv(df, target_key, bucket, **kwargs)
    elif file_format == "json":
        write_json(df, target_key, bucket, **kwargs)
    elif file_format == "parquet":
        write_parquet(df, target_key, bucket, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def main():
    """Main ETL function."""
    # Parse command line arguments
    args = parse_args()
    
    source_type = args["source_type"]
    source_path = args["source_path"]
    target_key = args["target_key"]
    file_format = args["file_format"]
    partition_cols = args["partition_cols"]
    
    # Log job parameters
    logger.info(
        "Starting data ingestion",
        source_type=source_type,
        source_path=source_path,
        target_key=target_key,
        file_format=file_format,
        partition_cols=partition_cols,
    )
    
    try:
        # Read data from source
        df = read_data(source_type, source_path)
        
        # Log data statistics
        logger.info(
            "Read data from source",
            rows=len(df),
            columns=list(df.columns),
        )
        
        # Transform data
        df = transform_data(df)
        
        # Write data to target
        write_data(df, target_key, file_format, partition_cols)
        
        # Log success
        logger.info(
            "Successfully wrote data to S3",
            bucket=get_bronze_bucket(),
            key=target_key,
            rows=len(df),
        )
    except Exception as e:
        # Log error
        logger.error(
            "Error in ETL job",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


if __name__ == "__main__":
    main()