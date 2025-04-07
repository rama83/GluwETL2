"""
Test script for local development and testing of ETL pipelines.
This script sets up a local environment, generates sample data,
runs the ETL scripts locally, and validates the results.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_s3

# Add the parent directory to the path to import the utility modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402

# Create a logger for this script
logger = get_logger(__name__)


def parse_args() -> Dict[str, str]:
    """
    Parse command line arguments.
    
    Returns:
        Dictionary of arguments
    """
    parser = argparse.ArgumentParser(description="Test ETL pipelines locally")
    parser.add_argument(
        "--pipeline",
        required=True,
        choices=["bronze-python", "bronze-spark", "silver-spark"],
        help="Pipeline to test",
    )
    parser.add_argument(
        "--data-size",
        type=int,
        default=1000,
        help="Number of rows in the sample data",
    )
    parser.add_argument(
        "--output-dir",
        default="local_dev/output",
        help="Directory to store the output data",
    )
    
    args = parser.parse_args()
    
    return {
        "pipeline": args.pipeline,
        "data_size": args.data_size,
        "output_dir": args.output_dir,
    }


def generate_sample_data(size: int = 1000) -> pd.DataFrame:
    """
    Generate sample data for testing.
    
    Args:
        size: Number of rows in the sample data
        
    Returns:
        DataFrame with sample data
    """
    # Generate sample data
    data = {
        "id": range(1, size + 1),
        "name": [f"Name {i}" for i in range(1, size + 1)],
        "value": [i * 10 for i in range(1, size + 1)],
        "date": [datetime.now().date() for _ in range(size)],
        "timestamp": [datetime.now() for _ in range(size)],
    }
    
    # Create a DataFrame
    df = pd.DataFrame(data)
    
    return df


def setup_local_environment(output_dir: str) -> None:
    """
    Set up the local environment for testing.
    
    Args:
        output_dir: Directory to store the output data
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create subdirectories for bronze and silver layers
    os.makedirs(os.path.join(output_dir, "bronze"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "silver"), exist_ok=True)
    
    logger.info(
        "Local environment set up",
        output_dir=output_dir,
    )


def setup_mock_s3() -> None:
    """
    Set up mock S3 for local testing.
    
    Returns:
        Mock S3 context
    """
    # Create the mock S3 buckets
    s3 = boto3.client("s3", region_name="us-east-1")
    
    # Create the bronze bucket
    bronze_bucket = config.get("s3.bronze.bucket")
    s3.create_bucket(Bucket=bronze_bucket)
    
    # Create the silver bucket
    silver_bucket = config.get("s3.silver.bucket")
    s3.create_bucket(Bucket=silver_bucket)
    
    # Create the temp bucket
    temp_bucket = config.get("s3.temp.bucket")
    s3.create_bucket(Bucket=temp_bucket)
    
    logger.info(
        "Mock S3 buckets created",
        bronze_bucket=bronze_bucket,
        silver_bucket=silver_bucket,
        temp_bucket=temp_bucket,
    )


def write_sample_data(
    df: pd.DataFrame,
    output_dir: str,
    file_format: str = "csv",
) -> str:
    """
    Write sample data to a file.
    
    Args:
        df: DataFrame with sample data
        output_dir: Directory to store the output data
        file_format: Format to write the data in
        
    Returns:
        Path to the sample data file
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(
        suffix=f".{file_format}",
        dir=output_dir,
        delete=False,
    ) as f:
        file_path = f.name
    
    # Write the data to the file
    if file_format == "csv":
        df.to_csv(file_path, index=False)
    elif file_format == "json":
        df.to_json(file_path, orient="records")
    elif file_format == "parquet":
        df.to_parquet(file_path, index=False)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    
    logger.info(
        "Sample data written",
        file_path=file_path,
        file_format=file_format,
        rows=len(df),
    )
    
    return file_path


def run_bronze_python_pipeline(
    sample_data_path: str,
    output_dir: str,
    file_format: str = "csv",
) -> None:
    """
    Run the Bronze layer Python pipeline.
    
    Args:
        sample_data_path: Path to the sample data file
        output_dir: Directory to store the output data
        file_format: Format of the sample data
    """
    # Set up the command
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src/bronze/python_ingest.py",
    )
    
    target_key = f"bronze/test_data.{file_format}"
    
    command = [
        "python",
        script_path,
        "--source-type", file_format,
        "--source-path", sample_data_path,
        "--target-key", target_key,
        "--file-format", file_format,
    ]
    
    # Run the command
    logger.info(
        "Running Bronze layer Python pipeline",
        command=" ".join(command),
    )
    
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(
            "Bronze layer Python pipeline failed",
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        raise RuntimeError(f"Bronze layer Python pipeline failed: {result.stderr}")
    
    logger.info(
        "Bronze layer Python pipeline completed successfully",
        stdout=result.stdout,
    )


def run_bronze_spark_pipeline(
    sample_data_path: str,
    output_dir: str,
    file_format: str = "csv",
) -> None:
    """
    Run the Bronze layer Spark pipeline.
    
    Args:
        sample_data_path: Path to the sample data file
        output_dir: Directory to store the output data
        file_format: Format of the sample data
    """
    # Set up the command
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src/bronze/spark_ingest.py",
    )
    
    target_path = os.path.join(output_dir, "bronze")
    
    # Create a temporary file with job arguments
    job_args = {
        "JOB_NAME": "test-bronze-spark",
        "source_type": file_format,
        "source_path": sample_data_path,
        "target_path": target_path,
        "file_format": file_format,
        "partition_cols": "",
    }
    
    job_args_path = os.path.join(output_dir, "job_args.json")
    with open(job_args_path, "w") as f:
        json.dump(job_args, f)
    
    # Set up the Spark submit command
    command = [
        "spark-submit",
        "--master", "local[*]",
        "--deploy-mode", "client",
        script_path,
        "--JOB_NAME", "test-bronze-spark",
        "--source_type", file_format,
        "--source_path", sample_data_path,
        "--target_path", target_path,
        "--file_format", file_format,
    ]
    
    # Run the command
    logger.info(
        "Running Bronze layer Spark pipeline",
        command=" ".join(command),
    )
    
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(
                "Bronze layer Spark pipeline failed",
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
            raise RuntimeError(f"Bronze layer Spark pipeline failed: {result.stderr}")
        
        logger.info(
            "Bronze layer Spark pipeline completed successfully",
            stdout=result.stdout,
        )
    except FileNotFoundError:
        logger.error(
            "Spark submit command not found. Make sure Spark is installed and in your PATH.",
        )
        raise


def run_silver_spark_pipeline(
    bronze_data_path: str,
    output_dir: str,
    file_format: str = "parquet",
) -> None:
    """
    Run the Silver layer Spark pipeline.
    
    Args:
        bronze_data_path: Path to the Bronze layer data
        output_dir: Directory to store the output data
        file_format: Format of the Bronze layer data
    """
    # Set up the command
    script_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src/silver/spark_process.py",
    )
    
    target_path = os.path.join(output_dir, "silver")
    
    # Create a temporary file with job arguments
    job_args = {
        "JOB_NAME": "test-silver-spark",
        "source_path": bronze_data_path,
        "source_format": file_format,
        "table_name": "test_table",
        "partition_cols": "",
        "apply_quality_checks": "true",
    }
    
    job_args_path = os.path.join(output_dir, "job_args.json")
    with open(job_args_path, "w") as f:
        json.dump(job_args, f)
    
    # Set up the Spark submit command
    command = [
        "spark-submit",
        "--master", "local[*]",
        "--deploy-mode", "client",
        script_path,
        "--JOB_NAME", "test-silver-spark",
        "--source_path", bronze_data_path,
        "--source_format", file_format,
        "--table_name", "test_table",
        "--apply_quality_checks", "true",
    ]
    
    # Run the command
    logger.info(
        "Running Silver layer Spark pipeline",
        command=" ".join(command),
    )
    
    try:
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(
                "Silver layer Spark pipeline failed",
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
            raise RuntimeError(f"Silver layer Spark pipeline failed: {result.stderr}")
        
        logger.info(
            "Silver layer Spark pipeline completed successfully",
            stdout=result.stdout,
        )
    except FileNotFoundError:
        logger.error(
            "Spark submit command not found. Make sure Spark is installed and in your PATH.",
        )
        raise


def validate_results(output_dir: str, pipeline: str) -> None:
    """
    Validate the results of the ETL pipeline.
    
    Args:
        output_dir: Directory with the output data
        pipeline: Pipeline that was tested
    """
    if pipeline == "bronze-python":
        # Check if the output file exists in the mock S3 bucket
        bronze_bucket = config.get("s3.bronze.bucket")
        s3 = boto3.client("s3", region_name="us-east-1")
        
        try:
            response = s3.list_objects_v2(
                Bucket=bronze_bucket,
                Prefix="bronze/",
            )
            
            if "Contents" in response:
                logger.info(
                    "Bronze layer Python pipeline results validated",
                    files=[obj["Key"] for obj in response["Contents"]],
                )
            else:
                logger.warning(
                    "No files found in the Bronze layer bucket",
                    bucket=bronze_bucket,
                    prefix="bronze/",
                )
        except Exception as e:
            logger.error(
                "Error validating Bronze layer Python pipeline results",
                error=str(e),
            )
    
    elif pipeline == "bronze-spark":
        # Check if the output files exist in the output directory
        bronze_dir = os.path.join(output_dir, "bronze")
        
        if os.path.exists(bronze_dir) and os.listdir(bronze_dir):
            logger.info(
                "Bronze layer Spark pipeline results validated",
                files=os.listdir(bronze_dir),
            )
        else:
            logger.warning(
                "No files found in the Bronze layer output directory",
                directory=bronze_dir,
            )
    
    elif pipeline == "silver-spark":
        # Check if the output files exist in the output directory
        silver_dir = os.path.join(output_dir, "silver")
        
        if os.path.exists(silver_dir) and os.listdir(silver_dir):
            logger.info(
                "Silver layer Spark pipeline results validated",
                files=os.listdir(silver_dir),
            )
        else:
            logger.warning(
                "No files found in the Silver layer output directory",
                directory=silver_dir,
            )


def main():
    """Main function."""
    # Parse command line arguments
    args = parse_args()
    
    pipeline = args["pipeline"]
    data_size = args["data_size"]
    output_dir = args["output_dir"]
    
    logger.info(
        "Starting ETL pipeline test",
        pipeline=pipeline,
        data_size=data_size,
        output_dir=output_dir,
    )
    
    try:
        # Set up the local environment
        setup_local_environment(output_dir)
        
        # Generate sample data
        df = generate_sample_data(data_size)
        
        # Determine the file format based on the pipeline
        file_format = "csv" if pipeline.startswith("bronze") else "parquet"
        
        # Write sample data to a file
        sample_data_path = write_sample_data(df, output_dir, file_format)
        
        # Run the appropriate pipeline
        if pipeline == "bronze-python":
            # Use mock S3 for the Python pipeline
            with mock_s3():
                setup_mock_s3()
                run_bronze_python_pipeline(sample_data_path, output_dir, file_format)
                validate_results(output_dir, pipeline)
        
        elif pipeline == "bronze-spark":
            run_bronze_spark_pipeline(sample_data_path, output_dir, file_format)
            validate_results(output_dir, pipeline)
        
        elif pipeline == "silver-spark":
            # First run the Bronze layer Spark pipeline to generate input for the Silver layer
            bronze_output_dir = os.path.join(output_dir, "bronze")
            run_bronze_spark_pipeline(sample_data_path, output_dir, "csv")
            
            # Then run the Silver layer Spark pipeline
            run_silver_spark_pipeline(bronze_output_dir, output_dir, "parquet")
            validate_results(output_dir, pipeline)
        
        logger.info(
            "ETL pipeline test completed successfully",
            pipeline=pipeline,
        )
    
    except Exception as e:
        logger.error(
            "Error in ETL pipeline test",
            pipeline=pipeline,
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


if __name__ == "__main__":
    main()