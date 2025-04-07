"""
Spark ETL script for ingesting data into the Bronze layer.
This script reads data from a source, performs basic transformations,
and writes the data to the Bronze layer in S3.
"""
import sys
from datetime import datetime
from typing import Dict, List, Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_type",
        "source_path",
        "target_path",
        "file_format",
        "partition_cols",
    ],
)

job.init(args["JOB_NAME"], args)

# Set job parameters
source_type = args["source_type"]
source_path = args["source_path"]
target_path = args["target_path"]
file_format = args["file_format"]
partition_cols = args.get("partition_cols", "").split(",") if args.get("partition_cols") else []

# Set current timestamp for partitioning
current_date = datetime.now().strftime("%Y-%m-%d")
current_time = datetime.now().strftime("%H-%M-%S")


def read_data(
    source_type: str,
    source_path: str,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read data from the source.
    
    Args:
        source_type: Type of source (csv, json, parquet, etc.)
        source_path: Path to the source data
        options: Additional options for reading the data
        
    Returns:
        DataFrame with the source data
    """
    options = options or {}
    
    if source_type == "csv":
        return spark.read.options(**options).csv(source_path, header=True, inferSchema=True)
    elif source_type == "json":
        return spark.read.options(**options).json(source_path)
    elif source_type == "parquet":
        return spark.read.options(**options).parquet(source_path)
    elif source_type == "orc":
        return spark.read.options(**options).orc(source_path)
    elif source_type == "jdbc":
        return spark.read.options(**options).format("jdbc").load()
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def transform_data(df: DataFrame) -> DataFrame:
    """
    Apply transformations to the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Add metadata columns
    df = df.withColumn("bronze_ingest_timestamp", current_timestamp())
    df = df.withColumn("bronze_ingest_date", lit(current_date))
    df = df.withColumn("bronze_ingest_time", lit(current_time))
    df = df.withColumn("bronze_source_path", lit(source_path))
    
    # Add additional transformations here
    
    return df


def write_data(
    df: DataFrame,
    target_path: str,
    file_format: str,
    partition_cols: Optional[List[str]] = None,
    options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Write data to the target.
    
    Args:
        df: DataFrame to write
        target_path: Path to write the data to
        file_format: Format to write the data in
        partition_cols: Columns to partition by
        options: Additional options for writing the data
    """
    options = options or {}
    writer = df.write.options(**options).mode("append")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    if file_format == "csv":
        writer.csv(target_path)
    elif file_format == "json":
        writer.json(target_path)
    elif file_format == "parquet":
        writer.parquet(target_path)
    elif file_format == "orc":
        writer.orc(target_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def main():
    """Main ETL function."""
    # Log job parameters
    print(f"Source type: {source_type}")
    print(f"Source path: {source_path}")
    print(f"Target path: {target_path}")
    print(f"File format: {file_format}")
    print(f"Partition columns: {partition_cols}")
    
    try:
        # Read data from source
        df = read_data(source_type, source_path)
        
        # Log data statistics
        print(f"Read {df.count()} rows and {len(df.columns)} columns from source")
        
        # Transform data
        df = transform_data(df)
        
        # Write data to target
        write_data(df, target_path, file_format, partition_cols)
        
        # Log success
        print(f"Successfully wrote data to {target_path}")
        
        # Commit the job
        job.commit()
    except Exception as e:
        # Log error
        print(f"Error in ETL job: {str(e)}")
        raise


if __name__ == "__main__":
    main()