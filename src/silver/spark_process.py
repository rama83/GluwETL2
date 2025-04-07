"""
Spark ETL script for processing data from the Bronze layer to the Silver layer.
This script reads data from the Bronze layer, applies transformations and data quality checks,
and writes the data to the Silver layer using S3Tables.
"""
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import StructType

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
        "source_path",
        "source_format",
        "table_name",
        "partition_cols",
        "apply_quality_checks",
    ],
)

job.init(args["JOB_NAME"], args)

# Set job parameters
source_path = args["source_path"]
source_format = args["source_format"]
table_name = args["table_name"]
partition_cols = args.get("partition_cols", "").split(",") if args.get("partition_cols") else []
apply_quality_checks = args.get("apply_quality_checks", "true").lower() == "true"

# Set current timestamp for metadata
current_date = datetime.now().strftime("%Y-%m-%d")
current_time = datetime.now().strftime("%H-%M-%S")


def read_from_bronze(
    source_path: str,
    source_format: str,
    options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Read data from the Bronze layer.
    
    Args:
        source_path: Path to the source data in the Bronze layer
        source_format: Format of the source data
        options: Additional options for reading the data
        
    Returns:
        DataFrame with the source data
    """
    options = options or {}
    
    if source_format == "csv":
        return spark.read.options(**options).csv(source_path, header=True, inferSchema=True)
    elif source_format == "json":
        return spark.read.options(**options).json(source_path)
    elif source_format == "parquet":
        return spark.read.options(**options).parquet(source_path)
    elif source_format == "orc":
        return spark.read.options(**options).orc(source_path)
    else:
        raise ValueError(f"Unsupported source format: {source_format}")


def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply transformations to the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Add metadata columns
    df = df.withColumn("silver_process_timestamp", current_timestamp())
    df = df.withColumn("silver_process_date", lit(current_date))
    df = df.withColumn("silver_process_time", lit(current_time))
    df = df.withColumn("silver_source_path", lit(source_path))
    
    # Convert string timestamps to proper timestamp type if they exist
    if "bronze_ingest_timestamp" in df.columns:
        df = df.withColumn(
            "bronze_ingest_timestamp",
            to_timestamp(col("bronze_ingest_timestamp"))
        )
    
    # Add additional transformations here
    # For example, data type conversions, column renaming, etc.
    
    return df


def apply_data_quality_checks(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Apply data quality checks to the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (valid_data, invalid_data)
    """
    # Create a column to track data quality issues
    df = df.withColumn("data_quality_issues", lit(None))
    
    # Apply data quality checks
    # For example, check for null values in required columns
    for column in df.columns:
        # Skip metadata columns
        if column.startswith("bronze_") or column.startswith("silver_") or column == "data_quality_issues":
            continue
        
        # Check for null values
        df = df.withColumn(
            "data_quality_issues",
            when(
                col(column).isNull() & col("data_quality_issues").isNull(),
                lit(f"Null value in {column}")
            ).when(
                col(column).isNull() & col("data_quality_issues").isNotNull(),
                concat(col("data_quality_issues"), lit(f", Null value in {column}"))
            ).otherwise(col("data_quality_issues"))
        )
    
    # Split into valid and invalid data
    valid_data = df.filter(col("data_quality_issues").isNull())
    invalid_data = df.filter(col("data_quality_issues").isNotNull())
    
    # Drop the data quality issues column from valid data
    valid_data = valid_data.drop("data_quality_issues")
    
    return valid_data, invalid_data


def write_to_silver(
    df: DataFrame,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    mode: str = "append",
) -> None:
    """
    Write data to the Silver layer using S3Tables.
    
    Args:
        df: DataFrame to write
        table_name: Name of the table in the Silver layer
        partition_cols: Columns to partition by
        mode: Write mode (append or overwrite)
    """
    # Get the S3 path for the table
    from awsglue.dynamicframe import DynamicFrame
    
    # Convert to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)
    
    # Write to S3Tables
    sink = glueContext.getSink(
        connection_type="s3",
        path=f"s3://data-lake-silver/processed/{table_name}/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_cols,
    )
    
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(
        catalogDatabase="silver",
        catalogTableName=table_name,
    )
    
    sink.writeFrame(dynamic_frame)


def write_invalid_data(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Write invalid data to a separate location for further analysis.
    
    Args:
        df: DataFrame with invalid data
        table_name: Name of the table
    """
    if df.count() == 0:
        print("No invalid data to write")
        return
    
    # Write to a separate location
    df.write.mode("append").parquet(f"s3://data-lake-silver/invalid/{table_name}/")
    
    print(f"Wrote {df.count()} invalid records to s3://data-lake-silver/invalid/{table_name}/")


def main():
    """Main ETL function."""
    # Log job parameters
    print(f"Source path: {source_path}")
    print(f"Source format: {source_format}")
    print(f"Table name: {table_name}")
    print(f"Partition columns: {partition_cols}")
    print(f"Apply quality checks: {apply_quality_checks}")
    
    try:
        # Read data from Bronze layer
        df = read_from_bronze(source_path, source_format)
        
        # Log data statistics
        print(f"Read {df.count()} rows and {len(df.columns)} columns from Bronze layer")
        
        # Apply transformations
        df = apply_transformations(df)
        
        # Apply data quality checks if enabled
        if apply_quality_checks:
            valid_data, invalid_data = apply_data_quality_checks(df)
            
            # Log data quality statistics
            print(f"Valid records: {valid_data.count()}")
            print(f"Invalid records: {invalid_data.count()}")
            
            # Write valid data to Silver layer
            write_to_silver(valid_data, table_name, partition_cols)
            
            # Write invalid data to a separate location
            write_invalid_data(invalid_data, table_name)
        else:
            # Write all data to Silver layer
            write_to_silver(df, table_name, partition_cols)
        
        # Log success
        print(f"Successfully processed data to Silver layer table: {table_name}")
        
        # Commit the job
        job.commit()
    except Exception as e:
        # Log error
        print(f"Error in ETL job: {str(e)}")
        raise


if __name__ == "__main__":
    main()