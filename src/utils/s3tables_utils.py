"""
S3Tables utilities for the AWS Data Lake Framework.
Provides functions for working with S3Tables in the Silver layer.
"""
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow.dataset import Expression, Scanner

# Import the configuration, logging, and error handling modules
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import config  # noqa: E402
from logging import get_logger  # noqa: E402
from errors import AWSError, DataError, handle_aws_error, retry  # noqa: E402

# Create a logger for this module
logger = get_logger(__name__)


def get_silver_bucket() -> str:
    """
    Get the configured Silver layer S3 bucket name.
    
    Returns:
        S3 bucket name for the Silver layer
    """
    return config.get("s3.silver.bucket")


def get_silver_prefix() -> str:
    """
    Get the configured Silver layer S3 prefix.
    
    Returns:
        S3 prefix for the Silver layer
    """
    return config.get("s3.silver.prefix", "processed/")


def get_s3tables_format() -> str:
    """
    Get the configured S3Tables format.
    
    Returns:
        S3Tables format (e.g., 'parquet')
    """
    return config.get("s3tables.format", "parquet")


def get_s3tables_compression() -> str:
    """
    Get the configured S3Tables compression.
    
    Returns:
        S3Tables compression (e.g., 'snappy')
    """
    return config.get("s3tables.compression", "snappy")


def get_s3tables_partition_cols() -> List[str]:
    """
    Get the configured S3Tables partition columns.
    
    Returns:
        List of partition column names
    """
    return config.get("s3tables.partition_cols", [])


def get_table_path(table_name: str) -> str:
    """
    Get the S3 path for a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        S3 path for the table
    """
    bucket = get_silver_bucket()
    prefix = get_silver_prefix()
    return f"s3://{bucket}/{prefix}{table_name}"


def get_table_metadata_path(table_name: str) -> str:
    """
    Get the S3 path for a table's metadata in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        S3 path for the table's metadata
    """
    table_path = get_table_path(table_name)
    return f"{table_path}/_metadata"


@handle_aws_error(service="s3")
def table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        True if the table exists, False otherwise
    """
    try:
        # Try to open the dataset
        ds.dataset(get_table_path(table_name))
        return True
    except (FileNotFoundError, pa.ArrowInvalid):
        return False
    except Exception as e:
        logger.warning(
            "Error checking if table exists",
            table_name=table_name,
            error=str(e),
        )
        return False


@handle_aws_error(service="s3")
def create_table(
    table_name: str,
    schema: Optional[pa.Schema] = None,
    partition_cols: Optional[List[str]] = None,
) -> str:
    """
    Create a new table in the Silver layer.
    
    Args:
        table_name: Name of the table
        schema: PyArrow schema for the table
        partition_cols: List of partition column names
        
    Returns:
        S3 path for the created table
    """
    table_path = get_table_path(table_name)
    
    # Use default partition columns if not specified
    if partition_cols is None:
        partition_cols = get_s3tables_partition_cols()
    
    # Create an empty dataset with the specified schema
    if schema:
        # Create an empty DataFrame with the schema
        empty_df = pd.DataFrame()
        for field in schema:
            empty_df[field.name] = pd.Series(dtype=field.type.to_pandas_dtype())
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(empty_df, schema=schema)
        
        # Write to S3
        pq.write_to_dataset(
            table,
            table_path,
            partition_cols=partition_cols,
            compression=get_s3tables_compression(),
        )
    else:
        # Create an empty directory structure
        s3 = boto3.client("s3", region_name=config.get("aws.region"))
        bucket = get_silver_bucket()
        prefix = f"{get_silver_prefix()}{table_name}/"
        s3.put_object(Bucket=bucket, Key=prefix)
    
    logger.info(
        "Table created",
        table_name=table_name,
        table_path=table_path,
        partition_cols=partition_cols,
    )
    
    return table_path


@handle_aws_error(service="s3")
def get_table_schema(table_name: str) -> pa.Schema:
    """
    Get the schema of a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        PyArrow schema for the table
    """
    table_path = get_table_path(table_name)
    
    try:
        # Open the dataset and get the schema
        dataset = ds.dataset(table_path)
        return dataset.schema
    except Exception as e:
        raise DataError(
            message=f"Failed to get schema for table {table_name}: {str(e)}",
            source="s3tables",
            table=table_name,
        ) from e


@handle_aws_error(service="s3")
def write_to_table(
    table_name: str,
    data: Union[pd.DataFrame, pa.Table],
    partition_cols: Optional[List[str]] = None,
    mode: str = "append",
    schema: Optional[pa.Schema] = None,
) -> str:
    """
    Write data to a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        data: Data to write (pandas DataFrame or PyArrow Table)
        partition_cols: List of partition column names
        mode: Write mode ('append' or 'overwrite')
        schema: PyArrow schema for the table (used if table doesn't exist)
        
    Returns:
        S3 path for the table
    """
    table_path = get_table_path(table_name)
    
    # Use default partition columns if not specified
    if partition_cols is None:
        partition_cols = get_s3tables_partition_cols()
    
    # Convert pandas DataFrame to PyArrow Table if necessary
    if isinstance(data, pd.DataFrame):
        if schema:
            table = pa.Table.from_pandas(data, schema=schema)
        else:
            table = pa.Table.from_pandas(data)
    else:
        table = data
    
    # Check if the table exists
    exists = table_exists(table_name)
    
    if not exists:
        # Create the table if it doesn't exist
        if schema:
            create_table(table_name, schema, partition_cols)
        else:
            create_table(table_name, table.schema, partition_cols)
    
    # Write to the table
    if mode == "overwrite":
        # Delete existing data
        s3 = boto3.resource("s3", region_name=config.get("aws.region"))
        bucket = get_silver_bucket()
        prefix = f"{get_silver_prefix()}{table_name}/"
        
        s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()
    
    # Write the data
    pq.write_to_dataset(
        table,
        table_path,
        partition_cols=partition_cols,
        compression=get_s3tables_compression(),
    )
    
    logger.info(
        "Data written to table",
        table_name=table_name,
        table_path=table_path,
        rows=len(table),
        columns=table.column_names,
        mode=mode,
    )
    
    return table_path


@handle_aws_error(service="s3")
def read_from_table(
    table_name: str,
    columns: Optional[List[str]] = None,
    filter_expr: Optional[Expression] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read data from a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        columns: List of columns to read
        filter_expr: PyArrow filter expression
        limit: Maximum number of rows to read
        
    Returns:
        pandas DataFrame with the table data
    """
    table_path = get_table_path(table_name)
    
    try:
        # Open the dataset
        dataset = ds.dataset(table_path)
        
        # Create a scanner with the specified options
        scanner = Scanner.from_dataset(
            dataset,
            columns=columns,
            filter=filter_expr,
        )
        
        # Read the data
        if limit:
            table = scanner.head(limit)
        else:
            table = scanner.to_table()
        
        # Convert to pandas DataFrame
        df = table.to_pandas()
        
        logger.info(
            "Data read from table",
            table_name=table_name,
            table_path=table_path,
            rows=len(df),
            columns=list(df.columns),
        )
        
        return df
    except Exception as e:
        raise DataError(
            message=f"Failed to read from table {table_name}: {str(e)}",
            source="s3tables",
            table=table_name,
        ) from e


@handle_aws_error(service="s3")
def get_table_partitions(table_name: str) -> List[Dict[str, str]]:
    """
    Get the partitions of a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of partition dictionaries
    """
    table_path = get_table_path(table_name)
    
    try:
        # Open the dataset
        dataset = ds.dataset(table_path)
        
        # Get the partition expressions
        partitions = []
        for fragment in dataset.get_fragments():
            partition_dict = {}
            for expr in fragment.partition_expression:
                # Extract the partition key and value
                if isinstance(expr, ds.FieldExpression):
                    key = expr.field_name
                    value = str(expr.value)
                    partition_dict[key] = value
            
            if partition_dict:
                partitions.append(partition_dict)
        
        return partitions
    except Exception as e:
        raise DataError(
            message=f"Failed to get partitions for table {table_name}: {str(e)}",
            source="s3tables",
            table=table_name,
        ) from e


@handle_aws_error(service="s3")
def delete_table(table_name: str) -> None:
    """
    Delete a table from the Silver layer.
    
    Args:
        table_name: Name of the table
    """
    s3 = boto3.resource("s3", region_name=config.get("aws.region"))
    bucket = get_silver_bucket()
    prefix = f"{get_silver_prefix()}{table_name}/"
    
    # Delete all objects with the table prefix
    s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()
    
    logger.info(
        "Table deleted",
        table_name=table_name,
        bucket=bucket,
        prefix=prefix,
    )


@handle_aws_error(service="s3")
def get_table_stats(table_name: str) -> Dict[str, Any]:
    """
    Get statistics for a table in the Silver layer.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table statistics
    """
    table_path = get_table_path(table_name)
    
    try:
        # Open the dataset
        dataset = ds.dataset(table_path)
        
        # Get the schema
        schema = dataset.schema
        
        # Count the rows
        scanner = Scanner.from_dataset(dataset)
        row_count = scanner.count_rows()
        
        # Get the partitions
        partitions = get_table_partitions(table_name)
        
        # Get the size
        s3 = boto3.client("s3", region_name=config.get("aws.region"))
        bucket = get_silver_bucket()
        prefix = f"{get_silver_prefix()}{table_name}/"
        
        size_bytes = 0
        file_count = 0
        
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    size_bytes += obj["Size"]
                    file_count += 1
        
        # Return the statistics
        return {
            "table_name": table_name,
            "table_path": table_path,
            "row_count": row_count,
            "column_count": len(schema.names),
            "columns": schema.names,
            "partition_count": len(partitions),
            "partitions": partitions,
            "size_bytes": size_bytes,
            "size_mb": size_bytes / (1024 * 1024),
            "file_count": file_count,
        }
    except Exception as e:
        raise DataError(
            message=f"Failed to get statistics for table {table_name}: {str(e)}",
            source="s3tables",
            table=table_name,
        ) from e


@handle_aws_error(service="s3")
def list_tables() -> List[str]:
    """
    List all tables in the Silver layer.
    
    Returns:
        List of table names
    """
    s3 = boto3.client("s3", region_name=config.get("aws.region"))
    bucket = get_silver_bucket()
    prefix = get_silver_prefix()
    
    # List all prefixes under the Silver layer prefix
    paginator = s3.get_paginator("list_objects_v2")
    tables = set()
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        if "CommonPrefixes" in page:
            for common_prefix in page["CommonPrefixes"]:
                # Extract the table name from the prefix
                prefix_str = common_prefix["Prefix"]
                table_name = prefix_str[len(prefix):]
                
                # Remove trailing slash
                if table_name.endswith("/"):
                    table_name = table_name[:-1]
                
                if table_name:
                    tables.add(table_name)
    
    return list(tables)


@handle_aws_error(service="s3")
def copy_table(
    source_table: str,
    dest_table: str,
    overwrite: bool = False,
) -> str:
    """
    Copy a table in the Silver layer.
    
    Args:
        source_table: Name of the source table
        dest_table: Name of the destination table
        overwrite: Whether to overwrite the destination table if it exists
        
    Returns:
        S3 path for the destination table
    """
    # Check if the source table exists
    if not table_exists(source_table):
        raise DataError(
            message=f"Source table {source_table} does not exist",
            source="s3tables",
            table=source_table,
        )
    
    # Check if the destination table exists
    if table_exists(dest_table) and not overwrite:
        raise DataError(
            message=f"Destination table {dest_table} already exists",
            source="s3tables",
            table=dest_table,
        )
    
    # Read from the source table
    df = read_from_table(source_table)
    
    # Write to the destination table
    table_path = write_to_table(
        dest_table,
        df,
        mode="overwrite" if overwrite else "append",
    )
    
    logger.info(
        "Table copied",
        source_table=source_table,
        dest_table=dest_table,
        table_path=table_path,
        rows=len(df),
    )
    
    return table_path


@handle_aws_error(service="s3")
def merge_tables(
    source_table: str,
    dest_table: str,
    join_columns: List[str],
    update_columns: Optional[List[str]] = None,
) -> Tuple[str, int]:
    """
    Merge data from a source table into a destination table.
    
    Args:
        source_table: Name of the source table
        dest_table: Name of the destination table
        join_columns: List of columns to join on
        update_columns: List of columns to update (all columns if None)
        
    Returns:
        Tuple of (table path, number of rows updated)
    """
    # Check if the source table exists
    if not table_exists(source_table):
        raise DataError(
            message=f"Source table {source_table} does not exist",
            source="s3tables",
            table=source_table,
        )
    
    # Check if the destination table exists
    if not table_exists(dest_table):
        raise DataError(
            message=f"Destination table {dest_table} does not exist",
            source="s3tables",
            table=dest_table,
        )
    
    # Read from the source and destination tables
    source_df = read_from_table(source_table)
    dest_df = read_from_table(dest_table)
    
    # Determine the columns to update
    if update_columns is None:
        update_columns = [col for col in source_df.columns if col not in join_columns]
    
    # Perform the merge
    merged_df = dest_df.copy()
    rows_updated = 0
    
    # Create a dictionary to map join column values to row indices
    join_dict = {}
    for i, row in dest_df.iterrows():
        key = tuple(row[col] for col in join_columns)
        join_dict[key] = i
    
    # Update the destination DataFrame with values from the source DataFrame
    for _, row in source_df.iterrows():
        key = tuple(row[col] for col in join_columns)
        
        if key in join_dict:
            # Update existing row
            idx = join_dict[key]
            for col in update_columns:
                if col in row and col in merged_df:
                    merged_df.at[idx, col] = row[col]
            rows_updated += 1
        else:
            # Insert new row
            merged_df = pd.concat([merged_df, pd.DataFrame([row])], ignore_index=True)
            rows_updated += 1
    
    # Write the merged DataFrame back to the destination table
    table_path = write_to_table(
        dest_table,
        merged_df,
        mode="overwrite",
    )
    
    logger.info(
        "Tables merged",
        source_table=source_table,
        dest_table=dest_table,
        table_path=table_path,
        rows_updated=rows_updated,
    )
    
    return table_path, rows_updated