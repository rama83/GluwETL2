# Getting Started with AWS Data Lake Framework

This guide provides step-by-step instructions for setting up and using the AWS Data Lake Framework based on the medallion architecture.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setting Up the Environment](#setting-up-the-environment)
- [Configuring AWS Resources](#configuring-aws-resources)
- [Developing ETL Scripts](#developing-etl-scripts)
- [Local Testing](#local-testing)
- [Deployment](#deployment)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Overview

The AWS Data Lake Framework implements a medallion architecture with:

- **Bronze Layer**: Raw data stored in S3
- **Silver Layer**: Refined data using S3Tables
- **Gold Layer**: (Not implemented in this version)

The framework provides:

- AWS Glue 5.0 ETL pipelines (Spark and Python shell)
- Comprehensive logging and error handling
- Configuration management
- Testing framework
- CI/CD pipeline integration
- Local development environment

## Prerequisites

Before you begin, ensure you have the following:

- AWS account with appropriate permissions
- AWS CLI installed and configured
- Python 3.9 or later
- PySpark (for local Spark development)
- Git

## Project Structure

```
GlueETL2/
├── src/
│   ├── bronze/         # Bronze layer ETL scripts
│   ├── silver/         # Silver layer ETL scripts
│   ├── utils/          # Utility functions
│   ├── config/         # Configuration files
│   ├── logging/        # Logging setup
│   ├── errors/         # Error handling
├── tests/              # Test files
├── cicd/               # CI/CD pipeline files
├── docs/               # Documentation
├── local_dev/          # Local development setup
├── requirements.txt    # Python dependencies
└── README.md           # Project overview
```

## Setting Up the Environment

1. **Clone the repository**:

   ```bash
   git clone https://github.com/your-org/GlueETL2.git
   cd GlueETL2
   ```

2. **Create a virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the environment**:

   Create a `.env` file in the root directory with your AWS credentials and configuration:

   ```
   AWS_REGION=us-east-1
   AWS_PROFILE=default
   GLUE_ETL_S3_BRONZE_BUCKET=your-bronze-bucket
   GLUE_ETL_S3_SILVER_BUCKET=your-silver-bucket
   GLUE_ETL_S3_TEMP_BUCKET=your-temp-bucket
   ```

## Configuring AWS Resources

1. **Create S3 buckets**:

   ```bash
   aws s3 mb s3://your-bronze-bucket
   aws s3 mb s3://your-silver-bucket
   aws s3 mb s3://your-temp-bucket
   ```

2. **Create IAM role for Glue**:

   Create an IAM role with the following policies:
   - AmazonS3FullAccess
   - AWSGlueServiceRole
   - CloudWatchLogsFullAccess

   Make note of the ARN for the CI/CD pipeline.

3. **Update configuration**:

   Edit `src/config/settings.yaml` to match your AWS environment:

   ```yaml
   aws:
     region: us-east-1
     profile: default
     glue:
       role: arn:aws:iam::123456789012:role/GlueETLRole
   
   s3:
     bronze:
       bucket: your-bronze-bucket
       prefix: raw/
     silver:
       bucket: your-silver-bucket
       prefix: processed/
     temp:
       bucket: your-temp-bucket
       prefix: temp/
   ```

## Developing ETL Scripts

### Bronze Layer ETL Scripts

The Bronze layer is responsible for ingesting raw data from various sources and storing it in S3. Two types of ETL scripts are provided:

1. **Spark ETL Script** (`src/bronze/spark_ingest.py`):
   - Use for large-scale data processing
   - Supports various data formats (CSV, JSON, Parquet, ORC)
   - Adds metadata columns for tracking

2. **Python Shell Script** (`src/bronze/python_ingest.py`):
   - Use for simpler data ingestion tasks
   - Uses pandas for data processing
   - More suitable for smaller datasets

To create a new Bronze layer ETL script:

1. Copy the appropriate template (Spark or Python shell)
2. Customize the `transform_data` function for your specific requirements
3. Add any additional functions needed for your use case

### Silver Layer ETL Scripts

The Silver layer processes data from the Bronze layer, applies transformations and data quality checks, and stores it in S3Tables. The provided script:

1. **Spark ETL Script** (`src/silver/spark_process.py`):
   - Reads data from the Bronze layer
   - Applies transformations and data quality checks
   - Writes data to S3Tables
   - Separates valid and invalid data

To create a new Silver layer ETL script:

1. Copy the template
2. Customize the `apply_transformations` and `apply_data_quality_checks` functions
3. Add any additional functions needed for your use case

## Local Testing

The framework provides a local testing environment for developing and testing ETL scripts before deploying them to AWS.

### Running Tests Locally

Use the `local_dev/test_etl.py` script to test your ETL pipelines:

```bash
# Test Bronze layer Python pipeline
python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output

# Test Bronze layer Spark pipeline
python local_dev/test_etl.py --pipeline bronze-spark --data-size 1000 --output-dir local_dev/output

# Test Silver layer Spark pipeline
python local_dev/test_etl.py --pipeline silver-spark --data-size 1000 --output-dir local_dev/output
```

### Writing Unit Tests

Create unit tests in the `tests` directory for your ETL scripts:

```python
# tests/bronze/test_python_ingest.py
import pandas as pd
from src.bronze.python_ingest import transform_data

def test_transform_data():
    # Create sample data
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "value": [10, 20, 30],
    })
    
    # Apply transformation
    result = transform_data(df)
    
    # Check results
    assert "bronze_ingest_timestamp" in result.columns
    assert "bronze_ingest_date" in result.columns
    assert "bronze_ingest_time" in result.columns
    assert len(result) == 3
```

Run the unit tests:

```bash
pytest tests/
```

## Deployment

### Manual Deployment

1. **Upload ETL scripts to S3**:

   ```bash
   aws s3 cp src/bronze/spark_ingest.py s3://your-temp-bucket/scripts/
   aws s3 cp src/bronze/python_ingest.py s3://your-temp-bucket/scripts/
   aws s3 cp src/silver/spark_process.py s3://your-temp-bucket/scripts/
   ```

2. **Create Glue jobs**:

   ```bash
   # Bronze layer Spark job
   aws glue create-job \
     --name bronze-spark-ingest \
     --role arn:aws:iam::123456789012:role/GlueETLRole \
     --command "Name=glueetl,ScriptLocation=s3://your-temp-bucket/scripts/spark_ingest.py,PythonVersion=3.9" \
     --glue-version 5.0 \
     --worker-type G.1X \
     --number-of-workers 5
   
   # Bronze layer Python shell job
   aws glue create-job \
     --name bronze-python-ingest \
     --role arn:aws:iam::123456789012:role/GlueETLRole \
     --command "Name=pythonshell,ScriptLocation=s3://your-temp-bucket/scripts/python_ingest.py,PythonVersion=3.9" \
     --glue-version 5.0 \
     --max-capacity 0.0625
   
   # Silver layer Spark job
   aws glue create-job \
     --name silver-spark-process \
     --role arn:aws:iam::123456789012:role/GlueETLRole \
     --command "Name=glueetl,ScriptLocation=s3://your-temp-bucket/scripts/spark_process.py,PythonVersion=3.9" \
     --glue-version 5.0 \
     --worker-type G.1X \
     --number-of-workers 5
   ```

3. **Run Glue jobs**:

   ```bash
   # Bronze layer Spark job
   aws glue start-job-run \
     --job-name bronze-spark-ingest \
     --arguments '{"--source_type":"csv","--source_path":"s3://your-bronze-bucket/raw/sample.csv","--target_path":"s3://your-bronze-bucket/raw/processed/","--file_format":"parquet"}'
   
   # Bronze layer Python shell job
   aws glue start-job-run \
     --job-name bronze-python-ingest \
     --arguments '{"--source-type":"csv","--source-path":"s3://your-bronze-bucket/raw/sample.csv","--target-key":"raw/processed/sample.parquet","--file-format":"parquet"}'
   
   # Silver layer Spark job
   aws glue start-job-run \
     --job-name silver-spark-process \
     --arguments '{"--source_path":"s3://your-bronze-bucket/raw/processed/","--source_format":"parquet","--table_name":"sample_table","--apply_quality_checks":"true"}'
   ```

### CI/CD Deployment

The framework includes a GitHub Actions workflow for CI/CD deployment:

1. **Set up GitHub repository**:
   - Push the code to a GitHub repository
   - Configure the following secrets in the repository settings:
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`
     - `AWS_REGION`
     - `S3_SCRIPTS_BUCKET`
     - `GLUE_ROLE_ARN`

2. **Trigger the workflow**:
   - Push to the main branch
   - Create a pull request to the main branch
   - Manually trigger the workflow with custom parameters

The workflow will:
   - Run tests
   - Deploy ETL scripts to S3
   - Create or update Glue jobs
   - Optionally run the ETL jobs

## Monitoring and Troubleshooting

### Monitoring ETL Jobs

1. **CloudWatch Logs**:
   - View logs in the CloudWatch console
   - Filter logs by job name or timestamp

2. **Glue Job Metrics**:
   - View job metrics in the Glue console
   - Monitor job duration, DPU hours, and success/failure rates

3. **Custom Logging**:
   - Use the framework's logging module for structured logging
   - Add custom log statements to your ETL scripts

### Troubleshooting

1. **Check CloudWatch Logs**:
   - Look for error messages and stack traces
   - Check for data quality issues

2. **Run Jobs Locally**:
   - Use the local testing environment to debug issues
   - Add print statements for debugging

3. **Check S3 Data**:
   - Verify that data is being written to the correct locations
   - Check file formats and partitioning

4. **Common Issues**:
   - **Permission errors**: Check IAM roles and policies
   - **Resource errors**: Check worker type and number of workers
   - **Data format errors**: Check source data format and schema
   - **Timeout errors**: Increase job timeout or optimize the script

## Next Steps

- Implement the Gold layer for business-level aggregations and metrics
- Add data quality monitoring and alerting
- Implement data lineage tracking
- Set up scheduled ETL jobs using AWS EventBridge