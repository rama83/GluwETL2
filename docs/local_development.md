# Local Development Guide

This guide provides detailed instructions for local development and testing of ETL pipelines in the AWS Data Lake Framework.

## Table of Contents

- [Setting Up the Local Environment](#setting-up-the-local-environment)
- [Running ETL Scripts Locally](#running-etl-scripts-locally)
- [Mocking AWS Services](#mocking-aws-services)
- [Debugging ETL Scripts](#debugging-etl-scripts)
- [Best Practices](#best-practices)

## Setting Up the Local Environment

### Prerequisites

- Python 3.9 or later
- Java 8 or later (for PySpark)
- Apache Spark (for Spark ETL scripts)
- Docker (optional, for containerized development)

### Installation

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

4. **Install Apache Spark** (if not already installed):

   - Download Apache Spark from https://spark.apache.org/downloads.html
   - Extract the archive to a directory of your choice
   - Add Spark's bin directory to your PATH
   - Set the SPARK_HOME environment variable

5. **Configure local settings**:

   Create a `.env` file in the root directory:

   ```
   AWS_REGION=us-east-1
   AWS_PROFILE=default
   GLUE_ETL_S3_BRONZE_BUCKET=local-bronze-bucket
   GLUE_ETL_S3_SILVER_BUCKET=local-silver-bucket
   GLUE_ETL_S3_TEMP_BUCKET=local-temp-bucket
   ```

6. **Create local directories**:

   ```bash
   mkdir -p local_dev/data/bronze
   mkdir -p local_dev/data/silver
   mkdir -p local_dev/output/bronze
   mkdir -p local_dev/output/silver
   ```

## Running ETL Scripts Locally

### Using the Test Script

The framework provides a test script (`local_dev/test_etl.py`) for running ETL pipelines locally:

```bash
# Test Bronze layer Python pipeline
python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output

# Test Bronze layer Spark pipeline
python local_dev/test_etl.py --pipeline bronze-spark --data-size 1000 --output-dir local_dev/output

# Test Silver layer Spark pipeline
python local_dev/test_etl.py --pipeline silver-spark --data-size 1000 --output-dir local_dev/output
```

### Running Python Shell Scripts Directly

You can run Python shell scripts directly:

```bash
python src/bronze/python_ingest.py \
  --source-type csv \
  --source-path local_dev/data/sample.csv \
  --target-key bronze/sample.parquet \
  --file-format parquet
```

### Running Spark Scripts with spark-submit

You can run Spark scripts using `spark-submit`:

```bash
spark-submit \
  --master local[*] \
  --deploy-mode client \
  src/bronze/spark_ingest.py \
  --JOB_NAME test-job \
  --source_type csv \
  --source_path local_dev/data/sample.csv \
  --target_path local_dev/output/bronze \
  --file_format parquet
```

### Using Docker for Local Development

You can use Docker to create a containerized development environment:

1. **Create a Dockerfile**:

   ```dockerfile
   FROM python:3.9-slim

   # Install Java for PySpark
   RUN apt-get update && \
       apt-get install -y openjdk-11-jdk && \
       apt-get clean

   # Install Apache Spark
   RUN pip install pyspark==3.3.0

   # Set up the working directory
   WORKDIR /app

   # Copy requirements and install dependencies
   COPY requirements.txt .
   RUN pip install -r requirements.txt

   # Copy the application code
   COPY . .

   # Set environment variables
   ENV PYTHONPATH=/app

   # Command to run when the container starts
   CMD ["bash"]
   ```

2. **Build and run the Docker container**:

   ```bash
   docker build -t glue-etl-local .
   docker run -it -v $(pwd):/app glue-etl-local
   ```

3. **Run ETL scripts inside the container**:

   ```bash
   python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output
   ```

## Mocking AWS Services

The framework uses the `moto` library to mock AWS services for local testing.

### Mocking S3

```python
from moto import mock_s3
import boto3

# Start the mock S3 service
with mock_s3():
    # Create a mock S3 client
    s3 = boto3.client("s3", region_name="us-east-1")
    
    # Create mock buckets
    s3.create_bucket(Bucket="local-bronze-bucket")
    s3.create_bucket(Bucket="local-silver-bucket")
    
    # Use the S3 client as usual
    s3.put_object(Bucket="local-bronze-bucket", Key="test.txt", Body="test")
    
    # Your ETL code here
```

### Mocking Glue

```python
from moto import mock_glue
import boto3

# Start the mock Glue service
with mock_glue():
    # Create a mock Glue client
    glue = boto3.client("glue", region_name="us-east-1")
    
    # Create mock Glue resources
    glue.create_database(DatabaseInput={"Name": "test_db"})
    
    # Your ETL code here
```

### Using the Test Script with Mocked Services

The `test_etl.py` script automatically uses mocked AWS services for the Python pipeline:

```bash
python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output
```

## Debugging ETL Scripts

### Debugging Python Shell Scripts

You can use standard Python debugging techniques:

1. **Using print statements**:

   ```python
   print(f"Data shape: {df.shape}")
   print(f"Column names: {df.columns}")
   ```

2. **Using the Python debugger (pdb)**:

   ```python
   import pdb

   def transform_data(df):
       pdb.set_trace()  # Debugger will stop here
       # Your code here
   ```

3. **Using logging**:

   ```python
   from logging import get_logger
   logger = get_logger(__name__)

   logger.info("Starting transformation", data_shape=df.shape)
   ```

### Debugging Spark Scripts

1. **Using Spark UI**:

   When running Spark locally, the Spark UI is available at http://localhost:4040

2. **Using print statements**:

   ```python
   print(f"Data count: {df.count()}")
   print(f"Schema: {df.schema}")
   ```

3. **Collecting data for inspection**:

   ```python
   # Collect a small sample for debugging
   sample = df.limit(10).collect()
   for row in sample:
       print(row)
   ```

4. **Using logging**:

   ```python
   from logging import get_logger
   logger = get_logger(__name__)

   logger.info("Starting transformation", count=df.count())
   ```

### Inspecting Data

1. **Saving data to local files**:

   ```python
   # For pandas DataFrames
   df.to_csv("debug_output.csv", index=False)
   
   # For Spark DataFrames
   df.write.csv("debug_output", header=True)
   ```

2. **Using data visualization**:

   ```python
   import matplotlib.pyplot as plt
   
   # Plot data distribution
   df["value"].hist()
   plt.savefig("distribution.png")
   ```

## Best Practices

### Code Organization

1. **Modular code**:
   - Break down ETL scripts into small, reusable functions
   - Use helper functions for common operations

2. **Configuration management**:
   - Use the framework's configuration module
   - Avoid hardcoding values in scripts

3. **Error handling**:
   - Use the framework's error handling utilities
   - Add appropriate try-except blocks

### Testing

1. **Unit tests**:
   - Write unit tests for individual functions
   - Use pytest for testing

2. **Integration tests**:
   - Test the entire ETL pipeline
   - Use mock AWS services

3. **Data validation**:
   - Validate input and output data
   - Check for data quality issues

### Performance Optimization

1. **Spark optimization**:
   - Use appropriate partitioning
   - Optimize join operations
   - Cache intermediate results when appropriate

2. **Resource management**:
   - Monitor memory usage
   - Adjust Spark configuration for local development

### Local Development Workflow

1. **Develop locally first**:
   - Write and test ETL scripts locally
   - Use small sample datasets

2. **Incremental testing**:
   - Test individual components before testing the entire pipeline
   - Add new features incrementally

3. **Version control**:
   - Commit changes frequently
   - Use feature branches for new development

4. **Documentation**:
   - Document your ETL scripts
   - Add comments for complex logic

## Troubleshooting

### Common Issues

1. **Spark installation issues**:
   - Ensure Java is installed and JAVA_HOME is set
   - Check that Spark is in your PATH
   - Verify that SPARK_HOME is set correctly

2. **Import errors**:
   - Check that all dependencies are installed
   - Verify that the Python path includes the project root

3. **AWS credential issues**:
   - Check that AWS credentials are configured correctly
   - Use the AWS CLI to verify credentials

4. **Data format issues**:
   - Verify that input data is in the expected format
   - Check for schema mismatches

### Getting Help

If you encounter issues that you can't resolve, try the following:

1. Check the project documentation
2. Look for similar issues in the project's issue tracker
3. Reach out to the project maintainers