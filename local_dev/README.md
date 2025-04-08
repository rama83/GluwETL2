# AWS Glue 5.0 Local Development

This directory contains tools and scripts for local development and testing of AWS Glue 5.0 ETL jobs using Docker.

## Overview

AWS Glue 5.0 is a serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development. The AWS Glue 5.0 Docker container allows you to develop and test your AWS Glue ETL jobs locally before deploying them to AWS.

## Prerequisites

- Docker and Docker Compose installed on your machine
- AWS credentials configured (if accessing AWS services)
- Git (for Windows users who want to use Git Bash to run shell scripts)

## Getting Started

1. **Start the AWS Glue 5.0 Docker container**:

   ```bash
   # From the project root directory
   docker-compose up -d
   ```

2. **Access Jupyter Notebook**:

   Open your browser and navigate to http://localhost:8888 to access the Jupyter notebook. The token is printed in the container logs, which you can view with:

   ```bash
   docker-compose logs
   ```

3. **Run ETL scripts inside the container**:

   You can use the provided scripts to run commands inside the container:

   ```bash
   # On Linux/macOS
   ./run_in_glue_container.sh python test_etl.py --pipeline bronze-python --data-size 1000 --output-dir ../local_dev/output

   # On Windows with Git Bash
   bash run_in_glue_container.sh python test_etl.py --pipeline bronze-python --data-size 1000 --output-dir ../local_dev/output

   # On Windows with Command Prompt or PowerShell
   run_in_glue_container.bat python test_etl.py --pipeline bronze-python --data-size 1000 --output-dir ../local_dev/output
   ```

   Both `run_in_glue_container.sh` (for Linux/macOS/Git Bash) and `run_in_glue_container.bat` (for Windows) provide the same functionality:
   - Check if the container is running and start it if needed
   - Run the specified command inside the container
   - Pass all arguments to the command

## Examples

### Running the Bronze Layer Python Pipeline

```bash
./run_in_glue_container.sh python test_etl.py --pipeline bronze-python --data-size 1000 --output-dir ../local_dev/output
```

### Running the Bronze Layer Spark Pipeline

```bash
./run_in_glue_container.sh python test_etl.py --pipeline bronze-spark --data-size 1000 --output-dir ../local_dev/output
```

### Running the Silver Layer Spark Pipeline

```bash
./run_in_glue_container.sh python test_etl.py --pipeline silver-spark --data-size 1000 --output-dir ../local_dev/output
```

### Running a Spark Job Directly

```bash
./run_in_glue_container.sh spark-submit --master local[*] ../src/bronze/spark_ingest.py --JOB_NAME test-job --source_type csv --source_path ../local_dev/data/sample.csv --target_path ../local_dev/output/bronze --file_format parquet
```

## Troubleshooting

### Container Not Starting

If the container fails to start, check the Docker logs:

```bash
docker-compose logs
```

### Permission Issues

If you encounter permission issues when running scripts inside the container, make sure the files have the correct permissions:

```bash
# On Linux/macOS
chmod +x run_in_glue_container.sh
```

### Path Issues

When running commands inside the container, remember that the project root is mounted at `/home/glue_user/workspace`. Adjust your paths accordingly.

### AWS Credentials

If you need to access AWS services from the container, make sure your AWS credentials are properly configured in `~/.aws/credentials` and mounted to the container.

## Additional Resources

- [AWS Glue 5.0 Documentation](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [AWS Glue 5.0 Docker Container Blog Post](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-5-0-jobs-locally-using-a-docker-container/)
- [AWS Glue GitHub Repository](https://github.com/aws-samples/aws-glue-samples)