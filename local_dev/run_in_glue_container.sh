#!/bin/bash
# Helper script for running ETL jobs in the AWS Glue 5.0 Docker container

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." &> /dev/null && pwd )"

# Check if the container is running
if ! docker ps | grep -q glue-local-dev; then
  echo "Starting Glue container..."
  cd "$PROJECT_ROOT" && docker-compose up -d
  
  # Wait for the container to start
  echo "Waiting for container to start..."
  sleep 5
fi

# Check if a command was provided
if [ $# -eq 0 ]; then
  echo "Usage: $0 <command> [args...]"
  echo "Examples:"
  echo "  $0 python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output"
  echo "  $0 spark-submit --master local[*] src/bronze/spark_ingest.py --JOB_NAME test-job --source_type csv --source_path local_dev/data/sample.csv --target_path local_dev/output/bronze --file_format parquet"
  exit 1
fi

# Build the command to run
CMD="cd /home/glue_user/workspace && $@"

# Run the command in the container
echo "Running command in Glue container: $CMD"
docker exec -it glue-local-dev bash -c "$CMD"