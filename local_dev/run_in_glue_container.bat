@echo off
REM Helper script for running ETL jobs in the AWS Glue 5.0 Docker container on Windows

REM Get the directory of this script and the project root
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."

REM Check if the container is running
docker ps | findstr "glue-local-dev" > nul
if %errorlevel% neq 0 (
  echo Starting Glue container...
  cd "%PROJECT_ROOT%" && docker-compose up -d
  
  REM Wait for the container to start
  echo Waiting for container to start...
  timeout /t 5 /nobreak > nul
)

REM Check if a command was provided
if "%~1"=="" (
  echo Usage: %0 ^<command^> [args...]
  echo Examples:
  echo   %0 python local_dev/test_etl.py --pipeline bronze-python --data-size 1000 --output-dir local_dev/output
  echo   %0 spark-submit --master local[*] src/bronze/spark_ingest.py --JOB_NAME test-job --source_type csv --source_path local_dev/data/sample.csv --target_path local_dev/output/bronze --file_format parquet
  exit /b 1
)

REM Build the command to run
set "CMD=cd /home/glue_user/workspace && %*"

REM Run the command in the container
echo Running command in Glue container: %CMD%
docker exec -it glue-local-dev bash -c "%CMD%"