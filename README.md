# AWS Data Lake Framework (Medallion Architecture)

A comprehensive framework for building and managing a data lake on AWS based on the medallion architecture.

## Architecture Overview

This framework implements a medallion architecture with:

- **Bronze Layer**: Raw data stored in S3
- **Silver Layer**: Refined data using S3Tables
- **Gold Layer**: (Not implemented in this version)

## Features

- AWS Glue 5.0 ETL pipelines (Spark and Python shell)
- Comprehensive logging and error handling
- Configuration management
- Testing framework
- CI/CD pipeline integration
- Local development environment

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
└── README.md           # This file
```

## Getting Started

See the [Getting Started Guide](docs/getting_started.md) for detailed instructions on setting up and using this framework.

## Local Development

For local development and testing, see the [Local Development Guide](docs/local_development.md).

## License

MIT