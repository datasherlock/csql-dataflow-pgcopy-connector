
# DataflowToCloudSQL
#### Developed By: Jerome Rajan, Staff Solutions Consultant, Google

## Overview

`DataflowToCloudSQL` is a Python-based project that integrates Apache Beam pipelines with Google Cloud SQL. 
The project is structured to allow reading data from a GCS bucket, and writing the results to a GCP CloudSQL Postgres
database. The primary aim of this repo is to demonstrate connecting to CloudSQL from Dataflow :
- Using a private IP
- Using IAM authentication
- Without a CloudSQL Auth Proxy

The solution uses SQLAlchemy's bulk loading capabilities in conjunction with Beam's distributed framework. 

## Project Structure

- **common/**: Contains common utility modules used across the project.
  - `config.ini`: Configuration file for storing database connection details and other dataflow configurations.
  - `get_connection.py`: Module for establishing a connection to Google Cloud SQL.
  - `Logger.py`: Module for logging throughout the application.
  - `parse_configs.py`: Handles the parsing of the configuration file (`config.ini`).
  - `utils.py`: Additional utility functions used across the project.

- **pipelines/**: Directory intended for storing Apache Beam pipeline definitions.

- **sinks/**: Directory intended for modules related to data sinks, such as CloudSQL

- **sources/**: Directory intended for modules related to data sources.
  - `read_from_source.py`: Module for reading data from the specified source. This is a sample record generator to demonstrate the CloudSQL write capability

- **Dockerfile**: Docker configuration for containerizing the application.

- **main.py**: The main entry point of the application that initializes and runs the Apache Beam pipeline.

- **requirements.txt**: Python dependencies required for the project.

- **setup.py**: Script for installing the project and its dependencies.

## Configuration

The application uses a `config.ini` file located in the `common/` directory for configuration. This file should contain necessary details such as database credentials, connection strings, and other configurable parameters.

Example `config.ini`:

```ini
[cloudsql]
instance=project:region:cloudsql-instance
database=postgres
schema=public
user=dataflow-sa@project_name.iam
password=''
batch=10000

[dataflow]
project=project_name
temp_location=""
staging_location=""
region=us-central1
service_account=dataflow-sa@project_name.iam.gserviceaccount.com

[source]
src_path=gs://bucket_name/df_big/output*
```

Copy the config.ini to a GCS path
```
gcloud storage cp common/config.ini gs://bucket_name/dataflowtocsql/config/config.ini
```

To build image using `gcloud builds` - 
```
gcloud builds submit --tag us-central1-docker.pkg.dev/project_name/repo_name/dataflow/dataflow2csql:1.0
```

Run the job with DataflowRunner - 
```
python main.py --config_path="gs://bucket_name/dataflowtocsql/config/config.ini" --runner=DataflowRunner --sdk_container_image="us-central1-docker.pkg.dev/project_name/repo_name/dataflow/dataflow2csql:2.0" --setup_file=./setup.py
```
