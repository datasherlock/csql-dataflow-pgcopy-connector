# Dataflow Flex Template for Cloud SQL PostgreSQL Connector

This repository provides a Dockerized Apache Beam Flex Template for running a Dataflow pipeline that loads CSV files into a Cloud SQL PostgreSQL database using the `COPY` command.

## Prerequisites

- Google Cloud SDK (`gcloud`) installed and configured.
- Docker installed and running on your local machine.
- Access to Google Cloud services, including Dataflow and Cloud SQL.

## Build and Push Using Cloud Build

```google cloud
gcloud builds submit --tag <region-docker-registry>/<project-id>/<repository>/<docker-image-name> <path-to-dockerfile>
```
## OR

## Build and Push Docker Image

### Step 1: Build the Docker Image

Build the Docker image for the Dataflow Flex Template.

```bash
docker build -t <docker-image-name> -f <path-to-dockerfile> .
```
Replace <docker-image-name> with your desired Docker image name and <path-to-dockerfile> with the relative path to your Dockerfile.

### Step 2: Tag the Docker Image
Tag the Docker image with your Artifact Registry or Container Registry path.
```bash
docker tag <docker-image-name> <region-docker-registry>/<project-id>/<repository>/<docker-image-name>
```
Replace:
- \<docker-image-name> with your Docker image name.
- \<region-docker-registry> with your Google Cloud region and registry (e.g., europe-west2-docker.pkg.dev).
- \<project-id> with your Google Cloud project ID.
- \<repository> with your Artifact Registry or Container Registry repository name.

### Step 3: Authenticate Docker with Google Cloud
Authenticate your Docker client with Google Cloud to push the image.

```bash
gcloud auth print-access-token --impersonate-service-account <service-account-email> | docker login \
  -u oauth2accesstoken \
  --password-stdin https://<region-docker-registry>
```
Replace <service-account-email> with the email of the service account you want to impersonate, and <region-docker-registry> with your registry.

### Step 4: Push the Docker Image to Google Cloud
Push the tagged Docker image to your Google Cloud registry.
```bash
docker push <region-docker-registry>/<project-id>/<repository>/<docker-image-name>
```

## Deploy Dataflow Flex Template
### Step 1: Build the Flex Template
Build the Dataflow Flex Template JSON using the image you pushed.
```bash
gcloud dataflow flex-template build gs://<bucket-name>/<template-file-name>.json \
  --image "<region-docker-registry>/<project-id>/<repository>/<docker-image-name>:latest" \
  --sdk-language "PYTHON" \
  --metadata-file "flex_template/metadata.json" 
```
Replace:
- \<bucket-name> with the name of your Google Cloud Storage bucket where the template JSON will be stored.
- \<template-file-name> with the name for your template JSON file.

### Step 2: Run the Dataflow Job
Run the Dataflow job using the Flex Template.

```bash
gcloud dataflow flex-template run "<job-name>" \
  --template-file-gcs-location gs://<bucket-name>/<template-file-name>.json \
  --region <region> \
  --parameters source_path="gs://<bucket-name>/<input-path>/*" \
  --parameters delimiter=<delimiter-ascii-value> \
  --parameters header=<TRUE-or-FALSE> \
  --parameters ^~^column_list="<column1>,<column2>" \
  --parameters instance_connection_name="<project-id>:<region>:<instance-id>" \
  --parameters database_name=<database-name> \
  --parameters database_user="<service-account-email>" \
  --parameters table_name="<schema-name>.<table-name>" \
  --project "<project-id>" \
  --parameters sdk_container_image="<sdk_container_image>" \
  --service-account-email <service-account-email> \
  --subnetwork="https://www.googleapis.com/compute/v1/projects/<project-id>/regions/<region>/subnetworks/<subnetwork-name>" \
  --disable-public-ips \
  --max-workers=200 \
  --num-workers=100 \
  --worker-machine-type=n2d-standard-16
```
Replace:
* \<job-name> with your desired job name.
* \<bucket-name> with the name of your GCS bucket.
* \<input-path> with the path to your input files in GCS.
* \<target-table> with the target table in your Cloud SQL PostgreSQL database.
* \<config-path> with the path to your configuration file in GCS.
* \<project-id> with your Google Cloud project ID.
* \<service-account-email> with the email of the service account to run the Dataflow job.
* \<region> with your Google Cloud region.
* \<subnetwork-name> with your subnetwork's name.

### Step 3: Running the Dataflow job from Composer/Airflow

You can also run the Dataflow Flex Template using an Airflow DAG. Below is an example configuration for running the template:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'csql-dataflow-pgcopy-dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start_flex_template = DataflowStartFlexTemplateOperator(
        task_id='start_flex_template',
        body={
            'launchParameter': {
                'jobName': 'csql-dataflow-pgcopy',
                'containerSpecGcsPath': 'gs://<your-bucket>/<path-to-json>/csql_dataflow_pgcopy-py.json',
                'parameters': {
                    'source_path': 'gs://<your-bucket>/<path-to-input-files>/*',
                    'delimiter': '29',  # ASCII value for the delimiter
                    'header': 'TRUE',  # Indicating whether the CSV files contain a header row
                    'column_list': 'col1,col2,col3',  # Comma-separated list of columns
                    'instance_connection_name': '<your-project>:<region>:<instance-id>',
                    'database_name': '<your-database-name>',
                    'database_user': '<your-database-user>',
                    'table_name': '<your-schema>.<your-table>',
                },
                'environment': {
                    'maxWorkers': 200,
                    'numWorkers': 100,
                    'serviceAccountEmail': '<service-account>@<your-project>.iam.gserviceaccount.com',
                    'machineType': 'n2d-standard-32',
                    'sdkContainerImage': '<region-docker-registry>/<project-id>/<repository>/<docker-image-name>',
                    'workerRegion': '<region_name>',
                    'subnetwork': 'https://www.googleapis.com/compute/v1/projects/<your-project>/regions/<region_name>/subnetworks/<your-subnetwork>',
                }
            }
        },
        location='<region_name>',
        project_id='<your-project-id>',
        gcp_conn_id='google_cloud_default',
    )

    start_flex_template
```

### Step 4: Run locally using Direct Runner
```bash
python your_pipeline_script.py \
    --runner=DirectRunner \
    --source_path=gs://your-bucket/path/to/input/files/* \
    --delimiter=44 \
    --header=TRUE \
    --column_list=col1,col2,col3 \
    --instance_connection_name=your-project:region:instance-id \
    --database_name=your_database_name \
    --database_user=your_database_user \
    --table_name=your_schema.your_table
```

