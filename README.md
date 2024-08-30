
# CSV To CloudSQL Dataflow Template
#### Author: Jerome Rajan

This Beam pipeline ingests CSV files from Google Cloud Storage (GCS), and efficiently loads them into a Cloud SQL PostgreSQL database using the `COPY` command. The template is designed for parallel processing, enabling you to load large datasets quickly.

## Problem Statement
Cloud SQL currently does not support running the `gcloud sql import csv` command with multiple CSV files simultaneously. 
This limitation arises because the command relies on the Cloud SQL Admin API, which does not allow concurrent executions. 
This Dataflow template addresses the issue by parallelizing the CSV imports across workers using STDIN and leveraging PostgreSQL's `COPY` command for fast and efficient data import.


## Features

* **Parallel Processing:** Leverages Apache Beam's capabilities to process and load data in parallel, significantly reducing load times for large datasets.
* **CloudSQL Optimized:** Utilizes the `COPY` command, a fast and efficient method for loading data into PostgreSQL, especially for large volumes.
* **Flexible and Configurable:**  Allows customization of parameters like:
    * Source CSV file path pattern in GCS
    * Delimiter used in CSV files
    * Presence or absence of a header row in CSV files
    * Column list to be loaded (matching the target table schema)
    * Cloud SQL connection parameters

## Prerequisites

* **Google Cloud Project:**  You'll need an active Google Cloud Project with the following APIs enabled:
    * Dataflow API
    * Cloud Storage API
    * Cloud SQL Admin API 
* **Cloud SQL Instance:** A provisioned Cloud SQL for PostgreSQL instance.
* **Service Account:** A service account with the necessary permissions to:
    * Read objects from the source GCS bucket.
    * Connect to your Cloud SQL instance and write data to the database.
* **Data in GCS:** Your CSV data should be stored in a GCS bucket.

## Getting Started

1. **Clone the Repository:** Clone this repository to your local machine.
   ```bash
   git clone https://github.com/your-username/dataflow-cloudsql-pgcopy.git
   cd dataflow-cloudsql-pgcopy
   ```

2. **Setup Virtual Environment:**
   ```bash
   python3 -m venv env
   source env/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configuration:**
   Update the following parameters in your execution command to match your environment and data:

   * **`--source_path`:** The path to your CSV files in GCS (e.g., `gs://your-bucket/your-data/*.csv`). You can use wildcards to match multiple files.
   * **`--delimiter`:** The integer ASCII value of the delimiter used in your CSV files. For example, use `44` for comma (`,`) and `9` for tab. 
   * **`--header`:** Set to `TRUE` if your CSV files have a header row; otherwise, set to `FALSE`.
   * **`--column_list`:** A comma-separated list of column names in your CSV files, corresponding to the target table schema.
   * **`--instance_connection_name`:**  The connection name of your Cloud SQL instance in the format `project-id:region:instance-id`.
   * **`--database_name`:** The name of the database in your Cloud SQL instance where the data will be loaded.
   * **`--database_user`:** The username of the Cloud SQL user (or service account email without the `.gserviceaccount.com` suffix) authorized to access the database.
   * **`--table_name`:**  The name of the target table (including schema, if applicable) where the data will be loaded.
   * **`--runner`:** The Apache Beam runner. For testing use `DirectRunner` . For running on Google Cloud use `DataflowRunner`.


5. **Running the Pipeline:** 
[Click Here for details on building and executing the template](./flex_template/README.md)

## How it Works

1. **File Pattern Matching:** The pipeline starts by identifying all CSV files in GCS that match the provided `source_path` pattern.
2. **Parallel Reads:** Apache Beam reads the matched CSV files in parallel, distributing the workload for efficient processing.
3. **Cloud SQL `COPY` Operation:**  The pipeline uses the `COPY` command to efficiently stream data from the CSV files directly into the specified Cloud SQL table.

## Security

This template prioritizes secure data handling:

* **IAM-Based Authentication:** Leverages Google Cloud's Identity and Access Management (IAM) for secure authentication to both GCS and Cloud SQL. 
* **Private IP:** Connects to your Cloud SQL instance using a private IP address, enhancing security by keeping traffic within Google's internal network. 

## Important Notes

* **Permissions:** Ensure that the service account used to run the pipeline has the required permissions for both GCS and Cloud SQL.
* **Data Types:** The `COPY` command in PostgreSQL is sensitive to data types. Ensure that the data types in your CSV files match the schema of your target table in Cloud SQL to avoid errors during data loading.

This README provides a comprehensive guide to get you started with the Dataflow CloudSQL PGCopy template. Let me know if you have any other questions.
