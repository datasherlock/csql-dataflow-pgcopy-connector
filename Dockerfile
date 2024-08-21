# Use the official Apache Beam SDK image as the base image
FROM python:3.12-slim as base

# Install the required Python packages
RUN pip install --no-cache-dir \
    google-cloud \
    apache-beam[gcp] \
    sqlalchemy \
    google-cloud-secret-manager \
    psycopg2-binary \
    pandas \
    google-auth \
    pg8000 \
    google-cloud-storage \
    setuptools \
    cloud-sql-python-connector

 # Copy files from the official SDK image, including script/dependencies
COPY --from=apache/beam_python3.12_sdk:2.58.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to execute the Beam pipeline with the Cloud SQL Proxy running
ENTRYPOINT ["/opt/apache/beam/boot"]
