import logging
import os
from datetime import datetime
from sources.csv import CSV
import apache_beam as beam
from google.cloud import storage
from io import StringIO

from common.Logger import Logger
from common.get_connection import GetConnection


class CopyCsvToPostgres(beam.DoFn):
    def __init__(self, config):
        self.connection_string = None
        self.engine = None
        self.connection = None
        self.config = config
        self.table_name = config.get_config("cloudsql", "table_name")
        self.delimiter = config.get_config("source", "delimiter")
        self.header = config.get_config("source", "header")
        self.columns = config.get_config("source", "column_list")
        self.instance_name = config.get_config("cloudsql", "instance")
        self.logger = Logger().get_logger()

    def start_bundle(self):
        conn = GetConnection(self.config)
        # self.connection = conn.get_engine().connect()
        self.engine = conn.get_engine()
        self.logger = Logger().get_logger()

    def copy_data_to_table(self, csv_buffer):
        start_time = datetime.now()  # Capture the start time
        source_file = CSV(self.delimiter, self.header, self.columns)
        self.logger.info(f"Starting copy operation at {start_time}")
        connection = self.engine.raw_connection()
        cursor = connection.cursor()
        # cursor.execute(f"TRUNCATE TABLE {self.table_name};") # This hints PG to not use WAL
        cursor.execute(f"""
            COPY {self.table_name} ({source_file.columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'{chr(source_file.delimiter)}', HEADER {source_file.header})
        """, stream=csv_buffer)
        connection.commit()
        cursor.close()

        end_time = datetime.now()  # Capture the end time
        self.logger.info(f"Completed copy operation at {end_time}")
        self.logger.info(f"Total time taken for copy operation: {end_time - start_time}")

    def process(self, element):
        gcs_path = element.metadata.path
        # Download the CSV file from GCS
        storage_client = storage.Client()
        bucket_name, file_path = gcs_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        file_content = blob.download_as_text()

        # The cursor.copy_expert method in the psycopg2 library allows you to use custom SQL commands with the COPY
        # operation. When using COPY FROM STDIN, PostgreSQL expects the input data to be streamed as if it were being
        # read from a standard input or a file. StringIO is an in-memory stream that behaves like a file object and
        # establishes compatibility with psycopg2 copy_expert
        buffer = StringIO()
        buffer.write(file_content)
        buffer.seek(0)

        # Copy the data from the buffer to the PostgreSQL table
        self.copy_data_to_table(buffer)

    def finish_bundle(self):
        if self.connection:
            self.connection.close()
