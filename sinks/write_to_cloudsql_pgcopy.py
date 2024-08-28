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
    def __init__(self, args):
        self.engine = None
        self.instance_connection_name = args.instance_connection_name
        self.database_name = args.database_name
        self.database_user = args.database_user
        self.table_name = args.table_name
        self.delimiter = args.delimiter
        self.header = args.header
        self.columns = args.column_list
        self.logger = Logger().get_logger()
        self.args = args

    def start_bundle(self):
        conn = GetConnection(database_user=self.database_user, database_name=self.database_name, instance_connection_name = self.instance_connection_name)
        self.engine = conn.get_engine()
        self.logger.info("Getting Connection Pool...")

    def copy_data_to_table(self, csv_buffer):
        start_time = datetime.now()
        source_file = CSV(self.delimiter, self.header, self.columns)
        self.logger.info(f"Starting copy operation at {start_time}")

        connection = self.engine.raw_connection()
        cursor = connection.cursor()

        cursor.execute(f"""
            COPY {self.table_name} ({source_file.columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'{chr(source_file.delimiter)}', HEADER {source_file.header})
        """, stream=csv_buffer)
        connection.commit()
        cursor.close()

        end_time = datetime.now()
        self.logger.info(f"Completed copy operation at {end_time}")
        self.logger.info(f"Total time taken for copy operation: {end_time - start_time}")

    def process(self, element):
        gcs_path = element.metadata.path
        storage_client = storage.Client()
        bucket_name, file_path = gcs_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        file_content = blob.download_as_text()

        buffer = StringIO()
        buffer.write(file_content)
        buffer.seek(0)

        self.copy_data_to_table(buffer)

    def finish_bundle(self):
        if self.engine:
            self.engine.dispose()
