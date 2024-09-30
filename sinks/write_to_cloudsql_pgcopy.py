from datetime import datetime
from sources.csv import CSV
import apache_beam as beam
from io import StringIO
import uuid
from common.Logger import Logger
from common.get_connection import GetConnection


class CopyCsvToPostgres(beam.DoFn):
    """
    A Beam DoFn class to copy CSV data into PostgreSQL partitions.

    Lifecycle Methods:
    - setup: Establishes a database connection once per worker.
    - process: Processes each element and writes the data to the appropriate partition in PostgreSQL.
    - teardown: Closes the database connection when the worker finishes.

    Args:
    - args: Configuration parameters for database and CSV setup.
    """

    def __init__(self, args):
        # Initialize parameters and logger
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
        self.quote = args.quote
        self.connection = None
        self.cursor = None

    def setup(self):
        """
        Called once per worker to create a database connection.
        This is useful for avoiding the overhead of repeatedly opening a connection for each bundle.
        """
        conn = GetConnection(database_user=self.database_user, database_name=self.database_name,
                             instance_connection_name=self.instance_connection_name)
        # Create an engine to connect to the database
        self.engine = conn.get_engine()
        self.logger.info("Creating CloudSQL Connection For The Worker...")

        # Get the raw connection and create a cursor for executing SQL statements
        self.connection = self.engine.raw_connection()
        self.cursor = self.connection.cursor()

    def copy_data_to_table(self, csv_buffer, partition_key):
        """
        Copies CSV data to the specified partition in the PostgreSQL table.

        Args:
        - csv_buffer: Buffered CSV data to be copied.
        - partition_key: The partition key used to determine the table partition.
        """
        start_time = datetime.now()

        # Create a CSV object to hold file-specific details
        source_file = CSV(self.delimiter, self.quote, self.header, self.columns)

        # Generate a unique ID for this copy operation for easy tracking
        unique_id = uuid.uuid4()

        # Generate the COPY statement, including partition information
        copy_statement = f"""
           {unique_id} - COPY {self.table_name}_{partition_key} ({source_file.columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'{chr(source_file.delimiter)}', HEADER {source_file.header}, QUOTE E'{chr(source_file.quote)}')
        """
        # Log the start of the copy operation
        self.logger.info(f"Starting copy operation at {start_time}")
        self.logger.info(copy_statement)

        # Execute the COPY statement using the cursor
        self.cursor.execute(copy_statement, stream=csv_buffer)

        # Commit the transaction to make changes persistent
        self.connection.commit()

        # Close the cursor to free resources
        self.cursor.close()

        end_time = datetime.now()

        # Log the end of the copy operation
        self.logger.info(f"Completed copy operation with ID - {unique_id} at {end_time}")
        self.logger.info(f"Total time taken for copy operation with ID {unique_id}: {end_time - start_time}")

    def process(self, element):
        """
        Processes each element to extract partition key and records,
        and then copies the data to the appropriate PostgreSQL partition.

        Args:
        - element: A tuple where element[0] is the partition key, and element[1] is the list of CSV records.
        """
        partition_key, records = element

        # Create an in-memory buffer to hold CSV records
        buffer = StringIO()

        # Write each record to the buffer, adding a newline character
        for record in records:
            buffer.write(record + "\n")

        # Reset the buffer's position to the beginning
        buffer.seek(0)

        # Copy the data to the corresponding PostgreSQL table partition
        self.copy_data_to_table(buffer, partition_key)

    def teardown(self):
        """
        Called once per worker to release any resources that were set up.
        Here, it closes the database connection and disposes of the engine.
        """
        # Close the connection if it exists
        if self.connection is not None:
            self.connection.close()

        # Dispose of the engine if it exists
        if self.engine is not None:
            self.engine.dispose()

        # Log that the database connection has been closed
        print("Database connection closed")
