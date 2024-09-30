import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles

from common.utils import parse_args
from sinks.write_to_cloudsql_pgcopy import CopyCsvToPostgres
from sources.csv import ExtractPartitionKey
from common.Logger import Logger
from common.get_connection import GetConnection


def run():
    """
    This function defines and runs an Apache Beam pipeline that processes CSV files,
    extracts partition keys, and loads the data into a partitioned PostgreSQL table.

    Steps in the Pipeline:
    - Match files from a source path.
    - Read matched files' metadata.
    - Extract file paths.
    - Chunk data using ReadAllFromText.
    - Extract partition keys from each line of CSV data.
    - Group the records by partition key.
    - Load the grouped data into a PostgreSQL table.
    """
    # Parse command-line arguments
    parser = parse_args()
    args, pipeline_args = parser.parse_known_args()

    # Extend the pipeline arguments to include the runner type
    pipeline_args.extend([
        f'--runner={args.runner}'
    ])

    # Set up pipeline options and create a new pipeline
    options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=options)

    # Define the Beam pipeline
    (
            p
            | 'MatchFiles' >> beam.io.fileio.MatchFiles(args.source_path.strip('"'))
            # Match all the files based on the provided source path. This allows for wildcard matching (e.g., "*.csv").

            | 'Read Matches' >> beam.io.fileio.ReadMatches()
            # Read the matched files. Produces a PCollection of ReadableFile objects that contain metadata.

            | 'Extract File Paths' >> beam.Map(lambda element: element.metadata.path)
            # Extract only the file paths from the metadata of each matched file. This is useful for reading the actual content.

            | 'Chunk Data' >> beam.io.textio.ReadAllFromText(desired_bundle_size=1073741824, skip_header_lines=0)
            # Read all data from the extracted file paths in chunks.
            # `desired_bundle_size=1073741824` sets the bundle size to 1 GB, which controls how much data each worker will process at a time.

            | 'Extract Partition Key' >> beam.ParDo(ExtractPartitionKey(args))
            # Use a ParDo to extract a partition key from each line of the CSV data. The partition key helps to determine which partition
            # of the target table the data belongs to. The `ExtractPartitionKey` class should yield a tuple of (partition_key, record).

            | 'Group by Partition Key' >> beam.GroupByKey()
            # Group the records by the extracted partition key. After this step, each key is associated with an iterable of records that belong
            # to the same partition. This makes the data ready for bulk insert operations into the correct table partitions.

            | 'CopyToPostgres' >> beam.ParDo(CopyCsvToPostgres(args))
        # For each group of records (partition_key, records), perform a bulk insert into the corresponding partitioned PostgreSQL table.
        # `CopyCsvToPostgres` handles the logic of writing each group of records to the appropriate partition.
    )

    # Run the pipeline and wait for completion
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
