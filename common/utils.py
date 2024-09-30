import argparse
from google.cloud import storage
from common.Logger import Logger

logger = Logger().get_logger()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dataflow Template for Parallel COPY of multiple CSVs to CloudSQL PostgreSQL"
    )

    parser.add_argument(
        '--source_path',
        type=str,
        required=True,
        help="The source path in GCS containing the CSV files to be loaded. Use a regex to match multiple files"
    )

    parser.add_argument(
        '--delimiter',
        type=int,
        required=True,
        help="The integer ASCII value of the character used to separate fields in the CSV files (e.g., comma, tab)."
    )

    parser.add_argument(
        '--header',
        type=str,
        choices=['TRUE', 'FALSE'],
        required=True,
        help="Indicates whether the CSV files contain a header row (TRUE or FALSE)."
    )

    parser.add_argument(
        '--column_list',
        type=str,
        required=True,
        help="A comma-separated list of column names corresponding to the fields in the CSV files. This should match "
             "the target table schema."
    )

    parser.add_argument(
        '--instance_connection_name',
        type=str,
        required=True,
        help="The CloudSQL instance connection name that is in the format project_id:region:instance_id"
    )

    parser.add_argument(
        '--database_name',
        type=str,
        required=True,
        help="The name of the target PostgreSQL database in Cloud SQL where the data will be loaded."
    )

    parser.add_argument(
        '--database_user',
        type=str,
        required=True,
        help="The user or IAM service account email to use for connecting to the Cloud SQL instance (without the "
             ".gserviceaccount.com suffix)."
    )

    parser.add_argument(
        '--table_name',
        type=str,
        required=True,
        help="The name of the target table in PostgreSQL where the CSV data will be loaded. This should be in the "
             "format schema_name.table_name"
    )

    parser.add_argument(
        '--runner',
        type=str,
        required=False,
        default="DirectRunner",
        help="The Beam runner to be used to execute the job."
    )

    return parser

