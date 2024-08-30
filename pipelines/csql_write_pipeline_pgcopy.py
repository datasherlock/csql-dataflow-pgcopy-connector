import apache_beam as beam
from apache_beam import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from common.get_connection import GetConnection

from common.Logger import Logger
from apache_beam.io.fileio import MatchFiles

from common.utils import parse_args
from sinks.write_to_cloudsql_pgcopy import CopyCsvToPostgres


def run():
    parser = parse_args()
    args, pipeline_args = parser.parse_known_args()
    logger = Logger().get_logger()

    pipeline_args.extend([
       f'--runner={args.runner}'
    ])

    options = PipelineOptions(pipeline_args)

    p = beam.Pipeline(options=options)

    # Define the Beam pipeline
    (
            p
            | 'MatchFiles' >> beam.io.fileio.MatchFiles(args.source_path.strip('"'))
            | 'Read Matches' >> beam.io.fileio.ReadMatches()

            # This glob expansion is not parallelizable. All the steps get fused into a single step by the Dataflow
            # runner. For such a fused bundle to parallelize, the first step needs to be parallelizable. To make the
            # pipeline parallelizable, we need to break fusion.This is done by adding a Reshuffle transform
            # https://stackoverflow.com/questions/68821162/how-to-enable-parallel-reading-of-files-in-dataflow
            | 'Reshuffle' >> Reshuffle()
            | 'CopyToPostgres' >> beam.ParDo(CopyCsvToPostgres(args))
    )

    result = p.run()
    result.wait_until_finish()

    conn = GetConnection(database_user=args.database_user, database_name=args.database_name,
                         instance_connection_name=args.instance_connection_name)
    pool = conn.get_engine()
    connection = pool.raw_connection()
    cursor = connection.cursor()
    logger.info("Calling pp procedure...")
    cursor.callproc(procname='public.test_proc', parameters=['public.demo_part_tbl', 'public.test_part'])
    logger.info(f"Proc executed successfully")

    logger.info('Running sample DML')

    query = f"""INSERT INTO public.test_part(col_1, col_2)	VALUES (1, '2');"""
    cursor.execute(query)
    connection.commit()
    cursor.close()
    logger.info(f'Executed query: {query}')
