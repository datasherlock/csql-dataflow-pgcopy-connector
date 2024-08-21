
import apache_beam as beam
from apache_beam import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from common.utils import parse_args
from apache_beam.io.fileio import MatchFiles

from sinks.write_to_cloudsql_pgcopy import CopyCsvToPostgres


def print_matched_files(file_metadata):
    file_path = file_metadata.path
    print(f"Matched file: {file_path}")
    return file_metadata


def run(config):
    parser = parse_args()
    args, pipeline_args = parser.parse_known_args()
    project = config.get_config("dataflow", "project")
    region = config.get_config("dataflow", "region")
    service_account = config.get_config("dataflow", "service_account")
    src_path = config.get_config("source", "src_path")
    runner = args.runner

    pipeline_args.extend([
        f'--project={project}',
        f'--region={region}',
        f'--service_account={service_account}',
        f'--no_use_public_ips',
        f'--experiments=use_runner_v2',
        f'--runner={runner}'
    ])

    options = PipelineOptions(pipeline_args)

    p = beam.Pipeline(options=options)

    # Define the Beam pipeline
    (
            p
            | 'MatchFiles' >> beam.io.fileio.MatchFiles(src_path)
            | 'Read Matches' >> beam.io.fileio.ReadMatches()

            # This glob expansion is not parallelizable. All the steps get fused into a single step by the Dataflow
            # runner. For such a fused bundle to parallelize, the first step needs to be parallelizable. To make the
            # pipeline parallelizable, we need to break fusion.This is done by adding a Reshuffle transform
            # https://stackoverflow.com/questions/68821162/how-to-enable-parallel-reading-of-files-in-dataflow
            | 'Reshuffle' >> Reshuffle()
            | 'CopyToPostgres' >> beam.ParDo(CopyCsvToPostgres(config))
    )

    result = p.run()
    result.wait_until_finish()
