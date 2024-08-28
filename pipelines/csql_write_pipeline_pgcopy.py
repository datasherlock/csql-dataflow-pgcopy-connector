import apache_beam as beam
from apache_beam import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from common.utils import parse_args
from apache_beam.io.fileio import MatchFiles
from sinks.write_to_cloudsql_pgcopy import CopyCsvToPostgres


def run():
    parser = parse_args()
    args, pipeline_args = parser.parse_known_args()

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
