import os

from common.utils import download_file, parse_args
from common.parse_configs import ParseConfigs
from pipelines.csql_write_pipeline_pgcopy import run
from common.Logger import Logger


def main():
    print("Reading Configs")
    parser = parse_args()
    known_args, _ = parser.parse_known_args()
    root_dir = os.path.dirname(os.path.abspath(__file__))
    tgt_path = os.path.join(root_dir, 'common/config.ini')
    download_file(known_args.config_path, tgt_path)
    config = ParseConfigs()
    run(config)


if __name__ == '__main__':
    main()
