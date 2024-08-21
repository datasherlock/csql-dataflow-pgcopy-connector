import configparser
import os
from common.Logger import Logger


class ParseConfigs:
    def __init__(self):
        self.logger = Logger().get_logger()
        self.parser = configparser.ConfigParser(interpolation=None)
        root_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger.info(f"Root directory is {root_dir}")
        config_path = os.path.join(root_dir, 'config.ini')
        self.parser.read(config_path)

    def get_config(self, section, option):
        return self.parser[section][option]


if __name__ == '__main__':
    config = ParseConfigs()
    print(config.get_config('cloudsql', 'query'))
