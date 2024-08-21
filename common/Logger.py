import logging


class Logger(object):
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

    def get_logger(self):
        return self.logger
