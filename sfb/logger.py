import os
import logging

class SfbLogger():

    LOG_FORMAT = '%(asctime)s %(levelname)8s %(message)s'
    LOG_FILE = 'sfb.log'

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.WARNING)

    def set_logger(self) -> None:
        log_dir = os.getcwd() + '/log'
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)

        log_filename = f'{log_dir}/{self.LOG_FILE}'
        handler = logging.FileHandler(filename=log_filename)
        handler.setFormatter(logging.Formatter(f'{self.LOG_FORMAT}'))
        self.logger.addHandler(handler)
