import os
import sys
import argparse
import json
import logging
import yaml

from services.bq import BigQueryEstimator

CONFIG_FILE = 'config/sfb.yaml'
BQ = 'BigQuery'
LOG_FORMAT = '%(asctime)s %(levelname)8s %(message)s'

class EntryPoint():

    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.__args = self.__get_args()
        self.__config = self.__get_config(current_dir)

        if self.__args.debug:
            self.__logger = self.__get_logger(current_dir)
        else:
            self.__logger = None

    def __get_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()

        # required
        group = parser.add_mutually_exclusive_group(required = True)
        group.add_argument(
            "-f", "--file",
            help="sql_file_path",
            type=str,
            nargs='*',
            default=None
        )
        group.add_argument(
            "-q", "--query",
            help="query_string",
            type=str,
            default=None
        )

        # optional
        parser.add_argument(
            "-s", "--source",
            help="source_type",
            type=str,
            choices=[BQ],
            default=BQ
        )
        parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds",
            type=float,
            default=None
        )
        parser.add_argument(
            '-v', '--verbose',
            help="verbose results",
            action='store_true',
            default=False
        )
        parser.add_argument(
            '-d', '--debug',
            help="for debugging",
            action='store_true',
            default=False
        )

        return parser.parse_args()

    def __get_config(self, current_dir: str) -> dict:
        config = None
        file = f'{current_dir}/{CONFIG_FILE}'
        if os.path.isfile(file):
            with open(file) as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
        return config

    def __get_logger(self, current_dir: str) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARNING)

        log_dir = f'{current_dir}/log'
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)

        handler = logging.FileHandler(filename=f'{log_dir}/error.log')
        handler.setFormatter(logging.Formatter(f'{LOG_FORMAT}'))
        logger.addHandler(handler)

        return logger

    def execute(self) -> dict:
        try:
            estimator = BigQueryEstimator(
                config_query_files=self.__config['QueryFiles'],
                logger=self.__logger,
                timeout=self.__args.timeout,
                verbose=self.__args.verbose
            )

            if self.__args.file:
                for sql in self.__args.file:
                    yield estimator.check_file(sql)
            elif self.__args.query:
                yield estimator.check_query(self.__args.query)

        except (FileNotFoundError, KeyError) as e:
            if self.__logger:
                self.__logger.exception(e, exc_info=False)
            raise e

        except Exception as e:
            if self.__logger:
                self.__logger.exception(e, exc_info=True)
            raise e

########################################
if __name__ == "__main__":
    results = []
    ep = EntryPoint()
    for i, result in enumerate(ep.execute()):
        results.append(result)

    print(json.dumps(results, indent=2))
    sys.exit(0)