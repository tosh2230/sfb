import os
import sys
import argparse
import json
import logging
import yaml

from sfb.services.bq import BigQueryEstimator

CONFIG_FILE = 'sfb.yaml'
LOG_FILE = 'sfb.log'
BQ = 'BigQuery'
LOG_FORMAT = '%(asctime)s %(levelname)8s %(message)s'

class EntryPoint():

    def __init__(self):
        self.__args = self.__get_args()
        if self.__args.config:
            self.__config = self.__get_config(self.__args.config)
        else:
            self.__config = None

        if self.__args.debug:
            self.__logger = self.__get_logger()
        else:
            self.__logger = None

    def __get_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()

        # required
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-f", "--file",
            help="sql_file_path", type=str, nargs='*'
        )
        group.add_argument(
            "-q", "--query",
            help="query_string", type=str, default=None
        )

        # optional
        parser.add_argument(
            "-c", "--config",
            help="config_file_path", type=str, default=f"./config/{CONFIG_FILE}"
        )
        parser.add_argument(
            "-s", "--source",
            help="source_type", type=str, choices=[BQ], default=BQ
        )
        parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds", type=float, default=None
        )
        parser.add_argument(
            '-v', '--verbose',
            help="verbose results", action='store_true', default=False
        )
        parser.add_argument(
            '-d', '--debug',
            help="logging for debug", action='store_true', default=False
        )

        return parser.parse_args()

    def __get_config(self, config_file_path: str) -> dict:
        config = None
        if os.path.isfile(config_file_path):
            with open(config_file_path) as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
        return config

    def __get_logger(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARNING)

        log_dir = os.getcwd() + '/log'
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)

        log_filename = f'{log_dir}/sfb.log'
        handler = logging.FileHandler(filename=log_filename)
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

            if self.__args.query is None:
                if self.__args.file:
                    files = self.__args.file
                else:
                    current_dir = os.getcwd() + '/sql'
                    files = [f'{current_dir}/{file}' for file in os.listdir(current_dir)]

                for sql in files:
                    yield estimator.check_file(sql)
            else:
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
    for result in ep.execute():
        results.append(result)

    print(json.dumps(results, indent=2))
    sys.exit(0)
