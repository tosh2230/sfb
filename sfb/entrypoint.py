import os
import argparse
import json
import logging
import yaml

from bq import Estimator as BqEstimator

CONFIG_FILE = 'config/sfb.yaml'
BQ = 'BigQuery'
ATHENA = 'Athena'

class EntryPoint():
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.__args = self.__get_args()
        self.__config = self.__get_config(current_dir)

        if self.__args.debug:
            self.__logger = self.__get_logger(current_dir)
        else:
            self.__logger = None

    def __get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-s", "--sql", 
            help="sql_file_path",
            type=str,
            nargs='*',
            required=True
        )
        parser.add_argument(
            "-d", "--data_source_type",
            help="ser data_source_type",
            type=str,
            choices=[BQ, ATHENA]
        )
        parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds",
            type=float,
        )
        parser.add_argument(
            '--debug',
            action='store_true'
        )

        return parser.parse_args()

    def __get_config(self, current_dir):
        config = None
        file = f'{current_dir}/{CONFIG_FILE}'
        if os.path.isfile(file):
            with open(file) as f:
                config = yaml.load(f, Loader=yaml.SafeLoader)
        return config

    def __get_logger(self, current_dir):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARNING)

        log_dir = f'{current_dir}/log'
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)

        handler = logging.FileHandler(filename=f"{log_dir}/error.log")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)8s %(message)s"))
        logger.addHandler(handler)

        return logger

    def execute(self):
        response = {}
        response['results'] = []

        try:
            print(json.dumps(self.__config, indent=2))
            config_file_list = self.__config.get('QueryFiles')

            for sql in self.__args.sql:
                estimator = None
                file_name = sql.split('/')[-1]
                service = config_file_list[file_name]['Service']

                if self.__args.data_source_type in (None, BQ) or service in (None, BQ):
                    estimator = BqEstimator(
                        logger=self.__logger,
                        timeout=self.__args.timeout,
                        config=self.__config
                    )
                elif self.__args.data_source_type == ATHENA or service in (ATHENA):
                    return {"Athena": "Now Coding..."}
                else:
                    raise argparse.ArgumentError

                result = estimator.check(sql)
                response['results'].append(result)

            return response

        except (FileNotFoundError, KeyError) as e:
            if self.__logger:
                self.__logger.exception(e, exc_info=False)
            raise e

        except Exception as e:
            self.__logger.exception(e, exc_info=True)
            raise e

########################################
if __name__ == "__main__":
    ep = EntryPoint()
    response = ep.execute()
    if response is  None:
        print('SQL files are not found.')
    for result in response['results']:
        print(json.dumps(result, indent=2))