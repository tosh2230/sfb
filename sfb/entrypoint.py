import os
import argparse
import json
import logging
import yaml

from bq import Estimator as BqEstimator

BQ = 'bq'
ATHENA = 'athena'

class EntryPoint():
    def __init__(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.__args = self.__get_args()
        self.__logger = self.__get_logger(current_dir)
        self.__conf = self.__get_conf(current_dir)

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

        return parser.parse_args()

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

    def __get_conf(self, current_dir):
        conf = None
        file = f'{current_dir}/config/config.yaml'
        if os.path.isfile(file):
            with open(f'{current_dir}/config/config.yaml') as f:
                conf = yaml.load(f, Loader=yaml.SafeLoader)
        return conf

    def execute(self):
        response = {}
        response['results'] = []

        try:
            conf_dst = self.__conf.get('Global', {}).get('DataSourceType')

            if self.__args.data_source_type in (None, BQ) or conf_dst in (None, BQ):
                __estimator = BqEstimator(logger=self.__logger, timeout=self.__args.timeout, conf=self.__conf)
            elif self.__args.data_source_type in (ATHENA) or conf_dst in (ATHENA):
                return {}
            else:
                raise argparse.ArgumentError

            for sql in self.__args.sql:
                result = __estimator.check(sql)
                response['results'].append(result)
            return response
        except FileNotFoundError as e:
            self.__logger.exception(e, exc_info=False)
            raise e
        except Exception as e:
            self.__logger.exception(e, exc_info=True)
            raise e

########################################
if __name__ == "__main__":
    ep = EntryPoint()
    response = ep.execute()
    if response is not None:
        for result in response['results']:
            print(json.dumps(result, indent=2))