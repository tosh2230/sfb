import os
import argparse
import json
import logging

from bq import Estimator as BqEstimator

BQ = 'bq'
ATHENA = 'athena'

current_dir = os.path.dirname(os.path.abspath(__file__))

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(filename=f"{current_dir}/log/error.log")
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)8s %(message)s"))
logger.addHandler(handler)

class EntryPoint():
    def __init__(self):
        __parser = argparse.ArgumentParser()
        __parser.add_argument(
            "-s", "--sql", 
            help="sql_file_path",
            type=str,
            nargs='*',
            required=True
        )
        __parser.add_argument(
            "-d", "--data_source_type",
            help="ser data_source_type",
            type=str,
            choices=[BQ, ATHENA]
        )
        __parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds",
            type=float,
        )

        self.__args = __parser.parse_args()

    def execute(self):
        response = {}
        response['results'] = []

        try:
            if self.__args.data_source_type in (BQ, None):
                __estimator = BqEstimator(logger=logger, timeout=self.__args.timeout)
            elif self.__args.data_source_type in (ATHENA):
                return {}
            else:
                raise argparse.ArgumentError

            for sql in self.__args.sql:
                result = __estimator.check(sql)
                response['results'].append(result)
            return response
        except FileNotFoundError as e:
            print(e)
        except Exception as e:
            raise e

########################################
if __name__ == "__main__":
    ep = EntryPoint()
    response = ep.execute()
    if 'results' in response:
        for result in response['results']:
            print(json.dumps((result), indent=2))