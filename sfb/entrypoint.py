import argparse
import json

from bq import Estimator

class EntryPoint():
    def __init__(self):
        __parser = argparse.ArgumentParser()
        __parser.add_argument(
            "-s", "--sql", 
            help="sql path",
            type=str,
            nargs='*',
            required=True
        )
        __parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds",
            type=float,
        )

        self.__args = __parser.parse_args()

    def execute(self):
        __estimator = Estimator(timeout=self.__args.timeout)
        response = {}
        response['results'] = []

        try:
            for sql in self.__args.sql:
                result = __estimator.check(sql)
                response['results'].append(result)
            return response
        except FileNotFoundError as e:
            print(e)

########################################
if __name__ == "__main__":
    ep = EntryPoint()
    response = ep.execute()
    for result in response['results']:
        print(json.dumps((result), indent=2))