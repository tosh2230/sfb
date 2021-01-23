import argparse

from bq import Estimator

class EntryPoint():
    def __init__(self):
        pass

    def get_args(self):
        __parser = argparse.ArgumentParser()
        __parser.add_argument(
            "sql_file_path", 
            help="sql file path",
            type=str,
        )
        __parser.add_argument(
            "-t", "--timeout", 
            help="request timeout seconds",
            type=float,
        )

        self.__args = __parser.parse_args()
    
    def execute(self):
        print(f"target_file: {self.__args.sql_file_path}")
        __estimator = Estimator(timeout=self.__args.timeout)
        __response = __estimator.check(self.__args.sql_file_path)
        return __response

if __name__ == "__main__":
    __entry_point = EntryPoint()
    __entry_point.get_args()
    response = __entry_point.execute()
    print(response)