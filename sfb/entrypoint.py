import argparse

from bq import Estimator

class EntryPoint():
    def __init__(self):
        pass

    def get_args(self):
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

        return self
    
    def execute(self):
        __estimator = Estimator(timeout=self.__args.timeout)

        try:
            for sql in self.__args.sql:
                print(__estimator.check(sql))
        except FileNotFoundError as e:
            print(e)
        finally:
            return self

########################################
if __name__ == "__main__":
    ep = EntryPoint()
    ep.get_args().execute()