"""
Entry point for the CLI
"""
import sys
from sfb.bq import Estimator

def cli():
    if len(sys.argv) == 2:
        estimator = Estimator(timeout=None)
        response = estimator.check(sys.argv[1])
        print(response)
    else:
        print('Please set a argument: sql_file_path')

if __name__ == '__main__':
    cli()