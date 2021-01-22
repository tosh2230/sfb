"""
Entry point for the CLI
"""
import sys
from sfb import bq

def cli():
    if len(sys.argv) == 2:
        estimator = bq.Estimator(0)
        response = estimator.check(sys.argv[1])
        print(response)
    else:
        print('Please set a argument: sql_file_path')