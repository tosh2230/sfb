"""
Entry point for the CLI
"""
import sys
from sfb.dryrun.estimation import Estimation

def cli():
    if len(sys.argv) == 2:
        estimation = Estimation()
        response = estimation.calc(sys.argv[1])
        print(response)
    else:
        print('Please set a argument: sql_file_path')