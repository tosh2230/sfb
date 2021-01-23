"""
Entry point for the CLI
"""
import sys
import argparse

from sfb.bq import Estimator

def cli():
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
    __args = __parser.parse_args()

    print(f"target_file: {__args.sql_file_path}")
    __estimator = Estimator(timeout=__args.timeout)
    __response = __estimator.check(__args.sql_file_path)
    print(__response)

if __name__ == '__main__':
    cli()