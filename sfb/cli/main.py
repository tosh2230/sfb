"""
Entry point for the CLI
"""
import sys
import argparse

from sfb.bq import Estimator
from sfb.entrypoint import EntryPoint

def cli():
    __entry_point = EntryPoint()
    __entry_point.get_args()
    response = __entry_point.execute()
    print(response)

if __name__ == '__main__':
    cli()