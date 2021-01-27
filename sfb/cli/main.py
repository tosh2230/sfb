"""
Entry point for the CLI
"""
import sys
import json
import argparse

from sfb.entrypoint import EntryPoint

def cli():
    ep = EntryPoint()
    return ep.execute()

if __name__ == '__main__':
    response = cli()
    if response is  None:
        print('SQL files are not found.')
    for result in response:
        print(json.dumps(result, indent=2))