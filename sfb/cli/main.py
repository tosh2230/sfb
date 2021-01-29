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
    for i, result in enumerate(cli()):
        if result is None:
            print('SQL files are not found.')
        else:
            print(json.dumps(result, indent=2))