"""
Entry point for the CLI
"""
import sys
import argparse

from sfb.entrypoint import EntryPoint

def cli():
    ep = EntryPoint()
    return ep.execute()

if __name__ == '__main__':
    response = cli()
    if 'results' in response:
        for result in response['results']:
            print(json.dumps((result), indent=2))