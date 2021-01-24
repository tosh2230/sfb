"""
Entry point for the CLI
"""
import sys
import argparse

from sfb.entrypoint import EntryPoint

def cli():
    ep = EntryPoint()
    ep.execute()

if __name__ == '__main__':
    cli()