"""
Entry point for the CLI
"""
import sys
import json
import argparse

from sfb.entrypoint import EntryPoint

def cli():
    ep = EntryPoint()
    for i, result in enumerate(ep.execute()):
        print(json.dumps(result, indent=2))
