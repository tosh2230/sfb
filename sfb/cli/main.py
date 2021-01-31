"""
Entry point for the CLI
"""
import sys
import json
import argparse

from sfb.entrypoint import EntryPoint

def cli():
    results = []
    ep = EntryPoint()
    for result in ep.execute():
        results.append(result)

    print(json.dumps(results, indent=2))
    sys.exit(0)
