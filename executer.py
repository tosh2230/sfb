"""
for local debugging
"""
import sys
import json

from sfb.entrypoint import EntryPoint

def main():
    results = {"Succeeded": [], "Failed": []}
    ep = EntryPoint()
    for response in ep.execute():
        results[response['Status']].append(response['Result'])

    print(json.dumps(results, indent=2))
    exit(0)

if __name__ == "__main__":
    main()