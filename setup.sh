#!/bin/bash

# test.pypi
python setup.py sdist bdist_wheel
check-manifest
twine upload --repository testpypi dist/*
pip --no-cache-dir install --upgrade --index-url https://test.pypi.org/simple/ sfb

# examples
sfb | jq .
sfb -f ./sql/*.sql | jq .
sfb -f ./sql/*.sql -c ./config/sfb.yaml -v | jq .
