#!/bin/sh

set -e

cd plantdb  # so poetry can find its `pyproject.toml` file!

poetry run python plantdb/bin/romi_scanner_rest_api.py -db "/myapp/db"