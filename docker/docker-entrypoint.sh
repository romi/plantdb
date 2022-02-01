#!/bin/sh

set -e
. /venv/bin/activate
/venv/bin/romi_scanner_rest_api -db "/myapp/db"