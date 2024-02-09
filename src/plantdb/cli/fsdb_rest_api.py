#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Serve the plant database through a REST API."""

import argparse
import logging
import os
import sys
from time import sleep

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from plantdb.fsdb import FSDB
from plantdb.log import configure_logger
from plantdb.rest_api import Archive
from plantdb.rest_api import File
from plantdb.rest_api import Image
from plantdb.rest_api import Mesh
from plantdb.rest_api import PointCloud
from plantdb.rest_api import PointCloudGroundTruth
from plantdb.rest_api import Refresh
from plantdb.rest_api import Scan
from plantdb.rest_api import ScanList
from plantdb.test_database import DATASET
from plantdb.test_database import test_database


def parsing():
    parser = argparse.ArgumentParser(description='Serve a local plantdb database (FSDB) through a REST API.')
    parser.add_argument('-db', '--db_location', type=str, default=os.environ.get("ROMI_DB", "/none"),
                        help='location of the database to serve')

    app_args = parser.add_argument_group("webserver arguments")
    app_args.add_argument('--host', type=str, default="0.0.0.0",
                          help="the hostname to listen on")
    app_args.add_argument('--port', type=int, default=5000,
                          help="the port of the webserver")
    app_args.add_argument('--debug', action='store_true',
                          help="enable debug mode")

    misc_args = parser.add_argument_group("other arguments")
    misc_args.add_argument("--test", action='store_true',
                           help="set up a temporary test database prior to starting the REST API.")

    return parser


def main():
    parser = parsing()
    args = parser.parse_args()

    # Instantiate the Flask application:
    app = Flask(__name__)
    CORS(app)
    api = Api(app)
    # Instantiate the logger:
    wlogger = logging.getLogger('werkzeug')
    logger = configure_logger('fsdb_rest_api')

    if args.test:
        args.db_location = test_database(DATASET, with_configs=True, with_models=True).path()

    if args.db_location == "/none":
        logger.error("Can't serve a local PlantDB as no path to the database was specified!")
        logger.info(
            "To specify the location of the local database to serve, either set the environment variable 'ROMI_DB' or use the `-db` or `--db_location` option.")
        sleep(1)
        sys.exit("Wrong database location!")

    # Connect to the database:
    db = FSDB(args.db_location)
    logger.info(f"Connecting to local plant database located at '{db.path()}'...")
    db.connect(unsafe=True)  # to avoid locking the database
    logger.info(f"Found {len(db.list_scans())} scans dataset to serve in local plant database.")

    # Initialize RESTful resources to serve:
    api.add_resource(ScanList, '/scans',
                     resource_class_args=tuple([db]))
    api.add_resource(Scan, '/scans/<string:scan_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(File, '/files/<path:path>',
                     resource_class_args=tuple([db]))
    api.add_resource(Refresh, '/refresh',
                     resource_class_args=tuple([db]))
    api.add_resource(Image, '/image/<string:scan_id>/<string:fileset_id>/<string:file_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(PointCloud, '/pointcloud/<string:scan_id>/<string:fileset_id>/<string:file_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(PointCloudGroundTruth, '/pcGroundTruth/<string:scan_id>/<string:fileset_id>/<string:file_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(Mesh, '/mesh/<string:scan_id>/<string:fileset_id>/<string:file_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(Archive, '/archive/<string:scan_id>',
                     resource_class_args=tuple([db]))

    # Start the Flask application:
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()