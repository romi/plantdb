#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# FSDB REST API - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: J. Legrand
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
FSDB REST API - Serve Plant Database through RESTful Endpoints

This module provides a RESTful API server for interacting with a local plant database (FSDB).
It is designed for the ROMI project and facilitates efficient data handling and retrieval of plant-related datasets.
The server enables users to query and manage plant scans, images, point clouds, and other related data files.

Key Features
------------
- Serve a local plant database (FSDB) through RESTful API endpoints.
- Manage plant scans and related data, including images, point clouds, and meshes.
- Retrieve and manage dataset files with various configurations.
- Run in test mode with optional preconfigured datasets or an empty test database.
- Lightweight server setup using Flask, with options for debugging and CORS support.

Usage Examples
--------------
To start the REST API server for a local plant database:

```shell
python fsdb_rest_api.py --db_location /path/to/your/database --host 127.0.0.1 --port 8080 --debug
```

To run the server with a temporary test database in debug mode:

```shell
python fsdb_rest_api.py --test --debug
```

RESTful endpoints include:
- `/scans`: List all scans available in the database.
- `/files/<path:path>`: Retrieve files from the database.
- `/image/<scan_id>/<fileset_id>/<file_id>`: Access specific images.
- `/pointcloud/<scan_id>/<fileset_id>/<file_id>`: Access specific point clouds.
- `/mesh/<scan_id>/<fileset_id>/<file_id>`: Retrieve related meshes.

For detailed command-line parameters, use the `--help` flag:
```shell
python fsdb_rest_api.py --help
```
"""

import argparse
import atexit
import logging
import os
import shutil
import sys
from time import sleep

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from plantdb.commons.fsdb import FSDB
from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.server.rest_api import Archive
from plantdb.server.rest_api import CurveSkeleton
from plantdb.server.rest_api import DatasetFile
from plantdb.server.rest_api import File
from plantdb.server.rest_api import FileCreate
from plantdb.server.rest_api import FileMetadata
from plantdb.server.rest_api import FilesetCreate
from plantdb.server.rest_api import FilesetFiles
from plantdb.server.rest_api import FilesetMetadata
from plantdb.server.rest_api import Image
from plantdb.server.rest_api import Login
from plantdb.server.rest_api import Mesh
from plantdb.server.rest_api import PointCloud
from plantdb.server.rest_api import PointCloudGroundTruth
from plantdb.server.rest_api import Refresh
from plantdb.server.rest_api import Register
from plantdb.server.rest_api import Scan
from plantdb.server.rest_api import ScanCreate
from plantdb.server.rest_api import ScanFilesets
from plantdb.server.rest_api import ScanMetadata
from plantdb.server.rest_api import ScansList
from plantdb.server.rest_api import ScansTable
from plantdb.server.rest_api import Sequence
from plantdb.commons.test_database import DATASET
from plantdb.commons.test_database import test_database


def parsing():
    parser = argparse.ArgumentParser(description='Serve a local plantdb database (FSDB) through a REST API.')
    parser.add_argument('-db', '--db_location', type=str, default=os.environ.get("ROMI_DB", "/none"),
                        help='location of the database to serve.')

    app_args = parser.add_argument_group("webserver arguments")
    app_args.add_argument('--host', type=str, default="0.0.0.0",
                          help="the hostname to listen on, defaults to '0.0.0.0'.")
    app_args.add_argument('--port', type=int, default=5000,
                          help="the port of the webserver, defaults to '5000'.")
    app_args.add_argument('--debug', action='store_true',
                          help="enable debug mode.")

    misc_args = parser.add_argument_group("other arguments")
    misc_args.add_argument("--test", action='store_true',
                           help="set up a temporary test database prior to starting the REST API.")
    misc_args.add_argument("--empty", action='store_true',
                           help="the test database will not be populated with toy dataset.")
    misc_args.add_argument("--models", action='store_true',
                           help="the test database will contain the trained CNN model.")

    log_opt = parser.add_argument_group("Logging options")
    log_opt.add_argument("--log-level", dest="log_level", type=str, default=DEFAULT_LOG_LEVEL, choices=LOG_LEVELS,
                         help="Level of message logging, defaults to 'INFO'.")

    return parser


def rest_api(db_location, host="0.0.0.0", port=5000, debug=False, test=False, empty=False, models=False,
             log_level=DEFAULT_LOG_LEVEL):
    """Initialize and configure a RESTful API server for Plant Database querying.

    This function sets up a Flask application with various RESTful endpoints to enable interaction with a
    local Plant Database (FSDB).
    RESTful routes are added for managing and retrieving various datasets and configurations, providing
    an interface for working with plant scans and related files. The application can be run in test
    mode with optional configurations for using sample datasets.

    Parameters
    ----------
    db_location : str
        The path to the local plant database to be served. If set to "/none", the server will raise
        an error and terminate unless the path is appropriately overridden in test mode.
    host : str, optional
        The hostname or IP address on which the Flask application will listen for incoming requests.
         Defaults to ``"0.0.0.0"``.
    port : int, optional
        The port number to bind the Flask application for incoming HTTP requests.
         Defaults to ``5000``.
    debug : bool, optional
        A boolean flag indicating whether Flask debugging mode should be enabled.
        Useful for debugging during development. Defaults to ``False``.
    test : bool, optional
        A boolean flag to specify if the application should run in test mode. When enabled, a test
        database will be instantiated with sample datasets or an empty configuration if specified.
         Defaults to ``False``.
    empty : bool, optional
        A boolean flag to specify whether the test database should be instantiated without any
        datasets or configurations. Defaults to ``False``.
    models : bool, optional
        A boolean flag to specify whether the test database should be populated with trained CNN models.
        Defaults to ``False``.
    log_level : str, optional
        The logging level to use for the application. Defaults to ``DEFAULT_LOG_LEVEL``.

    """
    # Instantiate the Flask application:
    app = Flask(__name__)
    CORS(app)
    api = Api(app)
    # Instantiate the logger:
    wlogger = logging.getLogger('werkzeug')
    logger = get_logger('fsdb_rest_api', log_level=log_level)

    if test:
        if empty:
            db_location = test_database(None).path()
        else:
            db_location = test_database(DATASET, with_configs=True, with_models=models).path()

        # Register cleanup if a temporary database was created
        def cleanup():
            logger.info(f"Cleaning up temporary database directory at '{db_location}'...")
            try:
                shutil.rmtree(db_location)
                logger.info(f"Successfully removed temporary directory at '{db_location}'.")
            except OSError as e:
                logger.error(f"Error removing temporary directory: {e}.")

        atexit.register(cleanup)

    if db_location == "/none":
        logger.error("Can't serve a local PlantDB as no path to the database was specified!")
        logger.info(
            "To specify the location of the local database to serve, either set the environment variable 'ROMI_DB' or use the `-db` or `--db_location` option.")
        sleep(1)
        sys.exit("Wrong database location!")

    # Connect to the database:
    db = FSDB(db_location)
    logger.info(f"Connecting to local plant database located at '{db.path()}'...")
    db.connect(unsafe=True)  # to avoid locking the database
    logger.info(f"Found {len(db.list_scans(owner_only=False))} scans dataset to serve in local plant database.")

    # Initialize RESTful resources to serve:
    api.add_resource(ScansList, '/scans',
                     resource_class_args=tuple([db]))
    api.add_resource(ScansTable, '/scans_info',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(Scan, '/scans/<string:scan_id>',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(File, '/files/<path:path>',
                     resource_class_args=tuple([db]))
    api.add_resource(DatasetFile, '/files/<string:scan_id>',
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
    api.add_resource(CurveSkeleton, '/skeleton/<string:scan_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(Sequence, '/sequence/<string:scan_id>',
                     resource_class_args=tuple([db]))
    api.add_resource(Archive, '/archive/<string:scan_id>',
                     resource_class_args=tuple([db, logger]))
    # User oriented endpoints
    api.add_resource(Register, '/register',
                     resource_class_args=tuple([db]))
    api.add_resource(Login, '/login',
                     resource_class_args=tuple([db]))
    # API endpoints for `plantdb.commons.fsdb.Scan`:
    api.add_resource(ScanCreate, '/api/scan',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(ScanMetadata, '/api/scan/<string:scan_id>/metadata',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(ScanFilesets, '/api/scan/<string:scan_id>/filesets',
                     resource_class_args=tuple([db, logger]))
    # API endpoints for `plantdb.commons.fsdb.Fileset`:
    api.add_resource(FilesetCreate, '/api/fileset',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(FilesetMetadata, '/api/fileset/<string:scan_id>/<string:fileset_name>/metadata',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(FilesetFiles, '/api/fileset/<string:scan_id>/<string:fileset_name>/files',
                     resource_class_args=tuple([db, logger]))
    # API endpoints for `plantdb.commons.fsdb.File`:
    api.add_resource(FileCreate, '/api/file',
                     resource_class_args=tuple([db, logger]))
    api.add_resource(FileMetadata, '/api/file/<string:scan_id>/<string:fileset_name>/<string:file_name>/metadata',
                     resource_class_args=tuple([db, logger]))

    # Start the Flask application:
    app.run(host=host, port=port, debug=debug)


def main():
    """Main function to initialize and execute the REST API server.

    This function utilizes argument parsing to extract user-provided input values
    for configuring and running the REST API server.
    """
    parser = parsing()
    args = parser.parse_args()
    rest_api(args.db_location, args.host, args.port, args.debug, args.test, args.empty, args.models, args.log_level)


if __name__ == '__main__':
    main()
