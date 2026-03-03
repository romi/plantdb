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

Environment Variables
---------------------
- ``ROMI_DB``: Path to the directory containing the FSDB. Default: '/myapp/db' (container)
- ``PLANTDB_API_PREFIX``: Prefix for the REST API URL. Default is empty.
- ``PLANTDB_API_SSL``: Enable SSL to use an HTTPS scheme. Default is `False`.
- ``FLASK_SECRET_KEY``: The secret key to use with flask. Default to random (64 bits secret).
- ``JWT_SECRET_KEY``: The secret key to use with JSON Web Token generator. Default to random (64 bits secret).
- ``SESSION_TIMEOUT``: Session JWT validity duration in seconds. Default `900` seconds (15 min).
- ``REFRESH_TIMEOUT``: Refresh JWT validity duration in seconds. Default `86400` seconds (1 day).
- ``MAX_SESSION``: The maximum number of concurrent sessions to allow. Default `10`.

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
from pathlib import Path
from time import sleep
from typing import Optional
from typing import Union

from flask import Flask
from flask_cors import CORS
from flask_restful import Api
from werkzeug.middleware.proxy_fix import ProxyFix

from plantdb.commons.auth.session import JWTSessionManager
from plantdb.commons.auth.session import _init_secret_key
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.log import DEFAULT_LOG_LEVEL
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.commons.test_database import DATASET
from plantdb.commons.test_database import test_database
from plantdb.server.api.assets import Archive
from plantdb.server.api.assets import CurveSkeleton
from plantdb.server.api.assets import DatasetFile
from plantdb.server.api.assets import File
from plantdb.server.api.file import FileCreate
from plantdb.server.api.file import FileMetadata
from plantdb.server.api.fileset import FilesetCreate
from plantdb.server.api.fileset import FilesetFiles
from plantdb.server.api.fileset import FilesetMetadata
from plantdb.server.api.base import HealthCheck
from plantdb.server.api.base import Home
from plantdb.server.api.assets import Image
from plantdb.server.api.auth import Login
from plantdb.server.api.auth import CreateApiToken
from plantdb.server.api.auth import Logout
from plantdb.server.api.assets import Mesh
from plantdb.server.api.assets import PointCloud
from plantdb.server.api.assets import PointCloudGroundTruth
from plantdb.server.api.base import Refresh
from plantdb.server.api.auth import Register
from plantdb.server.api.scan import Scan
from plantdb.server.api.scan import ScanCreate
from plantdb.server.api.scan import ScanFilesets
from plantdb.server.api.scan import ScanMetadata
from plantdb.server.api.scan import ScansList
from plantdb.server.api.scan import ScansTable
from plantdb.server.api.assets import Sequence
from plantdb.server.api.auth import TokenRefresh
from plantdb.server.api.auth import TokenValidation


def parsing() -> argparse.ArgumentParser:
    """Create and configure an argument parser for a REST API server.

    Returns
    -------
    argparse.ArgumentParser
        The configured argument parser capable of parsing and retrieving command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Serve a local plantdb database (FSDB) through a REST API.')
    parser.add_argument('-db', '--db_location', type=str, default=os.environ.get("ROMI_DB", None),
                        help='location of the database to serve.')

    app_args = parser.add_argument_group("webserver arguments")
    app_args.add_argument('--host', type=str, default="0.0.0.0",
                          help="hostname to listen on; defaults to '0.0.0.0'.")
    app_args.add_argument('--port', type=int, default=5000,
                          help="port of the webserver; defaults to '5000'.")
    app_args.add_argument('--debug', action='store_true',
                          help="enable debug mode.")
    app_args.add_argument('--proxy', action='store_true',
                          help="use when the server sits behind a reverse proxy.")

    misc_args = parser.add_argument_group("other arguments")
    misc_args.add_argument("--test", action='store_true',
                           help="set up a temporary test database before starting the REST API.")
    misc_args.add_argument("--empty", action='store_true',
                           help="do not populate the test database with toy datasets.")
    misc_args.add_argument("--models", action='store_true',
                           help="include trained CNN model in the test database.")

    log_opt = parser.add_argument_group("logging options")
    log_opt.add_argument("--log-level", dest="log_level", type=str, default=DEFAULT_LOG_LEVEL, choices=LOG_LEVELS,
                         help="logging level; defaults to 'INFO'.")

    return parser


def _get_env_secret(var_name: str, logger: logging.Logger) -> str:
    """Retrieve a secret from the environment or generate a new one if missing.

    Parameters
    ----------
    var_name : str
        Name of the environment variable holding the secret.
    logger : logging.Logger
        Logger instance for warning and debugging.

    Returns
    -------
    str
        The secret value, either read from the environment or newly generated.
    """
    secret = os.environ.get(var_name)
    if secret is None:
        logger.warning(f"No secret key was provided for {var_name}.")
        logger.info(f"Set one with the '{var_name}' environment variable or let the server generate a random one.")
    secret = _init_secret_key(secret)
    return secret


def _configure_app(secret_key: str, ssl: bool = False) -> Flask:
    """Create and configure a Flask application instance.

    Parameters
    ----------
    secret_key : str
        Secret key used for session signing.
    ssl : bool, optional
        Whether the app should enforce HTTPS for secure cookies.

    Returns
    -------
    flask.Flask
        The configured Flask application.
    """
    app = Flask(__name__)
    CORS(app)  # Enable Cross-Origin Resource Sharing
    app.config.update(
        SECRET_KEY=secret_key,
        SESSION_COOKIE_SECURE=ssl,
        # SESSION_COOKIE_HTTPONLY=True,
        # SESSION_COOKIE_SAMESITE="Strict",
    )
    return app


def _configure_api(app: Flask, proxy: bool, url_prefix: str, logger: logging.Logger) -> Api:
    """Attach a `flask_restful.Api` to the `app` and configure proxy handling.

    Parameters
    ----------
    app : flask.Flask
        The Flask application to extend.
    proxy : bool
        Whether the server is behind a reverse proxy.
    url_prefix : str
        URL prefix for all endpoints when using a proxy.
    logger : logging.Logger
        Logger instance for warning and debugging.

    Returns
    -------
    flask_restful.Api
        The configured API instance.
    """
    if proxy:
        logger.info("Setting up Flask application with proxy support...")
        api = Api(app, prefix=url_prefix)
        logger.info(f"Using prefix '{url_prefix}' for all RESTful endpoints.")
        # App is behind one proxy that sets the X-Forwarded-* headers.
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)
    else:
        api = Api(app)
    return api


def _setup_test_database(empty: bool, models: bool, db_path: Optional[Union[str, Path]],
                         logger: logging.Logger) -> Path:
    """Create a temporary test database, optionally populated with toy data.

    Parameters
    ----------
    empty : bool
        If ``True`` create an empty database.
    models : bool
        If ``True`` include pretrained CNN models.
    db_path : Optional[Union[str, Path]]
        Existing database location or ``None`` to create a temp folder.
    logger : logging.Logger
        A logger instance for warning and debugging.

    Returns
    -------
    Path
        The path to the created test database.
    """
    jwt_key = _get_env_secret("JWT_SECRET_KEY", logger)
    session_timeout = int(os.getenv("SESSION_TIMEOUT", 3600))
    max_sessions = int(os.getenv("MAX_SESSION", 10))
    if empty:
        logger.info("Setting up a temporary test database without any datasets or configurations...")
        db_path = test_database(
            None, db_path=db_path,
            session_manager=JWTSessionManager(secret_key=jwt_key, session_timeout=session_timeout,
                                              max_concurrent_sessions=max_sessions)
        ).path()
    else:
        logger.info("Setting up a temporary test database with sample datasets and configurations...")
        db_path = test_database(
            DATASET,
            db_path=db_path,
            with_configs=True,
            with_models=models,
            session_manager=JWTSessionManager(secret_key=jwt_key, session_timeout=session_timeout,
                                              max_concurrent_sessions=max_sessions)
        ).path()
    return Path(db_path)


def _register_resources(api: Api, db: FSDB, logger: logging.Logger) -> None:
    """Register all RESTful resources with the Flask-RESTful API.

    Parameters
    ----------
    api : Api
        The Flask-RESTful API instance.
    db : FSDB
        The database connection.
    logger : logging.Logger
        Logger instance for warning and debugging.
    """
    api.add_resource(
        Home,
        "/"
    )
    api.add_resource(
        HealthCheck,
        "/health",
        resource_class_args=(db,)
    )
    api.add_resource(
        ScansList,
        "/scans",
        resource_class_args=(db,)
    )
    api.add_resource(
        ScansTable,
        "/scans_info",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        Scan,
        "/scans/<string:scan_id>",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        File,
        "/files/<path:path>",
        resource_class_args=(db,)
    )
    api.add_resource(
        DatasetFile,
        "/files/<string:scan_id>",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        Refresh,
        "/refresh",
        resource_class_args=(db,)
    )
    api.add_resource(
        Image,
        "/image/<string:scan_id>/<string:fileset_id>/<string:file_id>",
        resource_class_args=(db,),
    )
    api.add_resource(
        PointCloud,
        "/pointcloud/<string:scan_id>/<string:fileset_id>/<string:file_id>",
        resource_class_args=(db,),
    )
    api.add_resource(
        PointCloudGroundTruth,
        "/pcGroundTruth/<string:scan_id>/<string:fileset_id>/<string:file_id>",
        resource_class_args=(db,),
    )
    api.add_resource(
        Mesh,
        "/mesh/<string:scan_id>/<string:fileset_id>/<string:file_id>",
        resource_class_args=(db,),
    )
    api.add_resource(
        CurveSkeleton,
        "/skeleton/<string:scan_id>",
        resource_class_args=(db,)
    )
    api.add_resource(
        Sequence,
        "/sequence/<string:scan_id>",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        Archive,
        "/archive/<string:scan_id>",
        resource_class_args=(db, logger)
    )
    # User-oriented endpoints
    api.add_resource(
        Register,
        "/register",
        resource_class_args=(db,)
    )
    api.add_resource(
        Login,
        "/login",
        resource_class_args=(db,)
    )
    api.add_resource(
        Logout,
        "/logout",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        TokenRefresh,
        "/token-refresh",
        resource_class_args=(db,)
    )
    api.add_resource(
        TokenValidation,
        "/token-validation",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        CreateApiToken,
        "/create-api-token",
        resource_class_args=(db, logger)
    )
    # Scan CRUD
    api.add_resource(
        ScanCreate,
        "/api/scan",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        ScanMetadata,
        "/api/scan/<string:scan_id>/metadata",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        ScanFilesets,
        "/api/scan/<string:scan_id>/filesets",
        resource_class_args=(db, logger)
    )
    # Fileset CRUD
    api.add_resource(
        FilesetCreate,
        "/api/fileset",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        FilesetMetadata,
        "/api/fileset/<string:scan_id>/<string:fileset_id>/metadata",
        resource_class_args=(db, logger),
    )
    api.add_resource(
        FilesetFiles,
        "/api/fileset/<string:scan_id>/<string:fileset_id>/files",
        resource_class_args=(db, logger),
    )
    # File CRUD
    api.add_resource(
        FileCreate,
        "/api/file",
        resource_class_args=(db, logger)
    )
    api.add_resource(
        FileMetadata,
        "/api/file/<string:scan_id>/<string:fileset_id>/<string:file_id>/metadata",
        resource_class_args=(db, logger),
    )


def rest_api(db_path: Optional[Union[str, Path]], proxy: bool = False, url_prefix: str = "", ssl: bool = False,
             log_level: str = DEFAULT_LOG_LEVEL, test: bool = False, empty: bool = False,
             models: bool = False) -> Flask:
    """Initialize and configure a RESTful API server for Plant Database querying.

    This function sets up a Flask application with various RESTful endpoints to enable interaction with a
    local Plant Database (FSDB).
    RESTful routes are added for managing and retrieving various datasets and configurations, providing
    an interface for working with plant scans and related files. The application can be run in test
    mode with optional configurations for using sample datasets.

    Parameters
    ----------
    db_path : str or pathlib.Path or None
        The path to the local plant database to be served. If set to "/none", the server will raise
        an error and terminate unless the path is appropriately overridden in test mode.
        If `None`, requires `test=True` and a temporary folder will be created.
    proxy : bool, optional
        Boolean flag indicating whether the application is behind a reverse proxy, ``False`` by default.
    url_prefix : str, optional
        Prefix for all endpoints, by default ""
    log_level : str, optional
        The logging level to use for the application. Defaults to ``DEFAULT_LOG_LEVEL``.
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
    """
    wlogger = logging.getLogger('werkzeug')
    logger = get_logger("fsdb_rest_api", log_level=log_level)

    # 1 - Application and API configuration
    secret_key = _get_env_secret("FLASK_SECRET_KEY", logger)
    app = _configure_app(secret_key, ssl=ssl)
    api = _configure_api(app, proxy, url_prefix, logger)

    # 2 - Handle test mode
    if test:
        db_path = _setup_test_database(empty=empty, models=models, db_path=db_path, logger=logger)

        def _cleanup() -> None:
            logger.info(f"Cleaning up temporary database directory at '{db_path}'.")
            try:
                shutil.rmtree(db_path)
                logger.info("Temporary directory removed.")
            except OSError as exc:
                logger.error(f"Error removing temporary directory: {exc!s}")

        atexit.register(_cleanup)

    # 3 - Validate path
    if not db_path:
        logger.error(
            "No path to the local PlantDB was specified; aborting startup."
        )
        logger.info(
            "Set the environment variable 'ROMI_DB' or use the '--db_location' CLI argument."
        )
        sleep(1)
        sys.exit("Wrong database location!")

    # 4 - Database connection
    jwt_key = _get_env_secret("JWT_SECRET_KEY", logger)
    session_timeout = int(os.getenv("SESSION_TIMEOUT", 3600))
    refresh_timeout = int(os.getenv("REFRESH_TIMEOUT", 86400))
    max_sessions = int(os.getenv("MAX_SESSION", 10))
    db = FSDB(
        db_path,
        session_manager=JWTSessionManager(
            secret_key=jwt_key,
            session_timeout=session_timeout,
            refresh_timeout=refresh_timeout,
            max_concurrent_sessions=max_sessions,
        ),
    )
    logger.info(f"Connecting to local plant database at '{db.path()}'.")
    db.connect()
    logger.info(
        f"Found {len(db.list_scans(owner_only=False))} scans to serve in the local database."
    )

    # 5 - Register resources
    _register_resources(api, db, logger)

    return app


def main():
    """Entry point for the REST API server.

    Parses command line arguments, builds the Flask application, and starts the development server
    with the supplied host/port and debug settings.
    """
    parser = parsing()
    args = parser.parse_args()

    app = rest_api(
        db_path=args.db_location,
        proxy=args.proxy,
        log_level=args.log_level,
        test=args.test,
        empty=args.empty,
        models=args.models,
    )
    # Start the Flask application:
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()
