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
# FSDB REST API - Serve Plant Database through RESTful Endpoints

This module provides a RESTful API server for interacting with a local plant database (FSDB).
It is designed for the ROMI project and facilitates efficient data handling and retrieval of plant-related datasets.
The server enables users to query and manage plant scans, images, point clouds, and other related data files.

## Key Features

- Serve a local plant database (FSDB) through RESTful API endpoints.
- Manage plant scans and related data, including images, point clouds, and meshes.
- Retrieve and manage dataset files with various configurations.
- Run in test mode with optional preconfigured datasets or an empty test database.
- Lightweight server setup using Flask, with options for debugging and CORS support.

## Environment Variables

- ``ROMI_DB``: Path to the directory containing the FSDB. Default: '/myapp/db' (container)
- ``API_PREFIX``: Prefix for the REST API URL. Default is empty.
- ``PLANTDB_API_SSL``: Enable SSL to use an HTTPS scheme. Default is `False`.
- ``FLASK_SECRET_KEY``: The secret key to use with flask. Default to random (64 bits secret).
- ``JWT_SECRET_KEY``: The secret key to use with JSON Web Token generator. Default to random (64 bits secret).
- ``SESSION_TIMEOUT``: Session JWT validity duration in seconds. Default `900` seconds (15 min).
- ``REFRESH_TIMEOUT``: Refresh JWT validity duration in seconds. Default `86400` seconds (1 day).
- ``MAX_SESSION``: The maximum number of concurrent sessions to allow. Default `10`.

## Usage Examples

To start the REST API server for a local plant database:

```shell
fsdb_rest_api -db /path/to/your/database --host 127.0.0.1 --port 8080
```

To run the server with a temporary test database in debug mode:

```shell
fsdb_rest_api --test --debug
```

For detailed command-line parameters, use the `--help` flag:
```shell
fsdb_rest_api --help
```
"""

import atexit
import logging
import os
import shutil
import sys
from pathlib import Path
from time import sleep
from typing import Optional
from typing import Union

import click
from click_option_group import OptionGroup
from click_option_group import optgroup
from flask import Flask
from flask import redirect
from flask_cors import CORS
from flask_restful import Api
from werkzeug.middleware.proxy_fix import ProxyFix

from plantdb.commons.log import get_logger
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import DEFAULT_LOG_LEVEL

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __name__.split('.')[-1])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)

from plantdb.commons import api_endpoints
from plantdb.commons.api_endpoints import API_PREFIX
from plantdb.commons.api_endpoints import ARCHIVE
from plantdb.commons.api_endpoints import CREATE_API_TOKEN
from plantdb.commons.api_endpoints import FILE
from plantdb.commons.api_endpoints import FILESET
from plantdb.commons.api_endpoints import FILESET_FILES
from plantdb.commons.api_endpoints import FILESET_MD
from plantdb.commons.api_endpoints import FILE_MD
from plantdb.commons.api_endpoints import FILE_PATH
from plantdb.commons.api_endpoints import HEALTH
from plantdb.commons.api_endpoints import HOME
from plantdb.commons.api_endpoints import IMAGE
from plantdb.commons.api_endpoints import LOGIN
from plantdb.commons.api_endpoints import LOGOUT
from plantdb.commons.api_endpoints import MESH
from plantdb.commons.api_endpoints import POINTCLOUD
from plantdb.commons.api_endpoints import REFRESH
from plantdb.commons.api_endpoints import REGISTER
from plantdb.commons.api_endpoints import SCAN
from plantdb.commons.api_endpoints import SCANS
from plantdb.commons.api_endpoints import SCANS_INFO
from plantdb.commons.api_endpoints import SCAN_FILESETS
from plantdb.commons.api_endpoints import SCAN_MD
from plantdb.commons.api_endpoints import SEQUENCE
from plantdb.commons.api_endpoints import SKELETON
from plantdb.commons.api_endpoints import TOKEN_REFRESH
from plantdb.commons.api_endpoints import TOKEN_VALIDATION
from plantdb.commons.api_endpoints import TIMELAPSES
from plantdb.commons.api_endpoints import TIMELAPSE
from plantdb.commons.api_endpoints import TIMELAPSE_SCANS
from plantdb.commons.auth.session import JWTSessionManager
from plantdb.commons.auth.session import _init_secret_key
from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.test_database import DATASET
from plantdb.commons.test_database import test_database
from plantdb.server.api.assets import Archive
from plantdb.server.api.assets import CurveSkeleton
from plantdb.server.api.assets import FilePath
from plantdb.server.api.assets import Image
from plantdb.server.api.assets import Mesh
from plantdb.server.api.assets import PointCloud
from plantdb.server.api.assets import Sequence
from plantdb.server.api.auth import CreateApiToken
from plantdb.server.api.auth import Login
from plantdb.server.api.auth import Logout
from plantdb.server.api.auth import Register
from plantdb.server.api.auth import TokenRefresh
from plantdb.server.api.auth import TokenValidation
from plantdb.server.api.base import HealthCheck
from plantdb.server.api.base import Home
from plantdb.server.api.base import Refresh
from plantdb.server.api.file import File
from plantdb.server.api.file import FileMetadata
from plantdb.server.api.fileset import Fileset
from plantdb.server.api.fileset import FilesetFiles
from plantdb.server.api.fileset import FilesetMetadata
from plantdb.server.api.scan import Scan
from plantdb.server.api.scan import ScanFilesets
from plantdb.server.api.scan import ScanMetadata
from plantdb.server.api.scan import ScansList
from plantdb.server.api.scan import ScansTable
from plantdb.server.api.timelapse import Timelapses
from plantdb.server.api.timelapse import Timelapse
from plantdb.server.api.timelapse import TimelapseScans


def _get_env_secret(var_name: str, logger: logging.Logger) -> str:
    """Retrieve a secret from the environment or generate a new one if missing.

    Parameters
    ----------
    var_name : str
        Name of the environment variable holding the secret.
    logger : logging.Logger
        A logger instance for warning and debugging.

    Returns
    -------
    str
        The secret value, either read from the environment or newly generated.
    """
    secret = os.environ.get(var_name)
    if secret is None:
        logger.warning(f"No secret key was provided for {var_name}.")
        logger.info(
            f"Set one with the '{var_name}' environment variable or let the server generate a random one."
        )
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


def _configure_api(
        app: Flask, proxy: bool, deploy_prefix: str, logger: logging.Logger
) -> Api:
    """Attach a `flask_restful.Api` to the `app` and configure proxy handling.

    Parameters
    ----------
    app : flask.Flask
        The Flask application to extend.
    proxy : bool
        Whether the application is behind a reverse proxy.
    deploy_prefix : str
        Optional deployment (reverse-proxy) prefix used when generating
        external-facing URLs in responses (e.g. ``/plantdb``).  The mount
        prefix is always ``API_PREFIX = "/api/v1"``; the deployment prefix
        is only used for link generation.
    logger : logging.Logger
        A logger instance for warning and debugging.

    Returns
    -------
    flask_restful.Api
        The configured API instance.
    """
    from plantdb.commons.api_endpoints import API_PREFIX
    logger.info("Setting up Flask application...")

    mount_prefix = API_PREFIX  # always mount at /api/v1

    if proxy:
        logger.info("Enabling proxy support for all RESTful endpoints.")
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)
        if deploy_prefix:
            logger.info(f"Using deployment prefix '{deploy_prefix}' for external URL generation.")

    api = Api(app, prefix=mount_prefix)
    return api


def _register_root_redirect(app: Flask, deploy_prefix: str = "") -> None:
    """Redirect the server root (``/``) to the API home endpoint.

    The API home is served at the API mount prefix (``'/api/v1'``), so a request to
    the bare root (``http://host:port/``, or ``http://host:port/{deploy_prefix}/``
    behind a reverse proxy) would otherwise 404. This registers a lightweight
    route that issues a ``302`` redirect to ``home(prefix=deploy_prefix)``.

    The ``deploy_prefix`` (reverse-proxy prefix, e.g. ``/plantdb``) is prepended to
    the generated ``Location`` so the browser stays under the proxy path; without a
    deployment prefix the redirect simply targets ``'/api/v1'``.

    Parameters
    ----------
    app : flask.Flask
        The Flask application to register the redirect on.
    deploy_prefix : str, optional
        Deployment (reverse-proxy) prefix prepended before ``/api/v1/...`` in the
        generated redirect location (e.g. ``/plantdb``).
    """
    @app.route("/")
    def _home_redirect():
        return redirect(api_endpoints.home(prefix=deploy_prefix), code=302)


def _setup_test_database(
        empty: bool,
        models: bool,
        db_path: Optional[Union[str, Path]],
        logger: logging.Logger,
) -> Path:
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
    pathlib.Path
        The path to the created test database.
    """
    jwt_key = _get_env_secret("JWT_SECRET_KEY", logger)
    session_timeout = int(os.getenv("SESSION_TIMEOUT", 3600))
    max_sessions = int(os.getenv("MAX_SESSION", 10))
    if empty:
        logger.info(
            "Setting up a temporary test database without any datasets or configurations..."
        )
        db_path = test_database(
            None,
            db_path=db_path,
            session_manager=JWTSessionManager(
                secret_key=jwt_key,
                session_timeout=session_timeout,
                max_concurrent_sessions=max_sessions,
            ),
        ).path()
    else:
        logger.info(
            "Setting up a temporary test database with sample datasets and configurations..."
        )
        db_path = test_database(
            DATASET,
            db_path=db_path,
            with_configs=True,
            with_models=models,
            session_manager=JWTSessionManager(
                secret_key=jwt_key,
                session_timeout=session_timeout,
                max_concurrent_sessions=max_sessions,
            ),
        ).path()
    return Path(db_path)


def _register_resources(api: Api, db: FSDB, logger: logging.Logger, deploy_prefix: str = "") -> None:
    """Register all resources with the Flask-RESTful API.

    The endpoint strings are generated by the helper functions defined in ``plantdb.commons.api_endpoints``.
    By keeping the mapping in a single data-structure we avoid duplicated code and the sanity-check errors that
    appear when trying to call the helpers inline.

    Parameters
    ----------
    api : flask_restful.Api
        The API instance.
    db : plantdb.commons.fsdb.core.FSDB
        The database instance.
    logger : logging.Logger
        A logger instance.
    deploy_prefix : str, optional
        Deployment (reverse-proxy) prefix passed to resources that generate
        external-facing URLs in their responses.
    """
    # Mapping of (resource class, endpoint function)
    RESOURCE_MAP = [
        # Core endpoints
        (Home, lambda: HOME),
        (HealthCheck, lambda: HEALTH),
        (Refresh, lambda: REFRESH),
        # Authentication
        (Register, lambda: REGISTER),
        (Login, lambda: LOGIN),
        (Logout, lambda: LOGOUT),
        (TokenRefresh, lambda: TOKEN_REFRESH),
        (TokenValidation, lambda: TOKEN_VALIDATION),
        (CreateApiToken, lambda: CREATE_API_TOKEN),
        # Scan CRUD
        (ScansList, lambda: SCANS),
        (ScansTable, lambda: SCANS_INFO),
        (Scan, lambda: SCAN.format(scan_id="<string:scan_id>")),
        (ScanMetadata, lambda: SCAN_MD.format(scan_id="<string:scan_id>")),
        (ScanFilesets, lambda: SCAN_FILESETS.format(scan_id="<string:scan_id>")),
        # Timelapse CRUD
        (Timelapses, lambda: TIMELAPSES),
        (Timelapse, lambda: TIMELAPSE.format(timelapse_id="<string:timelapse_id>")),
        (TimelapseScans, lambda: TIMELAPSE_SCANS.format(timelapse_id="<string:timelapse_id>")),
        # Fileset CRUD
        (Fileset, lambda: FILESET.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>")),
        (FilesetMetadata, lambda: FILESET_MD.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>")),
        (FilesetFiles, lambda: FILESET_FILES.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>")),

        # File CRUD
        (File, lambda: FILE.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>",
                                   file_id="<string:file_id>")),
        (FileMetadata, lambda: FILE_MD.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>",
                                              file_id="<string:file_id>")),

        # Assets CRUD
        (Image, lambda: IMAGE.format(scan_id="<string:scan_id>", fileset_id="<string:fileset_id>",
                                     file_id="<string:file_id>")),
        (FilePath, lambda: FILE_PATH.format(scan_id="<string:scan_id>", file_path="<path:path>")),
        (PointCloud, lambda: POINTCLOUD.format(scan_id="<string:scan_id>")),
        (Mesh, lambda: MESH.format(scan_id="<string:scan_id>")),
        (CurveSkeleton, lambda: SKELETON.format(scan_id="<string:scan_id>")),
        (Sequence, lambda: SEQUENCE.format(scan_id="<string:scan_id>")),
        (Archive, lambda: ARCHIVE.format(scan_id="<string:scan_id>")),
    ]

    # Resources that need the deployment prefix for external URL generation.
    _resources_with_prefix = (Home, ScansTable, Scan)

    # Register everything
    for resource, endpoint_func in RESOURCE_MAP:
        extra_kwargs = {}
        if resource in _resources_with_prefix:
            extra_kwargs["deploy_prefix"] = deploy_prefix
        api.add_resource(resource, endpoint_func(),
                         resource_class_args=(db, logger),
                         resource_class_kwargs=extra_kwargs)


def rest_api(
        db_path: Optional[Union[str, Path]],
        proxy: bool = False,
        api_prefix: str = "",
        deploy_prefix: str = "",
        ssl: bool = False,
        log_level: str = DEFAULT_LOG_LEVEL,
        test: bool = False,
        empty: bool = False,
        models: bool = False,
) -> Flask:
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
    api_prefix : str, optional
        **Deprecated.** Kept for backward compatibility; use ``deploy_prefix`` instead.
    deploy_prefix : str, optional
        Deployment (reverse-proxy) prefix prepended before ``/api/v1/...``
        when generating external-facing URLs in responses (e.g. ``/plantdb``).
        Default is ``""`` (no deployment prefix).
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
    wlogger = logging.getLogger("werkzeug")
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    # Resolve deployment prefix: use explicit ``deploy_prefix``, else ``api_prefix`` (deprecated).
    _deploy_prefix = deploy_prefix or api_prefix or ""
    # 1 - Application and API configuration
    secret_key = _get_env_secret("FLASK_SECRET_KEY", logger)
    app = _configure_app(secret_key, ssl=ssl)
    api = _configure_api(app, proxy, _deploy_prefix, logger)

    # 1b - Redirect the server root (``/``) to the API home endpoint.
    _register_root_redirect(app, _deploy_prefix)

    # 2 - Handle test mode
    if test:
        db_path = _setup_test_database(
            empty=empty, models=models, db_path=db_path, logger=logger
        )

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
        logger.error("No path to the local PlantDB was specified; aborting startup.")
        logger.info(
            "Set the environment variable 'ROMI_DB' or use the '-db' CLI argument."
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
        log_level=log_level,
    )
    logger.info(f"Connecting to local plant database at '{db.path()}'.")
    db.connect()
    logger.info(
        f"Found {len(db.list_scans(owner_only=False))} scans to serve in the local database."
    )

    # 5 - Register resources
    _register_resources(api, db, logger, deploy_prefix=_deploy_prefix)

    return app


# ---------------------------------------------------------------------------
# Click command-line interface
# ---------------------------------------------------------------------------


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "--host",
    default="0.0.0.0",
    show_default=True,
    help="Hostname to listen on.",
)
@click.option(
    "--port",
    type=int,
    default=5000,
    show_default=True,
    help="Port of the webserver.",
)
@click.option("--debug", is_flag=True, default=False, help="Enable debug mode.")
@click.option(
    "--proxy",
    is_flag=True,
    default=False,
    help="Use when the server sits behind a reverse proxy.",
)
@click.option(
    "--api-prefix",
    type=str,
    default="",
    show_default=True,
    help="Deployment (reverse-proxy) prefix prepended before /api/v1/... "
         "in generated endpoint URLs (e.g. '/plantdb'). "
         "The API version prefix /api/v1 is always included.",
)
@optgroup.group("Database", cls=OptionGroup)
@optgroup.option(
    "-db",
    "--db-path",
    type=click.Path(),
    default=os.getenv("ROMI_DB", None),
    show_default=True,
    help="Path to the local database to serve.",
)
@optgroup.option(
    "--test",
    is_flag=True,
    default=False,
    help="Set up a temporary test database before starting the REST API.",
)
@optgroup.option(
    "--empty",
    is_flag=True,
    default=False,
    help="Do not populate the test database with toy datasets.",
)
@optgroup.option(
    "--models",
    is_flag=True,
    default=False,
    help="Include trained CNN model in the test database.",
)
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default=DEFAULT_LOG_LEVEL,
    show_default=True,
    help="Logging level.",
)
def main(host, port, debug, proxy, api_prefix, db_path, test, empty, models, log_level):
    """FSDB REST API - Serve Plant Database through RESTful Endpoints."""
    app = rest_api(db_path=db_path, proxy=proxy, deploy_prefix=api_prefix, log_level=log_level.upper(), test=test,
                   empty=empty, models=models)
    # Start the Flask development server.
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    main()
