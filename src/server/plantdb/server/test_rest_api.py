#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import threading
import time
from pathlib import Path

from flask import Flask
from werkzeug.serving import make_server

from plantdb.server.cli.fsdb_rest_api import rest_api


class TestRestApiServer:
    """A test REST API server that runs in a separate thread for testing purposes.

    This class creates a Flask application that serves a PlantDB REST API and runs
    it in a separate thread so it doesn't block the current interpreter.

    Attributes
    ----------
    db_path : pathlib.Path
        Path to the database directory
    port : int
        Port number for the server
    host : str
        Host address for the server
    prefix : str
        URL prefix for API endpoints
    ssl : bool
        Whether to use SSL/HTTPS
    test : bool
        Whether to run in test mode, that is to populate with the test dataset (default: False)
    empty : bool
        Whether to create an empty database (default: False)
    models : bool
        Whether to create a database with models (default: False)
    app : Flask
        Flask application instance
    server : werkzeug.serving.BaseWSGIServer
        WSGI server instance
    thread : threading.Thread
        Thread running the server

    Examples
    --------
    >>> from plantdb.server.test_rest_api import TestRestApiServer
    >>> from plantdb.client.rest_api import list_scan_names
    >>>
    >>> # EXAMPLE 1 - Create a test database and start the Flask App serving a REST API
    >>> server = TestRestApiServer(test=True)
    >>> server.start()
    >>> scans_list = list_scan_names(host=server.host, port=server.port, prefix=server.prefix, ssl=server.ssl)
    >>> print(scans_list)
    ['arabidopsis000', 'real_plant', 'real_plant_analyzed', 'virtual_plant', 'virtual_plant_analyzed']
    >>> server.stop()
    >>>
    >>> # EXAMPLE 2 - Serve an existing database
    >>> from plantdb.commons.test_database import test_database
    >>> test_db = test_database()  # set up a temporary test database
    >>> print(test_db.path())
    /tmp/ROMI_DB_********
    >>> server = TestRestApiServer(db_path=test_db.path())
    >>> server.start()
    >>> list_scan_names(host=server.host, port=server.port, prefix=server.prefix, ssl=server.ssl)
    >>> print(scans_list)
    ['real_plant_analyzed']
    >>> server.stop()
    """

    def __init__(self, db_path='/none', port=5000, host='127.0.0.1', prefix='', ssl=False, test=False, empty=False,
                 models=False):
        """Initialize the test REST API server.

        Parameters
        ----------
        db_path : str or pathlib.Path
            Path to the database directory to serve
        port : int, optional
            Port number for the server (default: 5000)
        host : str, optional
            Host address for the server (default: '127.0.0.1')
        prefix : str, optional
            URL prefix for API endpoints (default: '')
        ssl : bool, optional
            Whether to use SSL/HTTPS (default: False)
        test : bool, optional
            Whether to run in test mode, that is to populate with the test dataset (default: False)
        empty : bool, optional
            Whether to create an empty database (default: False)
        models : bool, optional
            Whether to create a database with models (default: False)
        """
        self.db_path = Path(db_path)
        self.port = port
        self.host = host
        self.prefix = prefix
        self.ssl = ssl
        self.test = test
        self.empty = empty
        self.models = models
        self.app = None
        self.server = None
        self.thread = None
        self._setup_flask_app()

    def _setup_flask_app(self):
        """Setup the Flask application with REST API endpoints."""
        if self.test:
            self.db_path = "/none"
        rest_api_kwargs = {
            'proxy': self.prefix != '',
            'url_prefix': self.prefix,
            'test': self.test,
            'empty': self.empty,
            'models': self.models
        }
        self.app = rest_api(self.db_path, **rest_api_kwargs)

    def _is_port_available(self):
        """Check if the specified port is available on the given host.

        Returns
        -------
        bool
            True if port is available, False otherwise
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                result = sock.connect_ex((self.host, self.port))
                return result != 0  # Port is available if connection fails
        except Exception:
            return False

    def start(self):
        """Start the REST API server in a separate thread."""
        if self.thread is not None and self.thread.is_alive():
            return  # Already running

        # Check if port is available
        if not self._is_port_available():
            raise RuntimeError(f"Port {self.port} is already in use on host {self.host}")

        # Create server
        self.server = make_server(self.host, self.port, self.app, threaded=True)

        # Start server in separate thread
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        # Wait a bit for server to start
        time.sleep(0.5)

        print(
            f"Test REST API server started at {'https' if self.ssl else 'http'}://{self.host}:{self.port}{self.prefix}")

    def stop(self):
        """Stop the REST API server."""
        if self.server:
            self.server.shutdown()
            self.server = None

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
            self.thread = None

        print("Test REST API server stopped")

    def is_running(self):
        """Check if the server is running."""
        return self.thread is not None and self.thread.is_alive()

    def get_base_url(self):
        """Get the base URL for the API.

        Returns
        -------
        str
            The base URL for the API.
        """
        protocol = 'https' if self.ssl else 'http'
        return f"{protocol}://{self.host}:{self.port}{self.prefix}"

    def get_server_config(self):
        """Get the server configuration settings."""
        return {"host": self.host, "port": self.port, "prefix": self.prefix, "ssl": self.ssl}

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


def test_rest_api(db_path, port=5000, host='127.0.0.1', prefix='', ssl=False):
    """Create and return a TestRestApi instance.

    This is a convenience function that creates a TestRestApi instance with the
    specified parameters. The returned instance can be used to start/stop a
    test REST API server.

    Parameters
    ----------
    db_path : str or pathlib.Path
        Path to the database directory to serve
    port : int, optional
        Port number for the server (default: 5000)
    host : str, optional
        Host address for the server (default: '127.0.0.1')
    prefix : str, optional
        URL prefix for API endpoints (default: '/api/v1')
    ssl : bool, optional
        Whether to use SSL/HTTPS (default: False)

    Returns
    -------
    TestRestApiServer
        Configured TestRestApi instance

    Examples
    --------
    >>> from plantdb.commons.test_database import test_database
    >>> from plantdb.server.test_rest_api import test_rest_api
    >>> # Create test database
    >>> db = test_database(dataset=None)
    >>> # Create and start REST API server
    >>> api = test_rest_api(db.path(), port=8080)
    >>> api.start()
    >>> # Use the API
    >>> print(f"API running at: {api.get_base_url()}")
    >>> # Stop the server
    >>> api.stop()
    >>>
    >>> # Or use as context manager
    >>> with test_rest_api(db.path(), port=8080) as api:
    ...     # API is running here
    ...     print(f"API URL: {api.get_base_url()}")
    >>> # API is automatically stopped here
    """
    return TestRestApiServer(db_path, port, host, prefix, ssl)
