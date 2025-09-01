#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""WSGI Application Entry Point

This module serves as the Web Server Gateway Interface (WSGI) entry point for the application,
allowing web servers like Gunicorn, uWSGI, or Apache with mod_wsgi to interact with the application.

Key Features
------------
- Provides the application instance for WSGI-compatible web servers
- Serves as the deployment entry point in production environments
- Separates server configuration from application logic
- Enables standard deployment practices for Python web applications

Environment Variables
---------------------
- ``ROMI_DB`` : Path to the directory containing the FSDB. Default: '/myapp/db' (container)
- ``PLANTDB_API_PREFIX``: Prefix for the REST API URL. Default is empty.

Usage Examples
--------------
When deploying with uWSGI:

.. code-block:: bash

   uwsgi --http :5000 --module plantdb.server.cli.wsgi:application --callable application --master

Should then be accessible under: http://localhost:5000/plantdb/scans
"""

import os

from plantdb.server.cli.fsdb_rest_api import rest_api

# Get the path to the FSDB to serve using `ROMI_DB` environment variable, use '/myapp/db' as default (container)
romi_db = os.environ.get('ROMI_DB', '/myapp/db')
# Get the FSDB REST API URL prefix
url_prefix = os.environ.get("PLANTDB_API_PREFIX", "")

# Get the Flask application with a Proxy:
application = rest_api(romi_db, proxy=True, url_prefix=url_prefix,
                       log_level='INFO', test=False, empty=False, models=False)

if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5000, debug=True)
