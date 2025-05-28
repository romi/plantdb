#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#  Copyright (c) 2022 Univ. Lyon, ENS de Lyon, UCB Lyon 1, CNRS, INRAe, Inria
#  All rights reserved.
#  This file is part of the TimageTK library, and is released under the "GPLv3"
#  license. Please see the LICENSE.md file that should have been included as
#  part of this package.
# ------------------------------------------------------------------------------

"""WSGI Application Entry Point

This module serves as the Web Server Gateway Interface (WSGI) entry point for the application,
allowing web servers like Gunicorn, uWSGI, or Apache with mod_wsgi to interact with the application.

Key Features:
-------------
- Provides the application instance for WSGI-compatible web servers
- Serves as the deployment entry point in production environments
- Separates server configuration from application logic
- Enables standard deployment practices for Python web applications

Usage Examples:
---------------
When deploying with Gunicorn:

```
gunicorn wsgi:app
```

When deploying with uWSGI:
```shell
uwsgi --http :5000 --module plantdb.server.cli.wsgi:application --callable application --master
```
Should then be accessible under: https://localhost:5000/plantds/scans
"""

from plantdb.server.cli.fsdb_rest_api import rest_api

# Get the Flask application
application = rest_api("/data/ROMI/test_owner", proxy=True, log_level='INFO')

if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5000, debug=True)
