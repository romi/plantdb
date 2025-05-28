#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ------------------------------------------------------------------------------
#  Copyright (c) 2022 Univ. Lyon, ENS de Lyon, UCB Lyon 1, CNRS, INRAe, Inria
#  All rights reserved.
#  This file is part of the TimageTK library, and is released under the "GPLv3"
#  license. Please see the LICENSE.md file that should have been included as
#  part of this package.
# ------------------------------------------------------------------------------

"""
```shell
uwsgi --http :5000 --module plantdb.server.cli.wsgi:application --callable application --master
```
"""

from plantdb.server.cli.fsdb_rest_api import rest_api

# Get the Flask application
application = rest_api("/data/ROMI/test_owner", proxy=True, log_level='INFO')

if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5000, debug=True)
