#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Metadata Management for Database Scans

This module provides functionality for adding metadata to scans in a database.

Key Features:
- Connects to a database using the `FSDB` module.
- Retrieves all available scans from the database.
- Adds or updates metadata to scans for better classification and organization.
- Provides an easy-to-use method for specifying metadata, such as ownership and project details, in bulk or customized workflows.
"""

import os

from plantdb.fsdb import FSDB
from plantdb.fsdb_tools import add_metadata_to_scan

path = os.environ.get('ROMI_DB', None)

if path is None:
    raise ValueError('Environment variable ROMI_DB not set.')

db = FSDB(path)
db.connect("anonymous")

for scan in db.get_scans():
    new_md = {"owner": "test", "project": "test"}
    add_metadata_to_scan(db, scan, metadata=new_md)