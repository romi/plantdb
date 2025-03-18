#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Metadata Management for Database Scans

This script provides functionality for adding metadata to scans in a database.

Key Features:
- Connects to a database using the `FSDB` module.
- Retrieves all available scans from the database.
- Adds or updates metadata to scans for better classification and organization.
- Provides an easy-to-use method for specifying metadata, such as ownership and project details, in bulk or customized workflows.
"""

from plantdb.server.fsdb import FSDB
from plantdb.server.fsdb_tools import add_metadata_to_scan

#path = "/data/ROMI/Romi_Alexis/analyse/Arabidopsis/ahp6_E1"
#path = "/data/ROMI/Romi_Alexis/analyse/Arabidopsis/ahp6_E2"
#path = "/data/ROMI/Romi_GG/analysis/Phyllotaxis_afb1_mutant"
path = "/data/ROMI/test_owner"

owner_id = "geraldine"
project_id = "afb1_mutant"

db = FSDB(path)
db.create_user('alexis', 'Alexis LACROIX')
db.create_user('geraldine', 'Géraldine BRUNOUD')
db.create_user('fabfab', 'Fabrice Besnard')
db.connect(owner_id)

db.list_scans()  # list scans owned by current user

for scan in db.get_scans(owner_only=False):
    new_md = {}
    # Process the dataset owner
    owner = scan.get_metadata("owner", None)
    if owner is None:
        new_md["owner"] = db.user
    else:
        print(f"Scan '{scan.id}' already has an owner: '{owner}'!")
    # Process the project the dataset belongs to
    project = scan.get_metadata("project", None)
    if project is None:
        project = [project_id]
    else:
        if project_id not in project:
            project.append(project_id)
    new_md["project"] = project
    # Update the scan metadata
    add_metadata_to_scan(db, scan.id, metadata=new_md)

