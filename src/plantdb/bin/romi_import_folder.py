#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import a folder as a ``Fileset`` in a known ``Scan``.
"""
import argparse
import json
import os
from pathlib import Path

from plantdb.fsdb import FSDB


def parsing():
    parser = argparse.ArgumentParser(description="Import a folder as a ``Fileset`` in a known ``Scan``.")
    parser.add_argument('--metadata', metavar='metadata', type=str, default=None,
                        help='JSON or TOML file with metadata.')
    parser.add_argument('folder', metavar='folder', type=str,
                        help='Folder to add to scan. Folder name will be the fileset id.')
    parser.add_argument('scan', metavar='scan', type=str,
                        help='Scan folder to add the fileset to. /path/to/db/scan_id).')
    return parser


def run():
    parser = parsing()
    args = parser.parse_args()
    if args.metadata is not None:
        with open(args.metadata) as json_file:
            metadata = json.load(json_file)
    else:
        metadata = None

    scan_path = Path(args.scan)
    scan_id = os.path.basename(scan_path)
    db_path = scan_path.parent

    db = FSDB(db_path)
    db.connect()

    scan = db.get_scan(scan_id, create=True)

    folder_path = args.folder.rstrip('/')
    path = folder_path.split('/')
    fileset_id = path[-1]

    fileset = scan.create_fileset(fileset_id)
    try:
        for f in os.listdir(folder_path):
            if os.path.isfile(os.path.join(folder_path, f)):
                fi = fileset.create_file(os.path.splitext(f)[0])
                fi.import_file(os.path.join(folder_path, f))
    except:
        scan.delete_fileset(fileset_id)

    if metadata is not None:
        fileset.set_metadata(metadata)


if __name__ == '__main__':
    run()
