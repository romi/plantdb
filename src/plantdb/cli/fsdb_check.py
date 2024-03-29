#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import datetime
import json
import pathlib
from pathlib import Path
from shutil import copy

from tqdm import tqdm

from plantdb import FSDB
from plantdb.fsdb import _scan_json_file
from plantdb.log import LOGLEV
from plantdb.log import configure_logger


def parsing():
    parser = argparse.ArgumentParser(description='Check the local File System DataBase.')
    parser.add_argument('database', type=str,
                        help='Local database to check.')

    parser.add_argument('--log-level', dest='log_level', type=str, default='INFO', choices=LOGLEV,
                        help="Level of message logging, defaults to 'INFO'.")

    fix_opt = parser.add_argument_group("fix options")
    fix_opt.add_argument('--fix', action="store_true",
                         help="Fix all errors: remove missing reference & import extra local files.")
    fix_opt.add_argument('--fix-missing', dest='fix_missing', action="store_true",
                         help="Remove missing reference from each scan 'files.json'.")
    fix_opt.add_argument('--fix-extra', dest='fix_extra', action="store_true",
                         help="Import extra local file missing from fileset.")

    return parser


def backup_filename(file):
    """Create a backup a filename by adding a timestamp.

    Parameters
    ----------
    file : str or pathlib.Path
        The path to the file to backup.

    Examples
    --------
    >>> backup_filename("test/file.json")
    PosixPath('test/file_230601_105815.json')
    """
    file = Path(file)
    now = datetime.datetime.now()
    timestamp = now.strftime("%y%m%d_%H%M%S")
    fname = file.stem
    return file.with_stem(f"{fname}_{timestamp}")


def backup_file(file):
    """Backup a file by creating a timestamped copy.

    Parameters
    ----------
    file : str or pathlib.Path
        The path to the file to backup.
    """
    bak_fname = backup_filename(file)
    copy(file, bak_fname)
    return bak_fname


def _load_json(file: pathlib.Path) -> dict:
    with file.open(mode='r') as f:
        json_dict = json.load(f)
    return json_dict


def same_jsons(file_a, file_b) -> bool:
    """Test if two JSON files have the same content."""
    # Basic check, maybe using a third-party lib like `deepdiff` could give more insight.
    file_a = Path(file_a)
    file_b = Path(file_b)
    json_a = _load_json(file_a)
    json_b = _load_json(file_b)
    return json_a == json_b


def main():
    # - Parse the input arguments to variables:
    parser = parsing()
    args = parser.parse_args()

    # - Configure a logger from this application:
    global logger
    logger = configure_logger('fsdb_check', log_level=args.log_level)

    db = FSDB(args.database)
    db.connect()

    if args.fix:
        args.fix_missing = True
        # args.fix_extra = True  # TODO: write the method first!

    if args.fix_missing:
        logger.info("Removing missing reference from each scan 'files.json'...")
        for scan in tqdm(db.get_scans(), unit='scan'):
            scan_json = _scan_json_file(scan)
            # Backup the scan JSON file:
            bak_json = backup_file(scan_json)
            # Update the scan JSON file with loaded references only:
            scan.store()
            # Remove backup if it duplicates the scan JSON file contents:
            if same_jsons(scan_json, bak_json):
                logger.debug(f"Backup JSON '{bak_json.name}' duplicates scan JSON '{scan.id}'.")
                logger.debug(f"Removing '{bak_json.resolve()}'.")
                bak_json.unlink()

    if args.fix_extra:
        raise NotImplementedError

    db.disconnect()

if __name__ == '__main__':
    main()