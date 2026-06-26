#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import json
import pathlib
from pathlib import Path
from shutil import copy

import click
from tqdm import tqdm

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.path_helpers import _scan_json_file
from plantdb.commons.log import LOG_LEVELS

from plantdb.commons.log import get_logger


@click.command()
@click.argument('fsdb_path', type=click.Path(exists=True))
@click.option('--log-level', 'log_level', type=click.Choice(LOG_LEVELS), default='INFO',
              help="Level of message logging.", show_default=True)
@click.option('--fix', is_flag=True,
              help="Use this flag to fix all errors: remove missing reference & import extra local files.")
@click.option('--fix-missing', is_flag=True,
              help="Use this flag to remove missing reference from each scan 'files.json'.")
@click.option('--fix-extra', is_flag=True,
              help="Use this flag to import extra local file missing from fileset.")
def main(fsdb_path, log_level, fix, fix_missing, fix_extra):
    """Perform a sanity check of the given local File System DataBase (FSDB)."""
    # - Configure a logger from this application:
    global logger
    logger = get_logger('fsdb_healthcheck', log_level=log_level)

    db = FSDB(fsdb_path)
    db.connect()

    if fix:
        fix_missing = True
        # args.fix_extra = True  # TODO: write the method first!

    if fix_missing:
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

    if fix_extra:
        raise NotImplementedError

    db.disconnect()


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


if __name__ == '__main__':
    main()