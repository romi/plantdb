#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Health‑Check CLI

A command‑line utility that validates a local File System DataBase (FSDB) for structural consistency, reporting missing
references, and optionally fixing them.
It helps maintain clean, reliable scan datasets by detecting broken `files.json` entries and safely removing corrupt scans.

## Key Features

- **Sanity checking** of an FSDB directory, ensuring required marker files are present.
- **Detection of missing scan references** and automatic correction of `files.json` entries.
- **Interactive cleanup** of scans with structural problems, moving them to the OS trash after user confirmation.
- **Selective fixing** via `--fix-missing` (remove missing references) or `--fix-extra` (future support for importing extra files).
- **Configurable logging** with standard log‑level choices (`INFO`, `DEBUG`, etc.).
- **Zero‑auth operation**: works on local FSDBs without needing authentication.

## Usage Examples

### Basic health check (read‑only)
```shell
fsdb_healthcheck /path/to/my_fsdb

### Identify and automatically remove missing references
```shell
fsdb_healthcheck /path/to/my_fsdb --fix-missing
```
"""

import os
from logging import Logger
from pathlib import Path

import click
from click_option_group import OptionGroup
from click_option_group import optgroup
from send2trash import send2trash

from plantdb.commons.log import get_logger
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import DEFAULT_LOG_LEVEL

# Create a logger and set the environment variable
os.environ.setdefault('ROMI_APP_LOGGER', __name__.split('.')[-1])
logger = get_logger(os.getenv('ROMI_APP_LOGGER'), log_level=DEFAULT_LOG_LEVEL)

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.commons.fsdb.exceptions import NotAnFSDBError
from plantdb.commons.fsdb.file_ops import _load_scan
from plantdb.commons.fsdb.path_helpers import iter_scan_paths
from plantdb.commons.fsdb.validation import _is_scan_dataset
from plantdb.commons.utils import yes_no_abort_choice


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('db_path', type=click.Path(exists=True))
@optgroup.group("Fix", cls=OptionGroup)
@optgroup.option('--fix', is_flag=True,
                 help="Fix all errors by removing missing references and importing extra local files.")
@optgroup.option('--fix-missing', is_flag=True,
                 help="Remove missing references from each scan’s `files.json`.")
@optgroup.option('--fix-extra', is_flag=True,
                 help="Import extra local files that are missing from the fileset.")
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option('--log-level', 'log_level', type=click.Choice(LOG_LEVELS), default=DEFAULT_LOG_LEVEL,
                 help="Level of message logging.", show_default=True)
def main(db_path, fix, fix_missing, fix_extra, log_level):
    """Perform a sanity check of the given local File System DataBase (FSDB).

    This command performs health checks on the FSDB, identifying and optionally fixing
    inconsistencies in scan references and filesets.
    """
    # Get the logger and change the level if needed:
    logger = get_logger(os.environ.get('ROMI_APP_LOGGER', __name__))
    logger.setLevel(log_level)

    # Verify provided path is a directory
    db_path = Path(db_path)
    if not db_path.is_dir():
        logger.error("The provided path is not a directory.")
        raise ValueError("The provided path is not a directory.")

    # Ensure FSDB marker file exists to confirm valid FSDB
    marker_path = db_path / MARKER_FILE_NAME
    if not marker_path.is_file():
        logger.error(f"The given path does not contain the required marker file '{MARKER_FILE_NAME}'.")
        raise NotAnFSDBError(f"The given path does not refer to a valid FSDB.")

    # Instantiate FSDB object without authentication
    db = FSDB(db_path, no_auth=True)
    # do NOT use the `connect()` method

    # Empty FSDB handling
    if not any(iter_scan_paths(db_path)):
        # also consider hidden/extra check via iter helper; empty if no scans
        if not [f for f in db_path.iterdir() if f.is_dir() and not f.name.startswith('.')]:
            logger.warning(f"The FSDB at '{db_path}' is empty.")
            exit(0)  # Still valid as an empty FSDB

    # If --fix flag is set, enable missing‑reference fixing (extra fixing not yet implemented)
    if fix:
        fix_missing = True
        # fix_extra = True  # TODO: write the method first!

    if fix_missing:
        logger.info("Removing missing reference from each scan's 'files.json'...")
        fix_missing_scans_reference(db, logger)

    if fix_extra:
        # logger.info("Importing new fileset references to 'files.json'...")
        raise NotImplementedError


def fix_missing_scans_reference(db: FSDB, logger: Logger):
    """Identify and optionally remove scans with structural problems.

    The function walks through each scan in the DB (expanding timelapse
    containers) and validates the expected FSDB scan layout. Scans that
    fail the structural check are collected and presented to the user for
    confirmation before being moved to the operating system’s trash bin.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        An already‑initialized ``FSDB`` object representing the target
        file‑system database. Authentication is not required.
    logger : Logger
        A ``Logger`` instance used for informational and warning messages.

    Notes
    -----
    * The structural validation is performed by `_is_scan_dataset` with ``validate_json_fileset=False``
    * Missing references are fixed by calling ``_load_scan(..., updates_files_json=True)`` which rewrites
      the ``files.json`` file if necessary, after a backup.
    * Deleting scans is done via `send2trash`, which moves the directories to the OS trash instead
      of permanently removing them.

    See Also
    --------
    plantdb.commons.fsdb.validation._is_scan_dataset
    plantdb.commons.fsdb.file_ops._load_scan
    send2trash.send2trash
    """
    bad_dir = []
    total_scans = 0
    for scan_path in iter_scan_paths(db.path()):
        total_scans += 1
        if not _is_scan_dataset(scan_path, validate_json_fileset=False):
            bad_dir.append(scan_path)
        _ = _load_scan(db, scan_path.name, updates_files_json=True)

    if bad_dir:
        n_bad = len(bad_dir)
        logger.info(f"Found {n_bad} bad scans: {', '.join([scan.name for scan in bad_dir])}")

        # Display prominent warning before user confirmation
        warning_msg = (
            "\n"
            "!!! WARNING !!!\n"
            "The operation will DELETE the scans identified as 'bad scans'.\n"
            "Please review these scans thoroughly before confirming.\n"
            "Proceed with caution!\n"
        )
        click.secho(warning_msg, fg='red', bold=True)

        # Prompt until a definitive answer is given
        answer = False
        while answer != True:
            answer = yes_no_abort_choice(
                f"Did you review the scans that will be removed?",
                default=False, default_abort=False,
            )
            if answer is None:
                logger.warning("Aborted!")
                exit(0)

        # Final confirmation before deletion
        answer = yes_no_abort_choice(
            f"Do you want to remove {'this' if n_bad == 1 else 'these'} {n_bad} scan{'' if n_bad == 1 else 's'}?",
            default=False, default_abort=True,
        )
        if answer is None:
            logger.warning("Aborted!")
            exit(0)

        if answer:
            logger.info(f"Moving bad scans to the trash bin...")
            for scan_name in bad_dir:
                send2trash(scan_name)  # move to OS trash
            logger.info("Done.")
    else:
        logger.info(f"All {total_scans} scans are healthy!")


if __name__ == '__main__':
    main()
