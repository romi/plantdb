#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from logging import Logger
from pathlib import Path

import click
from click_option_group import OptionGroup
from click_option_group import optgroup
from send2trash import send2trash

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.core import MARKER_FILE_NAME
from plantdb.commons.fsdb.exceptions import NotAnFSDBError
from plantdb.commons.fsdb.file_ops import _load_scan
from plantdb.commons.fsdb.validation import _is_scan_dataset
from plantdb.commons.log import LOG_LEVELS
from plantdb.commons.log import get_logger
from plantdb.commons.utils import yes_no_abort_choice


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.argument('fsdb_path', type=click.Path(exists=True))
@optgroup.group("Fix", cls=OptionGroup)
@optgroup.option('--fix', is_flag=True,
                 help="Fix all errors by removing missing references and importing extra local files.")
@optgroup.option('--fix-missing', is_flag=True,
                 help="Remove missing references from each scan’s <code>files.json</code>")
@optgroup.option('--fix-extra', is_flag=True,
                 help="Import extra local files that are missing from the fileset.")
@optgroup.group("Logging", cls=OptionGroup)
@optgroup.option('--log-level', 'log_level', type=click.Choice(LOG_LEVELS), default='INFO',
                 help="Level of message logging.", show_default=True)
def main(fsdb_path, fix, fix_missing, fix_extra, log_level):
    """Perform a sanity check of the given local File System DataBase (FSDB).

    This command performs health checks on the FSDB, identifying and optionally fixing
    inconsistencies in scan references and filesets.
    """
    # - Configure a logger from this application:
    global logger
    logger = get_logger('fsdb_healthcheck', log_level=log_level)  # set level from CLI option

    # Verify provided path is a directory
    fsdb_path = Path(fsdb_path)
    if not fsdb_path.is_dir():
        logger.error("The provided path is not a directory.")
        raise ValueError("The provided path is not a directory.")

    # Ensure FSDB marker file exists to confirm valid FSDB
    marker_path = fsdb_path / MARKER_FILE_NAME
    if not marker_path.is_file():
        logger.error(f"The given path does not contain the required marker file '{MARKER_FILE_NAME}'.")
        raise NotAnFSDBError(f"The given path does not refer to a valid FSDB.")

    # Gather scan directories (ignore hidden folders)
    scan_dirs = [f for f in fsdb_path.iterdir() if f.is_dir() and not f.name.startswith('.')]
    # Empty FSDB handling
    if not scan_dirs:
        logger.warning(f"The FSDB at '{fsdb_path}' is empty.")
        exit(0)  # Still valid as an empty FSDB

    # If --fix flag is set, enable missing‑reference fixing (extra fixing not yet implemented)
    if fix:
        fix_missing = True
        # fix_extra = True  # TODO: write the method first!

    # Instantiate FSDB object without authentication
    db = FSDB(fsdb_path, no_auth=True)
    # do NOT use the `connect()` method

    if fix_missing:
        logger.info("Removing missing reference from each scan's 'files.json'...")
        fix_missing_scans_reference(db, scan_dirs, logger)

    if fix_extra:
        # logger.info("Importing new fileset references to 'files.json'...")
        raise NotImplementedError


def fix_missing_scans_reference(db: FSDB, scan_dirs: list[Path], logger: Logger):
    """Identify and optionally remove scans with structural problems.

    The function walks through each directory in ``scan_dirs`` and validates the expected FSDB scan layout.
    Scans that fail the structural check are collected and presented to the user for confirmation before
    being moved to the operating system’s trash bin.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        An already‑initialized ``FSDB`` object representing the target file‑system database.
        Authentication is not required for this operation.
    scan_dirs : list[Path]
        A list path, each pointing to a scan directory inside the FSDB.
    logger : Logger
        A ``Logger`` instance used for informational and warning messages throughout the process.

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
    # Track scans with structural problems
    bad_dir = []
    for scan_path in scan_dirs:
        # Validate scan folder structure (skip JSON fileset validation for now)
        if not _is_scan_dataset(scan_path, validate_json_fileset=False):
            bad_dir.append(scan_path)  # mark for possible removal
        scan_id = scan_path.name
        _ = _load_scan(db, scan_id, updates_files_json=True)  # load scan; updates files.json if needed

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
        logger.info(f"All {len(scan_dirs)} scans are healthy!")


if __name__ == '__main__':
    main()
