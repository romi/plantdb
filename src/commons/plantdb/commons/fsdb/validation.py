#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Python File System Validation Module

A utility module that provides essential validation functions for file system operations and identifier naming conventions.
This module ensures data integrity and safe operations when working with file system databases (FSDB) and identifiers.

## Key Features

- Identifier validation with comprehensive checks for naming conventions
- File system database (FSDB) validation
- Safe deletion verification for file system paths
- Detailed logging of validation failures
- Support for both string and Path objects for file system operations

## Usage Examples

```python
>>> from plantdb.commons.fsdb.validation import _is_valid_id, _is_fsdb, _is_safe_to_delete
>>> from pathlib import Path

>>> # Validate an identifier
>>> result = _is_valid_id("valid-name.123")  # Returns True
>>> result = _is_valid_id("invalid/name")    # Returns False

>>> # Check if a path is an FSDB database
>>> db_path = Path("/path/to/database")
>>> is_db = _is_fsdb(db_path)

>>> # Verify if a path is safe to delete
>>> path_to_delete = Path("/path/to/database/subfolder")
>>> is_safe = _is_safe_to_delete(path_to_delete)
```
"""

from pathlib import Path

from .path_helpers import _fileset_path
from .path_helpers import _scan_json_file
from ..log import get_logger

logger = get_logger(__name__)


def _is_valid_id(name):
    """Checks the validity of a given identifier name based on specified conditions.

    This function validates whether the provided name is a valid identifier by ensuring it
    is a string, non-empty, not longer than 255 characters, and contains only allowable
    characters. It logs errors for invalid input.

    Parameters
    ----------
    name : str
        The identifier name to be validated.

    Returns
    -------
    bool
        ``True`` if the name is valid, otherwise ``False``.
    """
    import re
    # Check if the name is a string
    if not isinstance(name, str):
        logger.error(f"Given name is not a string: '{name}'")
        return False

    # Check if the string is empty or too long (e.g., limit to 255 characters)
    if not name or len(name) > 255:
        logger.error(f"Given name is empty or too long: '{name}'")
        return False

    # Check for invalid characters (disallow slashes, backslashes, etc.)
    # Here we use a regex to allow alphanumeric, underscores, dashes and dots only
    if not re.match(r'^[\w\-\.]+$', name):
        logger.error(f"Given name contains invalid characters: '{name}'.")
        logger.info("Only alphanumeric characters, underscores, dashes and dots are allowed.")
        return False

    return True


def _is_fsdb(path, validate_json_fileset=False, extra_dirs: list[str] = ['configs']) -> bool:
    """Test if the given path is indeed an FSDB database.

    Do it by checking the presence of the ``MARKER_FILE_NAME`` and validating
    the directory structure to ensure it's a complete FSDB.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test as a valid FSDB database.
    validate_json_fileset : bool
        A boolean flag to enable the validation of the filesets defined in the scan's `files.json`.
    extra_dirs : list of str
        A list of directory names that can reside at the root of the filesystem database without trowing an error.
        Defaults to ``['configs']``.

    Returns
    -------
    bool
        ``True`` if an FSDB database, else ``False``.

    Examples
    --------
    >>> from plantdb.commons.fsdb.validation import _is_fsdb
    >>> from plantdb.commons.test_database import setup_empty_database
    >>> from plantdb.commons.test_database import setup_test_database
    >>> path = setup_empty_database()  # initialize an empty FSDB in the temporary directory
    >>> print(path)
    /tmp/ROMI_DB_********
    >>> print([path.name for path in path.iterdir()])  # only the 'marker' file is created
    ['romidb']
    >>> _is_fsdb(path)
    True
    >>> path = setup_test_database('real_plant', None)  # initialize an FSDB in the temporary directory with a test dataset
    >>> print(path)
    /tmp/ROMI_DB_********
    >>> print([path.name for path in path.iterdir()])  # database with a single dataset
    ['romidb', 'real_plant', 'groups.json', 'users.json', '.locks']
    >>> _is_fsdb(path)
    True
    """
    from .core import MARKER_FILE_NAME

    path = Path(path)
    # Check if the path is a directory
    if not path.is_dir():
        logger.error("The provided path is not related to a directory.")
        return False

    # Check if the marker file exists (original check)
    marker_path = path / MARKER_FILE_NAME
    if not marker_path.is_file():
        logger.error(f"The given path does not contain the required marker file '{MARKER_FILE_NAME}'.")
        return False

    # Check for at least one scan directory with proper structure
    scan_dirs = [f for f in path.iterdir() if f.is_dir() and not f.name.startswith('.')]
    # If no scan directories, it's an empty FSDB
    if not scan_dirs:
        logger.warning(f"The FSDB at '{path}' is empty.")
        return True  # Still valid as an empty FSDB

    bad_dir = []
    # Check if the scan directories have the required structure
    for scan_dir in scan_dirs:
        if scan_dir.name in extra_dirs:
            continue  # skip the verification for any folder declared as "extra"
        if not _is_scan_dataset(scan_dir, validate_json_fileset):
            bad_dir.append(str(scan_dir))

    if len(bad_dir) > 0:
        logger.warning(f"Found {len(bad_dir)} bad scan directories in FSDB database at: {path}")
        logger.info(f"{', '.join(bad_dir)}")  # list the bad scans
        logger.info("Use the `fsdb_healthcheck` CLI script to fix this.")

    return not bad_dir


def _is_scan_dataset(scan_path, validate_json_fileset=False) -> bool:
    """Test if the given path is an FSDB dataset.

    Parameters
    ----------
    scan_path : str or pathlib.Path
        A path to test as a valid FSDB scan dataset.
    validate_json_fileset : bool
        A boolean flag to enable the validation of the filesets defined in the scan's `files.json`.

    Returns
    -------
    bool
        ``True`` if the path is an FSDB scan dataset, else ``False``.
    """
    import json

    # Check for required subdirectories
    metadata_dir = scan_path / "metadata"
    if not metadata_dir.is_dir():
        logger.error(f"Missing required 'metadata' directory for '{scan_path.name}'.'")
        return False

    # Check if scan directory contains `files.json`
    files_json_path = scan_path / "files.json"
    if not files_json_path.is_file():
        logger.error(f"Missing required `files.json` in '{scan_path.name}'.'")
        return False

    # Try to parse files.json to ensure it's valid
    try:
        with open(files_json_path, 'r') as f:
            files_data = json.load(f)
    except (json.JSONDecodeError, IOError):
        # If files.json is not valid JSON or cannot be read, continue to next scan
        logger.error(f"Could not load required `files.json` from '{scan_path.name}'.'")
        return False

    # Check if files.json has the expected structure
    if "filesets" not in files_data:
        logger.error(f"Missing required 'filesets' entry in `files.json` for '{scan_path.name}'.'")
        return False

    if validate_json_fileset:
        for fs in files_data["filesets"]:
            _is_valid_fileset(scan_path, fs["id"], fs['files'])

    # If we reach here, we have a valid scan dataset structure
    return True


def _is_valid_fileset(scan_path, fileset_id, fs_info) -> bool:
    """Check if a given fileset ID corresponds to a valid fileset in the scan.

    A valid fileset must:
    1. Be defined in the scan's files.json registry
    2. Have the required files present

    Parameters
    ----------
    scan_path : str or pathlib.Path
        A path to test as a valid FSDB scan dataset.
    fileset_id : str
        The ID of the fileset to check.
    fs_info : list
        The list of files from the fileset to check.

    Returns
    -------
    bool
        ``True`` if the fileset is valid, ``False`` otherwise.
    """
    fs_path = scan_path / fileset_id
    if not fs_path.is_dir():
        logger.error(f"Missing fileset '{fileset_id}' defined in `files.json` for '{scan_path}'.'")
        return False

    # Check that all required files exist
    file_exists = _fileset_files_exists(fs_info, fs_path)
    if all(file_exists):
        return True
    else:
        logger.error(f"Missing {len(file_exists) - sum(file_exists)} files in '{fileset_id}' for '{scan_path}'.")
        return False


def _fileset_files_exists(fs_info, fs_path) -> list[bool]:
    """Check that all required files exist."""
    file_exists = []
    for file_info in fs_info:
        file_id = file_info.get("id")
        file_name = file_info.get("file")

        if file_id and file_name:
            # Construct the path to the file
            file_path = fs_path / file_name
            if not file_path.is_file():
                file_exists.append(False)
            else:
                file_exists.append(True)

    return file_exists


def _is_safe_to_delete(path) -> bool:
    """Tests if a given path is safe to delete.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test for safe deletion.

    Returns
    -------
    bool
        ``True`` if the path is safe to delete, else ``False``.

    Notes
    -----
    A path is safe to delete only if it's a sub-folder of a db.
    """
    path = Path(path).resolve()
    while True:
        # Test if the current path is a local DB (FSDB):
        if _is_fsdb(path):
            return True  # exit and return `True` if it is
        # Else, move to the parent directory & try again:
        newpath = path.parent
        # Check if we have indeed moved up to the parent directory
        if newpath == path:
            # Stop if we did not
            return False
        path = newpath
