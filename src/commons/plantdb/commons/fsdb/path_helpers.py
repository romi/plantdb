#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Path Helpers Module

A utility module that provides standardized path management functions for accessing and manipulating file paths within a plant database system.
This module simplifies the handling of file paths for scans, filesets, and individual files while maintaining a consistent directory structure.

## Key Features

- File system path resolution for scan directories and their components
- Path generation for various JSON metadata files (`files.json`, `metadata.json`, `measures.json`)
- Directory path management for filesets and their associated metadata
- File path resolution for individual files and their metadata
- Utility function for generating standardized filenames with specific extensions

## Usage Examples

```python
>>> from plantdb.commons.fsdb.path_helpers import _scan_path, _file_path, _get_filename
>>> from plantdb.commons.test_database import test_database
>>> # Initialize the database (creates base directory if needed)
>>> db = test_database(no_auth=True)
>>> db.connect()
>>> # Create a new scan named "experiment‑001"
>>> scan = db.create_scan("experiment-001")
>>> # Get the path to a scan directory
>>> scan_path = _scan_path(scan)
>>> print(scan_path)
/tmp/ROMI_DB_y1m6n8eq/experiment-001
>>> # Add a fileset to the scan
>>> fileset = scan.create_fileset("raw-data")
>>> # Store a file inside the fileset
>>> file = fileset.create_file("sensor")
>>> # Generate a standardized filename with extension
>>> new_filename = _get_filename(file, "csv")  # Returns: "file_id.csv"
>>> print(new_filename)
sensor.csv
>>> file.write_raw(b"timestamp,value\\n0,12.3\\n1,13.7", ext="csv")
>>> file_path = _file_path(file)
>>> print(file_path)
/tmp/ROMI_DB_kke0pk9s/experiment-001/raw-data/sensor.csv
```
"""
import pathlib


def _scan_path(scan) -> pathlib.Path:
    """Get the path to given scan.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        A scan to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the scan directory.
    """
    return (scan.db.basedir / scan.id).resolve()


def _scan_json_file(scan) -> pathlib.Path:
    """Get the path to scan's "files.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        A scan to get the files JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "files.json" file.
    """
    return _scan_path(scan) / "files.json"


def _scan_metadata_path(scan) -> pathlib.Path:
    """Get the path to scan's "metadata.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        A scan to get the metadata JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "metadata.json" file.
    """
    return _scan_path(scan) / "metadata" / "metadata.json"


def _scan_measures_path(scan) -> pathlib.Path:
    """Get the path to scan's "measures.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        A scan to get the measures JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "measures.json" file.
    """
    return _scan_path(scan) / "measures.json"


def _fileset_path(fileset) -> pathlib.Path:
    """Get the path to given fileset directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        A fileset to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset directory.
    """
    return _scan_path(fileset.scan) / fileset.id


def _fileset_metadata_path(fileset) -> pathlib.Path:
    """Get the path to given fileset metadata directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        A fileset to get the metadata directory path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset metadata directory.
    """
    return _scan_path(fileset.scan) / "metadata" / fileset.id


def _fileset_metadata_json_path(fileset) -> pathlib.Path:
    """Get the path to `fileset.id` metadata JSON file.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        A fileset to get the JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the f`fileset.id` metadata JSON file.
    """
    return _scan_path(fileset.scan) / "metadata" / f"{fileset.id}.json"


def _file_path(file) -> pathlib.Path:
    """Get the path to given file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        A file to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the file.
    """
    return _fileset_path(file.fileset) / file.filename


def _file_metadata_path(file) -> pathlib.Path:
    """Get the path to `file.id` metadata JSON file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        A file to get the metadata JSON path from.

    Returns
    -------
    pathlib.Path
        The path to the "<File.id>.json" file.
    """
    return _scan_path(file.fileset.scan) / "metadata" / file.fileset.id / f"{file.id}.json"


def _get_filename(file, ext) -> str:
    """Returns a `file` name using its ``id`` attribute and given extension.

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        A File object.
    ext : str
        The file extension to use.

    Returns
    -------
    str
        The corresponding file's name.
    """
    # Remove starting dot from extension:
    if ext.startswith('.'):
        ext = ext[1:]
    return f"{file.id}.{ext}"
