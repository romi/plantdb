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
from path_helpers import _scan_path, _file_path, _get_filename

# Get the path to a scan directory
scan_path = _scan_path(scan_object)

# Get the path to a specific file within a fileset
file_path = _file_path(file_object)

# Generate a standardized filename with extension
new_filename = _get_filename(file_object, "jpg")  # Returns: "file_id.jpg"
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
