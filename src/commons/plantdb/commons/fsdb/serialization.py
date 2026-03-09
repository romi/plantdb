#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Filesystem Database Serialization

This module provides functionality for serializing and deserializing filesystem database (FSDB) objects, facilitating the structured management and manipulation of files and filesets within a scan-based hierarchical storage system.

## Key Features

* Parsing and conversion of filesystem database components (Scans, Filesets, and Files)
* Validation of filesystem entries and their required attributes
* Hierarchical data structure serialization to dictionary format
* Error handling for missing IDs, filenames, and non-existent files
* Path validation and verification for filesystem integrity

## Usage Examples

```python
# Example of converting a scan to a dictionary representation
from plantdb.commons.fsdb.core import Scan

# Assuming we have a scan object
scan = Scan(...)

# Convert scan structure to dictionary
scan_dict = _scan_to_dict(scan)
# Result format:
# {
#     "filesets": [
#         {
#             "id": "fileset_id",
#             "files": [
#                 {
#                     "id": "file_id",
#                     "file": "filename.ext"
#                 }
#             ]
#         }
#     ]
# }

# Parse a fileset from dictionary
fileset_info = {"id": "fileset_id"}
fileset = _parse_fileset(scan, fileset_info)

# Parse a file from dictionary
file_info = {"id": "file_id", "file": "example.txt"}
file = _parse_file(fileset, file_info)
```
"""

from .path_helpers import _file_path
from .path_helpers import _fileset_path
from .exceptions import FileNoFileNameError
from .exceptions import FileNoIDError
from .exceptions import FileNotFoundError
from .exceptions import FilesetNoIDError
from .exceptions import FilesetNotFoundError
from ..log import get_logger

logger = get_logger(__name__)

def _parse_fileset(scan, fileset_info):
    """Get a `Fileset` instance for given `db` & `scan` by parsing provided `fileset_info`.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The scan instance to associate the returned ``Fileset`` to.
    fileset_info : dict
        The fileset dictionary with the fileset 'id' entry

    Returns
    -------
    plantdb.commons.fsdb.core.Fileset
        The ``Fileset`` instance from parsed JSON.
    """
    from plantdb.commons.fsdb.core import Fileset
    fsid = fileset_info.get("id", None)
    if fsid is None:
        raise FilesetNoIDError("Fileset: No ID")

    fileset = Fileset(scan, fsid)
    # Get the expected directory path and check it exists:
    path = _fileset_path(fileset)
    if not path.is_dir():
        logger.debug(f"Missing fileset directory: {path}")
        raise FilesetNotFoundError(scan, fsid)
    return fileset


def _parse_file(fileset, file_info):
    """Get a `File` instance for given `fileset` by parsing provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        The fileset instance to associate the returned ``File`` to.
    file_info : dict
        The file dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.core.File
        The ``File`` instance from parsed JSON.

    Raises
    ------
    plantdb.commons.fsdb.exceptions.FileNoIDError
        If the 'id' entry is missing from `file_info`.
    plantdb.commons.fsdb.exceptions.FileNoFileNameError
        If the 'file' entry is missing from `file_info`.
    plantdb.commons.fsdb.exceptions.FileNotFoundError
        If the file is not found on drive.
    """
    from plantdb.commons.fsdb.core import File
    fid = file_info.get("id", None)
    if fid is None:
        logger.debug(f"Input `file_info`: {file_info}")
        raise FileNoIDError(f"File: No ID for file '{file_info}'")

    filename = file_info.get("file", None)
    if filename is None:
        logger.debug(f"Input `file_info`: {file_info}")
        raise FileNoFileNameError(f"File: No filename for file '{file_info}'")

    file = File(fileset, fid)
    file.filename = filename  # set the filename attribute
    # Get the expected file path and check it exists:
    path = _file_path(file)
    if not path.is_file():
        logger.debug(f"Missing file: {path}")
        raise FileNotFoundError(f"File: Not found at '{path}'")
    return file


def _file_to_dict(file):
    """Returns a dict with the file "id" and "filename".

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        A file to get "id" and "filename" from.

    Returns
    -------
    dict
        ``{"id": file.id, "file": file.filename}``
    """
    return {"id": file.id, "file": file.filename}


def _fileset_to_dict(fileset):
    """Returns a dict with the fileset "id" and the "files" dict.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        A fileset to get "id" and "files" dictionary from.

    Returns
    -------
    dict
        ``{"id": fileset.id, "files": {"id": file.id, "file": file.filename}}``

    See Also
    --------
    plantdb.commons.fsdb._file_to_dict
    """
    files = []
    for f in fileset.files.values():
        files.append(_file_to_dict(f))
    return {"id": fileset.id, "files": files}


def _scan_to_dict(scan):
    """Returns a dict with the scan's filesets and files structure.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        A scan instance to get underlying filesets and files structure from.

    Returns
    -------
    dict
        ``{"filesets": {"id": fileset.id, "files": {"id": file.id, "file": file.filename}}}``

    See Also
    --------
    plantdb.commons.fsdb._fileset_to_dict
    plantdb.commons.fsdb._file_to_dict
    """
    filesets = []
    for fileset in scan.filesets.values():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}
