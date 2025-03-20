#!/usr/bin/env python
# -*- coding: utf-8 -*-

from .path_helpers import _file_path
from .path_helpers import _fileset_path
from .exceptions import FileNoFileNameError
from .exceptions import FileNoIDError
from .exceptions import FilesetNoIDError
from .exceptions import FilesetNotFoundError
from ..log import get_logger

logger = get_logger(__name__)

def _parse_fileset(scan, fileset_info):
    """Get a `Fileset` instance for given `db` & `scan` by parsing provided `fileset_info`.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The scan instance to associate the returned ``Fileset`` to.
    fileset_info : dict
        The fileset dictionary with the fileset 'id' entry

    Returns
    -------
    plantdb.commons.fsdb.Fileset
        The ``Fileset`` instance from parsed JSON.
    """
    from plantdb.commons.fsdb import Fileset
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
    fileset : plantdb.commons.fsdb.Fileset
        The fileset instance to associate the returned ``File`` to.
    file_info : dict
        The file dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.File
        The ``File`` instance from parsed JSON.

    Raises
    ------
    FileNoIDError
        If the 'id' entry is missing from `file_info`.
    FileNoFileNameError
        If the 'file' entry is missing from `file_info`.
    FileNotFoundError
        If the file is not found on drive.
    """
    from plantdb.commons.fsdb import File
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
    file : plantdb.commons.fsdb.File
        A file to get "id" and "filename" from.

    Returns
    -------
    dict
        ``{"id": file.get_id(), "file": file.filename}``
    """
    return {"id": file.get_id(), "file": file.filename}


def _fileset_to_dict(fileset):
    """Returns a dict with the fileset "id" and the "files" dict.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        A fileset to get "id" and "files" dictionary from.

    Returns
    -------
    dict
        ``{"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}``

    See Also
    --------
    plantdb.commons.fsdb._file_to_dict
    """
    files = []
    for f in fileset.get_files():
        files.append(_file_to_dict(f))
    return {"id": fileset.get_id(), "files": files}


def _scan_to_dict(scan):
    """Returns a dict with the scan's filesets and files structure.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan instance to get underlying filesets and files structure from.

    Returns
    -------
    dict
        ``{"filesets": {"id": fileset.get_id(), "files": {"id": file.get_id(), "file": file.filename}}}``

    See Also
    --------
    plantdb.commons.fsdb._fileset_to_dict
    plantdb.commons.fsdb._file_to_dict
    """
    filesets = []
    for fileset in scan.get_filesets():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}
