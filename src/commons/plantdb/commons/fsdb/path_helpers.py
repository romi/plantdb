#!/usr/bin/env python
# -*- coding: utf-8 -*-

def _scan_path(scan):
    """Get the path to given scan.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the scan directory.
    """
    return (scan.db.basedir / scan.id).resolve()


def _scan_json_file(scan):
    """Get the path to scan's "files.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan to get the files JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "files.json" file.
    """
    return _scan_path(scan) / "files.json"


def _scan_metadata_path(scan):
    """Get the path to scan's "metadata.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan to get the metadata JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "metadata.json" file.
    """
    return _scan_path(scan) / "metadata" / "metadata.json"


def _scan_measures_path(scan):
    """Get the path to scan's "measures.json" file.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan to get the measures JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the scan's "measures.json" file.
    """
    return _scan_path(scan) / "measures.json"


def _fileset_path(fileset):
    """Get the path to given fileset directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        A fileset to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset directory.
    """
    return _scan_path(fileset.scan) / fileset.id


def _fileset_metadata_path(fileset):
    """Get the path to given fileset metadata directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        A fileset to get the metadata directory path from.

    Returns
    -------
    pathlib.Path
        The path to the fileset metadata directory.
    """
    return _scan_path(fileset.scan) / "metadata" / fileset.id


def _fileset_metadata_json_path(fileset):
    """Get the path to `fileset.id` metadata JSON file.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        A fileset to get the JSON file path from.

    Returns
    -------
    pathlib.Path
        The path to the f`fileset.id` metadata JSON file.
    """
    return _scan_path(fileset.scan) / "metadata" / f"{fileset.id}.json"


def _file_path(file):
    """Get the path to given file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.File
        A file to get the path from.

    Returns
    -------
    pathlib.Path
        The path to the file.
    """
    return _fileset_path(file.fileset) / file.filename


def _file_metadata_path(file):
    """Get the path to `file.id` metadata JSON file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.File
        A file to get the metadata JSON path from.

    Returns
    -------
    pathlib.Path
        The path to the "<File.id>.json" file.
    """
    return _scan_path(file.fileset.scan) / "metadata" / file.fileset.id / f"{file.id}.json"


def _get_filename(file, ext):
    """Returns a `file` name using its ``id`` attribute and given extension.

    Parameters
    ----------
    file : plantdb.commons.fsdb.File
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
