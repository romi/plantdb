#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from zipfile import ZipFile

from plantdb.commons.fsdb.exceptions import FileNotFoundError
from plantdb.commons.fsdb.exceptions import FilesetNotFoundError
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import ScanNotFoundError
from plantdb.server.core.utils import compute_fileset_matches


def resource_file(db, scan_id, task_name, **kwargs):
    """Retrieve a specific ``File`` object from the database.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The database instance.
    scan_id : str
        Identifier of the scan containing the requested file.
    task_name : str
        Name of the task (fileset) and file to fetch.
    **kwargs
        Additional arguments passed to ``FSDB.get_scan`` (e.g. JWT token).

    Returns
    -------
    tuple
        Either ``(file, 200)`` on success or ``(error_dict, status_code)`` on failure.
        The error dictionary always uses the key ``"error"`` for consistency.
    """

    # Get the corresponding `Scan` instance
    try:
        scan = db.get_scan(scan_id, **kwargs)

    except NoAuthUserError as e:
        return {'error': str(e)}, 401  # HTTP 401 Unauthorized (authentication)
    except ScanNotFoundError:
        return {"error": f"Scan '{scan_id}' not found!"}, 400

    task_fs_map = compute_fileset_matches(scan)
    # Get the corresponding `Fileset` instance
    try:
        fs = scan.get_fileset(task_fs_map[task_name])
    except KeyError:
        return {"error": f"No fileset mapped for task '{task_name}'."}, 404
    except FilesetNotFoundError:
        return {"error": f"Fileset for task '{task_name}' not found."}, 404

    # Get the `File` corresponding to the resource
    try:
        file = fs.get_file(task_name)
    except FileNotFoundError:
        return {"error": f"File '{fs.id}/{task_name}' not found."}, 404
    except Exception as exc:                     # Unexpected internal error
        # Use JSON‑serializable payload; Flask will handle conversion.
        return {"error": f"Internal server error: {str(exc)}"}, 500

    # Success – return the File object (Flask‑RESTful resources expect the
    # object itself; the caller can decide the HTTP status if required).
    return file


def is_within_directory(directory, target):
    """Check if a target path is within a directory.

    This function determines if the absolute path of the target is located
    within the absolute path of the directory. It uses `os.path.commonpath`
    to perform the comparison.

    Parameters
    ----------
    directory : str or pathlib.Path
        The path to the directory to check against.
    target : str or pathlib.Path
        The path to the target to check if it resides within the directory.

    Returns
    -------
    bool
        ``True`` if the target path is within the directory, ``False`` otherwise.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    return os.path.commonpath([abs_directory]) == os.path.commonpath([abs_directory, abs_target])


def is_directory_in_archive(archive_path, target_dir):
    """Check if a specific directory exists within an archive file.

    This function checks whether a given directory is present at the top level of a ZIP archive.

    Parameters
    ----------
    archive_path : str or pathlib.Path
        The path to the ZIP archive file.
    target_dir : str
        The name of the target directory to check for within the archive.

    Returns
    -------
    bool
        True if the target directory exists at the top level of the archive, False otherwise.
    """
    with ZipFile(archive_path, 'r') as zip_ref:
        # List all members in the zip file
        top_level_members = [name for name in zip_ref.namelist() if '/' not in name]
        # Check if the target directory is among them
        return f"{target_dir}/" in top_level_members or target_dir in top_level_members
