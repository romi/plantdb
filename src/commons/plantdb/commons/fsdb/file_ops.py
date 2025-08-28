#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from shutil import rmtree

from tqdm import tqdm

from .exceptions import FileNoIDError
from .exceptions import FilesetNoIDError
from .exceptions import FilesetNotFoundError
from .metadata import _load_file_metadata
from .metadata import _load_fileset_metadata
from .metadata import _load_metadata
from .metadata import _load_scan_metadata
from .path_helpers import _file_metadata_path
from .path_helpers import _file_path
from .path_helpers import _fileset_metadata_json_path
from .path_helpers import _fileset_metadata_path
from .path_helpers import _fileset_path
from .path_helpers import _scan_json_file
from .path_helpers import _scan_measures_path
from .path_helpers import _scan_path
from .serialization import _parse_file
from .serialization import _parse_fileset
from .serialization import _scan_to_dict
from .validation import _is_safe_to_delete

from ..log import get_logger

logger = get_logger(__name__)

def _load_scan(db, scan_id):
    """Load ``Scan`` from given database.

    List subdirectories of ``db.basedir`` as ``Scan`` instances.
    May be restrited to the presense of subdirectories in .

    Parameters
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance to use to list the ``Scan``.
    scan_id : str
        The name of the scan to load.

    Returns
    -------
    list of plantdb.commons.fsdb.Scan
         The list of ``fsdb.Scan`` found in the database.

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    plantdb.commons.fsdb._scan_files_json
    plantdb.commons.fsdb._load_scan_filesets
    plantdb.commons.fsdb._load_scan_metadata

    Examples
    --------
    >>> from plantdb.commons.fsdb import FSDB
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_scans
    >>> db = dummy_db()
    >>> db.connect()
    >>> db.create_scan("007")
    >>> db.create_scan("111")
    >>> scans = _load_scans(db)
    >>> print(scans)
    []
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scans = _load_scans(db)
    >>> print(scans)
    [<plantdb.commons.fsdb.Scan object at 0x7fa01220bd50>]
    """
    from plantdb.commons.fsdb import Scan
    required_fs = db.required_filesets
    req_files_json = db.required_files_json

    scan = Scan(db, scan_id)
    scan_path = _scan_path(scan)

    # If specific filesets are required, test if they exist as subdirectories:
    if required_fs is not None:
        req_subdir = all([scan_path.joinpath(subdir).is_dir() for subdir in required_fs])
    else:
        req_subdir = True

    # If the `files.json` associated to the scan is required, test if it exists:
    if req_files_json:
        req_file = _scan_json_file(scan).is_file()
    else:
        req_file = True

    if db.dummy and scan_path.is_dir():
        return scan
    elif scan_path.is_dir() and req_subdir and req_file:
        # Parse the fileset, metadata and measure if:
        #  - path to scan directory exists
        #  - required subdirectories exists, if any
        #  - required files exists, if any
        scan.filesets = _load_scan_filesets(scan)
        scan.metadata = _load_scan_metadata(scan)
        scan.measures = _load_scan_measures(scan)
    else:
        scan = None
    return scan


def _load_scans(db):
    """Load list of ``Scan`` from given database.

    List subdirectories of ``db.basedir`` as ``Scan`` instances.
    May be restrited to the presense of subdirectories in .

    Parameters
    ----------
    db : plantdb.commons.fsdb.FSDB
        The database instance to use to list the ``Scan``.

    Returns
    -------
    dict of plantdb.commons.fsdb.Scan
         The scan-id indexex dictionary of ``fsdb.Scan`` found in the database.

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    plantdb.commons.fsdb._scan_files_json
    plantdb.commons.fsdb._load_scan_filesets
    plantdb.commons.fsdb._load_scan_metadata

    Examples
    --------
    >>> from plantdb.commons.fsdb import FSDB
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_scans
    >>> db = dummy_db()
    >>> db.connect()
    >>> db.create_scan("007")
    >>> db.create_scan("111")
    >>> scans = _load_scans(db)
    >>> print(scans)
    []
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scans = _load_scans(db)
    >>> print(scans)
    [<plantdb.commons.fsdb.Scan object at 0x7fa01220bd50>]
    """
    # List all subdirectories of the database path:
    dir_names = db.path().iterdir()
    # Filter out non-directories:
    dir_names = [dir_name for dir_name in dir_names if dir_name.is_dir()]
    # Return empty list if no directory found:
    if len(dir_names) == 0:
        return {}

    # Loop through the directories and load them as scan if they meet the criteria
    scans = {}
    for dir_name in tqdm(dir_names, unit="scan"):
        scan_name = dir_name.name
        scan = _load_scan(db, scan_name)
        if scan is not None:
            scans[scan_name] = scan
    return scans


def _load_dummy_fileset(scan):
    """Create lightweight "dummy" filesets from a scan by populating only file paths.

    This function creates a more efficient representation of filesets by avoiding
    the creation of full File objects. Instead, it populates the filesets with
    direct file paths, which requires less processing and memory. This is useful
    for operations that only need to know which files exist without requiring
    their full metadata or content.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The scan object containing filesets to be loaded in dummy mode.
        Used to access the scan's directory path.

    Returns
    -------
    dict
        A dictionary mapping fileset IDs to their corresponding Fileset objects.
        Each Fileset's `files` attribute contains paths instead of File objects.

    Notes
    -----
    - This function modifies the standard behavior of Fileset objects by storing
      pathlib.Path objects in the `files` attribute instead of File objects.
    - The resulting filesets are not suitable for operations that require full
      File objects with their metadata.
    - The leading underscore indicates this is an internal function not meant
      for general use outside the module.

    Examples
    --------
    >>> from plantdb.commons.fsdb import Scan
    >>> scan = Scan(db, "my_scan_id")
    >>> filesets = _load_dummy_fileset(scan)
    >>> # Access files as paths, not as File objects
    >>> for fs_id, fileset in filesets.items():
    ...     print(f"Fileset {fs_id} contains {len(fileset.files)} files")
    ...     for file_path in fileset.files:
    ...         print(f"  - {file_path.name}")
    """
    from plantdb.commons.fsdb import Fileset
    filesets = {}  # Dictionary to store filesets indexed by their IDs

    # Iterate through directories in the scan path, each directory corresponds to a fileset ID
    for fs_id in scan.path().iterdir():
        # Create a Fileset object for the current ID
        fs = Fileset(scan, fs_id)

        # Directly populate the files attribute with paths instead of File objects
        # This creates a "dummy" fileset with minimal processing
        fs.files = list(fs.path().iterdir())

        # Store the fileset in dictionary using its ID as key
        filesets[fs_id] = fs

    return filesets


def _load_scan_filesets(scan):
    """Load the list of ``Fileset`` from given `scan` dataset and return them as a dict.

    Load the list of filesets using "filesets" top-level entry from ``files.json``.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The instance to use to get the list of ``Fileset``.

    Returns
    -------
    dict
        A dictionary where keys are `fsid` (id of the filesets) and values are
        the `Fileset` instances.

    See Also
    --------
    plantdb.commons.fsdb._scan_files_json
    plantdb.commons.fsdb._load_scan_filesets

    Notes
    -----
    May delete a fileset if unable to load it!

    Examples
    --------
    >>> from plantdb.commons.fsdb import FSDB
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_scan_filesets
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> filesets = _load_scan_filesets(scan)
    >>> print(filesets)
    {'fsid_001': <plantdb.commons.fsdb.Fileset object at 0x7fa0122232d0>}
    """
    filesets = {}
    # Get the path to the `files.json` associated to the `scan`:
    files_json = _scan_json_file(scan)
    # Load it:
    with files_json.open(mode="r") as f:
        structure = json.load(f)
    # Get the list of info (dict) about the filesets
    filesets_info = structure["filesets"]
    if isinstance(filesets_info, list):
        for n, fileset_info in enumerate(filesets_info):
            try:
                fileset = _load_fileset(scan, fileset_info)
                fsid = fileset_info.get("id")
                if fsid is None:
                    raise FilesetNoIDError(f"Missing 'id' for the {n}-th 'filesets' entry.")
                filesets[fsid] = fileset
            except FilesetNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'filesets' entry from '{scan.id}'.")
                logger.debug(f"Current `fileset_info`: {fileset_info}")
            except FilesetNotFoundError:
                fsid = fileset_info.get("id", None)
                logger.error(f"Fileset directory '{fsid}' not found for scan {scan.id}, skip it.")
    else:
        raise IOError(f"Could not find a list of filesets in '{files_json}'.")

    return filesets


def _load_fileset(scan, fileset_info):
    """Load a fileset and set its attributes.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The scan object to use to get the list of ``fsdb.Fileset``
    fileset_info: dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.Fileset
        A fileset with its ``files`` & ``metadata`` attributes restored.

    Examples
    --------
    >>> import json
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_fileset
    >>> from plantdb.commons.fsdb.file_ops import _scan_json_file
    >>> db = dummy_db(with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    >>> json_path = _scan_json_file(scan)
    >>> with json_path.open(mode="r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fs = _load_fileset(scan, filesets_info[0])
    >>> print(fs.id)
    fileset_001
    >>> print([f.id for f in files])
    ['dummy_image', 'test_image', 'test_json']
    """
    fileset = _parse_fileset(scan, fileset_info)
    fileset.files = _load_fileset_files(fileset, fileset_info)
    fileset.metadata = _load_fileset_metadata(fileset)
    return fileset


def _load_fileset_files(fileset, fileset_info):
    """Load the list of ``File`` from given `fileset`.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        The instance to use to get the list of ``File``.
    fileset_info : dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    list of plantdb.commons.fsdb.File
         The list of ``File`` found in the `fileset`.

    See Also
    --------
    plantdb.commons.fsdb._load_file

    Notes
    -----
    May delete a file if unable to load it!

    Examples
    --------
    >>> import json
    >>> from plantdb.commons.fsdb.serialization import _parse_fileset
    >>> from plantdb.commons.fsdb import FSDB
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _scan_json_file,  _load_fileset_files
    >>> db = dummy_db(with_fileset=True, with_file=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    >>> json_path = _scan_json_file(scan)
    >>> with json_path.open(mode="r") as f: structure = json.load(f)
    >>> filesets_info = structure["filesets"]
    >>> fileset = _parse_fileset(scan.db, scan, filesets_info[0])
    >>> files = _load_fileset_files(fileset, filesets_info[0])
    >>> print([f.id for f in files])
    ['dummy_image', 'test_image', 'test_json']
    """
    scan_id = fileset.scan.id
    files = {}
    files_info = fileset_info.get("files", None)
    if isinstance(files_info, list):
        for n, file_info in enumerate(files_info):
            try:
                file = _load_file(fileset, file_info)
                fid = file_info.get("id")
                if fid is None:
                    raise FileNotFoundError(f"Missing 'id' for the {n}-th 'files' entry.")
            except FileNoIDError:
                logger.error(f"Could not get an 'id' entry for the {n}-th 'files' entry from '{scan_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            except FileNotFoundError:
                fs_id = fileset.id
                fname = file_info.get("file")
                logger.error(f"Could not find file '{fname}' for '{scan_id}/{fs_id}'.")
                logger.debug(f"Current `file_info`: {file_info}")
            else:
                files[fid] = file
    else:
        raise IOError(f"Expected a list of files in `files.json` from dataset '{fileset.scan.id}'!")
    return files


def _load_file(fileset, file_info):
    """Get a `File` instance for given `fileset` using provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        The instance to associate the returned ``File`` to.
    file_info : dict
        Dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.File
        The `File` instance with metadata.

    See Also
    --------
    plantdb.commons.fsdb._parse_file
    plantdb.commons.fsdb._load_file_metadata
    """
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _load_measures(path):
    """Load a measure dictionary from a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to the file containing the measure to load.

    Returns
    -------
    dict
        The measure dictionary.

    Raises
    ------
    IOError
        If the data returned by ``json.load`` is not a dictionary.
    """
    return _load_metadata(path)


def _load_scan_measures(scan):
    """Load the measures for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The dataset to load the measures for.

    Returns
    -------
    dict
        The measures' dictionary.
    """
    return _load_measures(_scan_measures_path(scan))


def _delete_file(file):
    """Delete the given file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.File
        A file instance to delete.

    Raises
    ------
    IOError
        If the file path is outside the database.

    Notes
    -----
    We have to delete:
      - the JSON metadata file associated to the file.
      - the file

    See Also
    --------
    plantdb.commons.fsdb._file_path
    plantdb.commons.fsdb._is_safe_to_delete
    """
    if file.filename is None:
        # The filename attribute is defined when the file is written!
        logger.error(f"No 'filename' attribute defined for file id '{file.id}'.")
        logger.info("It means the file is not written on disk.")
        return

    file_path = _file_path(file)
    if not _is_safe_to_delete(file_path):
        logger.error(f"File {file.filename} is not in the current database.")
        logger.debug(f"File path: '{file_path}'")
        raise IOError("Cannot delete files or directories outside of a local DB.")

    # - Delete the JSON metadata file associated to the `File` instance:
    file_md_path = _file_metadata_path(file)
    if file_md_path.is_file():
        try:
            file_md_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(
                f"Could not delete the JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"JSON metadata file path: '{file_md_path}'.")
        else:
            logger.info(
                f"Deleted JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    # - Delete the file associated to the `File` instance:
    if file_path.is_file():
        try:
            file_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(f"Could not delete file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"File path: '{file_path}'.")
        else:
            logger.info(f"Deleted file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    return


def _delete_fileset(fileset):
    """Delete the given fileset.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        A fileset instance to delete.

    Raises
    ------
    IOError
        If the fileset path is outside the database.

    Notes
    -----
    We have to delete:
      - the files in the fileset
      - the fileset JSON metadata file
      - the fileset metadata directory
      - the fileset directory

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    plantdb.commons.fsdb._fileset_path
    plantdb.commons.fsdb._is_safe_to_delete
    """
    fileset_path = _fileset_path(fileset)
    if not _is_safe_to_delete(fileset_path):
        logger.error(f"Fileset {fileset.id} is not in the current database.")
        logger.debug(f"Fileset path: '{fileset_path}'.")
        raise IOError("Cannot delete files or directories outside of a local DB.")

    # - Delete the `Files` (and their metadata) belonging to the `Fileset` instance:
    files_list = fileset.list_files()
    for f_id in files_list:
        fileset.delete_file(f_id)

    # - Delete the JSON metadata file associated to the `Fileset` instance:
    json_md = _fileset_metadata_json_path(fileset)
    try:
        json_md.unlink(missing_ok=False)
    except FileNotFoundError:
        logger.warning(f"Could not find the JSON metadata file for fileset '{fileset.id}'.")
        logger.debug(f"JSON metadata file path: '{json_md}'.")
    else:
        logger.info(f"Deleted the JSON metadata file for fileset '{fileset.id}'.")

    # - Delete the metadata directory associated to the `Fileset` instance:
    dir_md = _fileset_metadata_path(fileset)
    try:
        rmtree(dir_md, ignore_errors=True)
    except:
        logger.warning(f"Could not find metadata directory for fileset '{fileset.id}'.")
        logger.debug(f"Metadata directory path: '{dir_md}'.")
    else:
        logger.info(f"Deleted metadata directory for fileset '{fileset.id}'.")

    # - Delete the directory associated to the `Fileset` instance:
    try:
        rmtree(fileset_path, ignore_errors=True)
    except:
        logger.warning(f"Could not find directory for fileset '{fileset.id}'.")
        logger.debug(f"Fileset directory path: '{fileset_path}'.")
    else:
        logger.info(f"Deleted directory for fileset '{fileset.id}'.")
    return


def _delete_scan(scan):
    """Delete the given scan, starting by its `Fileset`s.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan instance to delete.

    Raises
    ------
    IOError
        If the scan path is outside the database.

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    plantdb.commons.fsdb._is_safe_to_delete
    """
    scan_path = _scan_path(scan)
    if not _is_safe_to_delete(scan_path):
        raise IOError("Cannot delete files outside of a DB.")

    # - Delete the whole directory will get rid of everything (metadata, filesets, files):
    try:
        rmtree(scan_path, ignore_errors=True)
    except:
        logger.warning(f"Could not find directory for scan '{scan.id}'.")
        logger.debug(f"Scan path: '{scan_path}'.")
    else:
        logger.info(f"Deleted directory for scan '{scan.id}'.")

    return


def _make_fileset(fileset):
    """Create the fileset directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        The fileset to use for directory creation.

    See Also
    --------
    plantdb.commons.fsdb._fileset_path
    """
    path = _fileset_path(fileset)
    # Create the fileset directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return


def _make_scan(scan):
    """Create the scan directory.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The scan to use for directory creation.

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    """
    path = _scan_path(scan)
    # Create the scan directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return


def _store_scan(scan):
    """Dump the fileset and files structure associated to a `scan` on drive.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        A scan instance to save by dumping its underlying structure in its "files.json".

    See Also
    --------
    plantdb.commons.fsdb._scan_to_dict
    plantdb.commons.fsdb._scan_files_json
    """
    structure = _scan_to_dict(scan)
    files_json = _scan_json_file(scan)
    with files_json.open(mode="w") as f:
        json.dump(structure, f, sort_keys=True, indent=4, separators=(',', ': '))
    logger.debug(f"The `files.json` file for scan '{scan.id}' has been updated!")
    return
