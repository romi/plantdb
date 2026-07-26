#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# File Operations Module

A comprehensive module for managing file system operations in a hierarchical database structure, providing functionality for loading, storing, and manipulating scans, filesets, and individual files in a structured database environment.

## Key Features

- **Scan Management**: Load and manipulate scan data structures with associated metadata
- **Fileset Handling**: Create, load, and delete filesets with their corresponding files and metadata
- **File Operations**: Manage individual files within filesets, including metadata handling
- **Directory Structure Management**: Create and maintain hierarchical directory structures for scans and filesets
- **Metadata Management**: Load and store metadata for scans, filesets, and individual files
- **Error Handling**: Comprehensive error checking and logging for file operations
- **Data Serialization**: JSON-based serialization for storing and loading data structures

## Usage Examples

```python
>>> from plantdb.commons.fsdb.file_ops import _load_scans,_make_scan, _store_scan, _delete_scan
>>> from plantdb.commons.test_database import test_database
>>> # Initialize the database (creates base directory if needed)
>>> db = test_database(no_auth=True)
>>> db.connect()
>>> # Load all scans
>>> scans = _load_scans(db)
>>> # Create and store a new scan
>>> scan = db.create_scan("scan_001")
>>> _make_scan(scan)
PosixPath('/tmp/ROMI_DB_sgqmij8t/scan_001')
>>> _store_scan(scan)
>>> # Delete a scan
>>> _delete_scan(scan)
```
"""

import json
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

from send2trash import send2trash
from tqdm import tqdm

from .exceptions import FileNotFoundError
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
from .validation import _is_scan_dataset
from .validation import _is_valid_fileset
from ..log import get_logger
from ..utils import backup_file

# ----------------------------------------------------------------------
# NOTE: The following imports are only needed for type‑checking / IDE hints.
# Importing them at runtime creates a circular dependency with `core.py`.
# By guarding them with `TYPE_CHECKING` we keep static‑type information
# without executing the import when the module is loaded.
# ----------------------------------------------------------------------
if TYPE_CHECKING:
    from .core import FSDB
    from .core import Scan
    from .core import Fileset
    from .core import File
# ----------------------------------------------------------------------

logger = get_logger(__name__)


def _load_scans(db: 'FSDB', updates_files_json: bool = False) -> dict[str, 'Scan']:
    """Load all scans from a PlantDB filesystem database.

    This internal helper iterates over the sub‑directories of the database path and attempts to instantiate
    a `Scan` for each directory that follows the expected naming convention.
    Directories that cannot be loaded are skipped and reported via the logger.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The PlantDB filesystem database instance from which scans should be loaded.
    updates_files_json : bool
        A boolean flag indicating whether to update the ``files.json`` when entries are not found on drive.

    Returns
    -------
    dict[str, plantdb.commons.fsdb.core.Scan]
        Dictionary mapping each successfully loaded scan name to its corresponding `Scan` instance.
        If no scan directories are present, an empty dictionary is returned.

    Raises
    ------
    OSError
        If the database path cannot be accessed (_e.g._, due to permission issues or the path not existing).

    Notes
    -----
    * Hidden directories (names starting with ``'.'``) are ignored.
    * Scans that fail to load are collected in ``bad_scans`` and reported at *INFO* level via the logger.
    * The function returns an empty dictionary rather than ``None`` when no
      scans are found, which simplifies downstream handling.

    See Also
    --------
    _load_scan : Loads a single scan directory into a ``Scan`` object.

    Examples
    --------
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> from plantdb.commons.test_database import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_scans
    >>> db = dummy_db()
    >>> db.create_scan("007")
    >>> db.create_scan("111")
    >>> scans = _load_scans(db)
    >>> print(scans)
    []
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scans = _load_scans(db)
    >>> print(scans)
    [<plantdb.commons.fsdb.core.Scan object at 0x7fa01220bd50>]
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
    bad_scans = set()
    for dir_name in tqdm(dir_names, unit="scan"):
        scan_name = dir_name.name  # get the scan name from the directory
        if scan_name.startswith('.'):
            continue  # ignore dot-folders (hidden)
        # Try to load each scan directory as a `Scan` instance with:
        scan = _load_scan(db, scan_name, updates_files_json)
        # If the scan could be loaded, add it to the dictionary of scans
        if scan is not None:
            scans[scan_name] = scan
        else:
            bad_scans.add(scan_name)

    if bad_scans:
        n_bad = len(bad_scans)
        logger.info(f"Found {n_bad} bad scans: {', '.join(bad_scans)}")

    return scans


def _load_scan(db: 'FSDB', scan_id: str, updates_files_json: bool = False) -> 'Scan | None':
    """Load a single scan from the filesystem database.

    This internal helper retrieves the scan identified by ``scan_id`` from the
    ``db`` instance, attempts to populate its filesets, metadata and manual
    measures, and returns the fully populated ``Scan`` object.
    If the scan directory does not exist, ``None`` is returned.

    Parameters
    ----------
    db : plantdb.commons.fsdb.core.FSDB
        The filesystem database instance from which the scan should be loaded.
    scan_id : str
        Identifier of the scan to load.
        Must correspond to a directory name inside the database's scan root.
    updates_files_json : bool
        A boolean flag indicating whether to update the ``files.json`` when entries are not found on drive.

    Returns
    -------
    plantdb.commons.fsdb.core.Scan | None
        The loaded ``Scan`` object if the scan directory exists; otherwise ``None``.

    See Also
    --------
    _load_scans : Load all scans from a database.
    _scan_path : Compute the filesystem path of a given scan.
    _load_scan_filesets : Load filesets belonging to a scan.
    _load_scan_metadata : Load metadata associated with a scan.
    _load_scan_measures : Load manual measures for a scan.

    Examples
    --------
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> from plantdb.commons.test_database import dummy_db
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
    [<plantdb.commons.fsdb.core.Scan object at 0x7fa01220bd50>]
    """
    from plantdb.commons.fsdb.core import Scan

    scan = Scan(db, scan_id)  # initialize and empty Scan instance
    scan_path = _scan_path(scan)  # path the scan directory
    # If the directory exists, try to load the scan:
    if _is_scan_dataset(scan_path, validate_json_fileset=False):
        # Try to load the filesets and their files
        scan.filesets, needs_update = _load_scan_filesets(scan)
        if needs_update and updates_files_json:
            files_json = _scan_json_file(scan)
            backup_file(files_json)  # create a backup file
            _store_scan(scan)  # update the scan's ``files.json``
        # Try to load the scan's metadata
        scan.metadata = _load_scan_metadata(scan)
        # Try to load the scan's manual measure, if any
        scan.measures = _load_scan_measures(scan)
    else:
        scan = None
    return scan


def _load_dummy_fileset(scan: 'Scan') -> dict[str, 'Fileset']:
    """Create lightweight "dummy" filesets from a scan by populating only file paths.

    This function creates a more efficient representation of filesets by avoiding
    the creation of full File objects. Instead, it populates the filesets with
    direct file paths, which requires less processing and memory. This is useful
    for operations that only need to know which files exist without requiring
    their full metadata or content.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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
    >>> from plantdb.commons.fsdb.core import Scan
    >>> scan = Scan(db, "my_scan_id")
    >>> filesets = _load_dummy_fileset(scan)
    >>> # Access files as paths, not as File objects
    >>> for fs_id, fileset in filesets.items():
    ...     print(f"Fileset {fs_id} contains {len(fileset.files)} files")
    ...     for file_path in fileset.files:
    ...         print(f"  - {file_path.name}")
    """
    from plantdb.commons.fsdb.core import Fileset
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


def _load_scan_filesets(scan: 'Scan') -> tuple[dict[str, 'Fileset'] | None, bool]:
    """Load the ``Fileset`` mapping from given `scan` dataset and return them as a dict.

    Load the list of filesets using "filesets" top-level entry from ``files.json``.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The instance to use to get the list of ``Fileset``.

    Returns
    -------
    dict or None
        A dictionary where keys are `fsid` (id of the filesets) and values are the `Fileset` instances.
        May be ``None` if the filesets could not be loaded.
    bool
        A boolean indicating whether to update the scan's ``files.json``.

    See Also
    --------
    plantdb.commons.fsdb._scan_files_json
    plantdb.commons.fsdb._load_scan_filesets

    Notes
    -----
    May delete a fileset if unable to load it!

    Examples
    --------
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> from plantdb.commons.test_database import dummy_db
    >>> from plantdb.commons.fsdb.file_ops import _load_scan_filesets
    >>> db = dummy_db(with_fileset=True)
    >>> db.connect()
    >>> scan = db.get_scan("myscan_001")
    >>> filesets = _load_scan_filesets(scan)
    >>> print(filesets)
    {'fsid_001': <plantdb.commons.fsdb.core.Fileset object at 0x7fa0122232d0>}
    """
    filesets = {}
    # Get the path to the `files.json` associated with the `scan`:
    files_json = _scan_json_file(scan)
    # Load it:
    with files_json.open(mode="r") as f:
        structure = json.load(f)

    # Inform `_load_scan` to update the `files.json` associated with the `scan`
    needs_update = False

    # Get the list of info (dict) about the filesets
    filesets_info = structure["filesets"]
    if isinstance(filesets_info, list):
        for fileset_info in filesets_info:
            try:
                fileset, _needs_update = _load_fileset(scan, fileset_info)
            except Exception as e:
                logger.error(e)
                needs_update = True
            else:
                needs_update = needs_update or _needs_update
                filesets[fileset.id] = fileset
    else:
        logger.error(f"Could not load a list of filesets for scan '{scan.id}' from: '{files_json}'")
        return None, True

    return filesets, needs_update


def _load_fileset(scan: 'Scan', fileset_info: dict[str, str | list]) -> tuple['Fileset | None', bool]:
    """Load a fileset and set its attributes.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The scan object to use to get the list of ``fsdb.core.Fileset``
    fileset_info: dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.core.Fileset | None
        A fileset with its ``files`` & ``metadata`` attributes restored.
    bool
        A boolean indicating whether to update the scan's ``files.json``.

    Examples
    --------
    >>> import json
    >>> from plantdb.commons.test_database import dummy_db
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
    _is_valid_fileset(scan.path(), fileset_info['id'], fileset_info['files'])
    fileset = _parse_fileset(scan, fileset_info)
    fileset.files, needs_update = _load_fileset_files(fileset, fileset_info)
    fileset.metadata = _load_fileset_metadata(fileset)
    return fileset, needs_update


def _load_fileset_files(fileset: 'Fileset', fileset_info: dict[str, str | list]) -> tuple[dict[str, 'File'], bool]:
    """Load the list of ``File`` from given `fileset`.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        The instance to use to get the list of ``File``.
    fileset_info : dict
        Dictionary with the fileset id and listing its files, ``{'files': [], 'id': str}``.

    Returns
    -------
    dict
        The file ID indexed dictionary of ``File`` found in the `fileset`.
    bool
        A boolean indicating whether to update the scan's ``files.json``.

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
    >>> from plantdb.commons.fsdb.core import FSDB
    >>> from plantdb.commons.test_database import dummy_db
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
    files: dict[str, File] = {}
    files_info = fileset_info.get("files", None)

    # Inform `_load_fileset` to update the `files.json` associated with the `scan`
    needs_update = False

    if isinstance(files_info, list):
        for idx, file_info in enumerate(files_info):
            try:
                file = _load_file(fileset, file_info)
            except Exception as e:
                logger.error(e)
                needs_update = True
            else:
                files[file.id] = file
    else:
        raise IOError(f"Expected a list of files in `files.json` from dataset '{fileset.scan.id}'!")

    return files, needs_update


def _load_file(fileset: 'Fileset', file_info: dict[str, str]) -> 'File':
    """Get a ``File`` instance for given `fileset` using provided `file_info`.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        The instance to associate the returned ``File`` to.
    file_info : dict
        Dictionary with the file 'id' and 'file' entries, ``{'file': str, 'id': str}``.

    Returns
    -------
    plantdb.commons.fsdb.core.File
        The `File` instance with metadata.

    See Also
    --------
    plantdb.commons.fsdb._parse_file
    plantdb.commons.fsdb._load_file_metadata
    """
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _load_measures(path: str | Path) -> dict[str, Any]:
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


def _load_scan_measures(scan: 'Scan') -> dict[str, Any]:
    """Load the measures for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The dataset to load the measures for.

    Returns
    -------
    dict
        The measures' dictionary.
    """
    return _load_measures(_scan_measures_path(scan))


def _delete_file(file: 'File') -> None:
    """Delete the given file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        A file instance to delete.

    Raises
    ------
    IOError
        If the file path is outside the database.

    Notes
    -----
    We have to delete:
      - the JSON metadata file associated with the file.
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

    # - Delete the JSON metadata file associated with the `File` instance:
    file_md_path = _file_metadata_path(file)
    if file_md_path.is_file():
        try:
            file_md_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(
                f"Could not delete the JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"JSON metadata file path: '{file_md_path}'.")
        else:
            logger.debug(
                f"Deleted JSON metadata file for file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    # - Delete the file associated with the `File` instance:
    if file_path.is_file():
        try:
            file_path.unlink(missing_ok=False)
        except FileNotFoundError:
            logger.error(f"Could not delete file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")
            logger.debug(f"File path: '{file_path}'.")
        else:
            logger.debug(f"Deleted file '{file.id}' from '{file.fileset.scan.id}/{file.fileset.id}'.")

    return


def _delete_fileset(fileset: 'Fileset') -> None:
    """Delete the given fileset.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
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

    # - Delete the `Files` (and their metadata) belonging to the `Fileset` instance:
    files_list = fileset.list_files()
    for f_id in files_list:
        fileset.delete_file(f_id)

    # - Delete the JSON metadata file associated with the `Fileset` instance:
    json_md = _fileset_metadata_json_path(fileset)
    try:
        json_md.unlink(missing_ok=False)
    except FileNotFoundError:
        logger.warning(f"Could not find the JSON metadata file for fileset '{fileset.id}'.")
        logger.debug(f"JSON metadata file path: '{json_md}'.")
    else:
        logger.debug(f"Deleted the JSON metadata file for fileset '{fileset.id}'.")

    # - Delete the metadata directory associated with the `Fileset` instance:
    dir_md = _fileset_metadata_path(fileset)
    try:
        send2trash(dir_md)
    except:
        logger.warning(f"Could not find metadata directory for fileset '{fileset.id}'.")
        logger.debug(f"Metadata directory path: '{dir_md}'.")
    else:
        logger.debug(f"Deleted metadata directory for fileset '{fileset.id}'.")

    # - Delete the directory associated with the `Fileset` instance:
    try:
        send2trash(fileset_path)
    except:
        logger.warning(f"Could not find directory for fileset '{fileset.id}'.")
        logger.debug(f"Fileset directory path: '{fileset_path}'.")
    else:
        logger.debug(f"Deleted directory for fileset '{fileset.id}'.")
    return


def _delete_scan(scan: 'Scan') -> None:
    """Delete the given scan, starting by its `Fileset`s.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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
        send2trash(scan_path)
    except:
        logger.warning(f"Could not find directory for scan '{scan.id}'.")
        logger.debug(f"Scan path: '{scan_path}'.")
    else:
        logger.debug(f"Deleted directory for scan '{scan.id}'.")

    return


def _make_fileset(fileset: 'Fileset') -> Path:
    """Create the fileset directory.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        The fileset to use for directory creation.

    Returns
    -------
    pathlib.Path
        The created fileset directory.

    See Also
    --------
    plantdb.commons.fsdb._fileset_path
    """
    path = _fileset_path(fileset)
    # Create the fileset directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return path


def _make_scan(scan: 'Scan') -> Path:
    """Create the scan directory.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The scan to use for directory creation.

    Returns
    -------
    pathlib.Path
        The created scan directory.

    See Also
    --------
    plantdb.commons.fsdb._scan_path
    """
    path = _scan_path(scan)
    # Create the scan directory if it does not exist:
    if not path.is_dir():
        path.mkdir(parents=True)
    return path


def _store_scan(scan: 'Scan') -> None:
    """Dump the fileset and files structure associated with a `scan` on drive.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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
