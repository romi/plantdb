#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
from pathlib import Path

from .path_helpers import _file_metadata_path
from .path_helpers import _fileset_metadata_json_path
from .path_helpers import _scan_metadata_path
from ..log import get_logger

logger = get_logger(__name__)


def _load_fileset_metadata(fileset):
    """Load the metadata for a fileset.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.Fileset
        The fileset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_fileset_metadata_json_path(fileset))


def _load_metadata(path):
    """Load a metadata dictionary from a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to the file containing the metadata to load.

    Returns
    -------
    dict
        The metadata dictionary.

    Raises
    ------
    IOError
        If the data returned by ``json.load`` is not a dictionary.
    """
    from json import JSONDecodeError
    path = Path(path)
    md = {}
    if path.is_file():
        with path.open(mode="r") as f:
            try:
                md = json.load(f)
            except JSONDecodeError:
                logger.error(f"Could not load JSON file '{path}'")
        if not isinstance(md, dict):
            raise IOError(f"Could not obtain a dictionary from JSON: {path}")
    else:
        logger.debug(f"Could not find JSON file '{path}'")

    return md


def _load_scan_metadata(scan):
    """Load the metadata for a scan dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The dataset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    scan_md = {}
    md_path = _scan_metadata_path(scan)
    if md_path.exists():
        scan_md.update(_load_metadata(md_path))
    # FIXME: next lines are here to solve the issue that most scans dataset have no metadata as they have been saved in the 'images' fileset metadata...
    img_fs_path = md_path.parent / 'images.json'  # path to 'images' fileset metadata
    if img_fs_path.exists():
        img_fs_md = _load_metadata(img_fs_path)
        scan_md.update({'object': img_fs_md.get('object', {})})
        scan_md.update({'hardware': img_fs_md.get('hardware', {})})
        scan_md.update({'acquisition_date': img_fs_md.get('acquisition_date', None)})
    return scan_md


def _load_file_metadata(file):
    """Load the metadata for a file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.File
        The file to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_file_metadata_path(file))


def _mkdir_metadata(path):
    """Create the parent directories from given path.

    Parameters
    ----------
    path : str or pathlib.Path
        The path to a file.

    Notes
    -----
    Used to check the availability of the 'metadata' directory inside a dataset.
    """
    dir = Path(path).parent
    if not dir.is_dir():
        dir.mkdir(parents=True, exist_ok=True)
    return


def _store_metadata(path, metadata):
    """Save a metadata dictionary as a JSON file.

    Parameters
    ----------
    path : str or pathlib.Path
        JSON file path to use for saving the metadata.
    metadata : dict
        The metadata dictionary to save.
    """
    _mkdir_metadata(path)
    with path.open(mode="w") as f:
        json.dump(metadata, f, sort_keys=True, indent=4, separators=(',', ': '))
    return


def _store_scan_metadata(scan):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Scan
        The dataset to save the metadata for.
    """
    _store_metadata(_scan_metadata_path(scan), scan.metadata)
    return


def _store_fileset_metadata(fileset):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.Fileset
        The fileset to save the metadata for.
    """
    _store_metadata(_fileset_metadata_json_path(fileset), fileset.metadata)
    return


def _store_file_metadata(file):
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.File
        The file to save the metadata for.
    """
    _store_metadata(_file_metadata_path(file), file.metadata)
    return


def _get_metadata(metadata=None, key=None, default={}):
    """Get a copy of `metadata[key]`.

    Parameters
    ----------
    metadata : dict, optional
        The metadata dictionary to get the key from.
    key : str, optional
        The key to get from the metadata dictionary.
        By default, return a copy of the whole metadata dictionary.
    default : Any, optional
        The default value to return if the key do not exist in the metadata.
        Default is an empty dictionary ``{}``.

    Returns
    -------
    Any
        The value saved under `metadata['key']`, if any.
    """
    # Do a deepcopy of the value because we don't want the caller to inadvertently change the values.
    if metadata is None:
        return default
    elif key is None:
        return copy.deepcopy(metadata)
    else:
        return copy.deepcopy(metadata.get(str(key), default))


def _set_metadata(metadata, data, value):
    """Set a `data` `value` in `metadata` dictionary.

    Parameters
    ----------
    metadata : dict
        The metadata dictionary to get the key from.
    data : str or dict
        If a string, a key to address the `value`.
        If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
    value : any, optional
        The value to assign to `data` if the latest is not a dictionary.

    Raises
    ------
    IOError
        If `value` is `None` when `data` is a string.
        If `data` is not of the right type.
    """
    if isinstance(data, str):
        if value is None:
            raise IOError(f"No value given for key '{data}'!")
        # Do a deepcopy of the value because we don't want the caller to inadvertently change the values.
        metadata[data] = copy.deepcopy(value)
    elif isinstance(data, dict):
        for key, value in data.items():
            _set_metadata(metadata, key, value)
    else:
        raise IOError(f"Invalid key: {data}")
    return
