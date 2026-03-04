#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Metadata Management Module

This module provides a robust interface for managing metadata in JSON format, offering essential functionality for loading, storing, and manipulating metadata for files, filesets, and scans within a plant database system.

## Key Features

- **Metadata Loading Operations**
  - Load metadata from JSON files for files, filesets, and scan datasets
  - Handle JSON decode errors gracefully with appropriate error logging
  - Support both string and Path objects for file paths
  - Maintain backward compatibility with legacy metadata storage patterns

- **Metadata Storage Operations**
  - Save metadata to JSON files with consistent formatting
  - Automatic creation of necessary directory structures
  - Support for hierarchical metadata organization

- **Metadata Manipulation**
  - Deep copy functionality to prevent unintended modifications
  - Flexible key-value pair management
  - Support for both single value updates and bulk dictionary updates

## Usage Examples

```python
# Loading metadata from a file
metadata = _load_metadata("path/to/metadata.json")

# Setting metadata values
metadata_dict = {}
_set_metadata(metadata_dict, "key", "value")
# Or update with a dictionary
_set_metadata(metadata_dict, {"key1": "value1", "key2": "value2"}, None)

# Getting metadata with a default value
value = _get_metadata(metadata_dict, "key", default={})
```
"""

import copy
import json
from pathlib import Path
from typing import Any
from typing import Mapping

from .path_helpers import _file_metadata_path
from .path_helpers import _fileset_metadata_json_path
from .path_helpers import _scan_metadata_path
from ..log import get_logger

logger = get_logger(__name__)


def _load_fileset_metadata(fileset) -> dict:
    """Load the metadata for a fileset.

    Parameters
    ----------
    fileset : plantdb.commons.fsdb.core.Fileset
        The fileset to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_fileset_metadata_json_path(fileset))


def _load_metadata(path) -> dict:
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


def _load_scan_metadata(scan) -> dict:
    """Load the metadata for a scan dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
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


def _load_file_metadata(file) -> dict:
    """Load the metadata for a file.

    Parameters
    ----------
    file : plantdb.commons.fsdb.core.File
        The file to load the metadata for.

    Returns
    -------
    dict
        The metadata dictionary.
    """
    return _load_metadata(_file_metadata_path(file))


def _mkdir_metadata(path) -> None:
    """Create the parent directories from a given path.

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


def _store_metadata(path, metadata) -> None:
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


def _store_scan_metadata(scan) -> None:
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Scan
        The dataset to save the metadata for.
    """
    _store_metadata(_scan_metadata_path(scan), scan.metadata)
    return


def _store_fileset_metadata(fileset) -> None:
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.Fileset
        The fileset to save the metadata for.
    """
    _store_metadata(_fileset_metadata_json_path(fileset), fileset.metadata)
    return


def _store_file_metadata(file) -> None:
    """Save the metadata for a dataset.

    Parameters
    ----------
    scan : plantdb.commons.fsdb.core.File
        The file to save the metadata for.
    """
    _store_metadata(_file_metadata_path(file), file.metadata)
    return


def _get_metadata(metadata: Mapping[str, Any] = None, key: str = None, default: Any = None, ) -> Any:
    """Return a copy of ``metadata[key]`` or of the whole ``metadata`` mapping when ``key`` is omitted.

    Parameters
    ----------
    metadata : Mapping[str, Any] | None, optional
        Source dictionary. If ``None`` the ``default`` value is returned.
    key : str | None, optional
        Specific key to look up. When omitted, a copy of the entire mapping is returned.
    default : Any, optional
        Value to return when ``metadata`` is ``None`` or the ``key`` is absent.
        ``None`` is used as the sentinel; a new empty ``dict`` will be created
        on‑the‑fly if you need an empty mapping.

    Returns
    -------
    Any
        A deep‑copy of the requested value.
    """
    if metadata is None:
        # Ensure the caller can’t mutate a shared default object.
        return copy.deepcopy(default) if default is not None else {}
    if key is None:
        return copy.deepcopy(metadata)
    # ``dict.get`` already returns ``default`` when the key is missing.
    return copy.deepcopy(metadata.get(key, default))


def _set_metadata(metadata: dict[str, Any], data: str | Mapping[str, Any], value: Any = None) -> None:
    """Set a `data` `value` in the `metadata` dictionary.

    Parameters
    ----------
    metadata : dict[str, Any]
        The metadata dictionary to get the key from.
    data : str | Mapping[str, Any]
        If a string, a key to address the `value`.
        If a dictionary, update the metadata dictionary with `data` (`value` is then unused).
    value : Any | None, optional
        The value to assign to `data` if the latest is not a dictionary.

    Raises
    ------
    TypeError
        If `data` is neither a ``str`` nor a ``Mapping``.

    Examples
    --------
    >>> from plantdb.commons.fsdb.metadata import _set_metadata
    >>> md = {'description': 'This is the original description'}
    >>> _set_metadata(md, 'description', 'Updated fileset description')
    >>> print(md)
    {'description': 'Updated fileset description'}
    >>> _set_metadata(md, {'object': {'family': 'UFO', 'size': 'big'}})
    >>> print(md)
    {'description': 'Updated fileset description', 'object': {'family': 'UFO', 'size': 'big'}}
    >>> _set_metadata(md, {'object': {'size': 'smaller'}})
    >>> print(md)
    {'description': 'Updated fileset description', 'object': {'size': 'small'}}
    """
    if isinstance(data, str):
        if value is None:
            logger.warning(f"Metadata key '{data}' was set to `None`!")
        # Shallow‑copy immutable objects, deepcopy mutable containers.
        if isinstance(value, (dict, list, set)):
            metadata[data] = copy.deepcopy(value)
        else:
            metadata[data] = value
    elif isinstance(data, dict):
        for key, subvalue in data.items():
            _set_metadata(metadata, key, subvalue)
    else:
        raise TypeError(f"Invalid key type: {type(data).__name__}")
    return
