#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

from ..log import get_logger

logger = get_logger(__name__)


def _is_valid_id(name):
    """Checks the validity of a given identifier name based on specified conditions.

    This function validates whether the provided name is a valid identifier by ensuring it
    is a string, non-empty, not longer than 255 characters, and contains only allowable
    characters. It logs errors for invalid input.

    Parameters
    ----------
    name : str
        The identifier name to be validated.

    Returns
    -------
    bool
        ``True`` if the name is valid, otherwise ``False``.
    """
    import re
    # Check if the name is a string
    if not isinstance(name, str):
        logger.error(f"Given name is not a string: '{name}'")
        return False

    # Check if the string is empty or too long (e.g., limit to 255 characters)
    if not name or len(name) > 255:
        logger.error(f"Given name is empty or too long: '{name}'")
        return False

    # Check for invalid characters (disallow slashes, backslashes, etc.)
    # Here we use a regex to allow alphanumeric, underscores, dashes and dots only
    if not re.match(r'^[\w\-\.]+$', name):
        logger.error(f"Given name contains invalid characters: '{name}'.")
        logger.info("Only alphanumeric characters, underscores, dashes and dots are allowed.")
        return False

    return True


def _is_fsdb(path):
    """Test if the given path is indeed an FSDB database.

    Do it by checking the presence of the ``MARKER_FILE_NAME``.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test as a valid FSDB database.

    Returns
    -------
    bool
        ``True`` if an FSDB database, else ``False``.
    """
    from .core import MARKER_FILE_NAME
    path = Path(path)
    marker_path = path / MARKER_FILE_NAME
    return marker_path.is_file()


def _is_safe_to_delete(path):
    """Tests if given path is safe to delete.

    Parameters
    ----------
    path : str or pathlib.Path
        A path to test for safe deletion.

    Returns
    -------
    bool
        ``True`` if the path is safe to delete, else ``False``.

    Notes
    -----
    A path is safe to delete only if it's a sub-folder of a db.
    """
    path = Path(path).resolve()
    while True:
        # Test if the current path is a local DB (FSDB):
        if _is_fsdb(path):
            return True  # exit and return `True` if it is
        # Else, move to the parent directory & try again:
        newpath = path.parent
        # Check if we have indeed moved up to the parent directory
        if newpath == path:
            # Stop if we did not
            return False
        path = newpath
