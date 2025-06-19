#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Add a constant at the top of the file with the other constants
import os

def api_prefix():
    """Set the API prefix for all URL generation functions.

    Parameters
    ----------
    prefix : str, optional
        The prefix to add to all API URLs, e.g., '/plantdb'. Defaults to empty string.

    Examples
    --------
    >>> import os
    >>> from plantdb.commons import api_prefix
    >>> api_prefix()
    ''
    >>> os.environ['PLANTDB_API_PREFIX'] = "/plantdb"
    >>> api_prefix()
    '/plantdb'
    """
    prefix = os.environ.get("PLANTDB_API_PREFIX", "")  # Default to no prefix
    return prefix.rstrip('/')  # Remove trailing slash if present