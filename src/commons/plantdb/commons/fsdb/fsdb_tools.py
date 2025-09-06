#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# FSDB Tools

A utility module for managing metadata in a plant database system, providing functionality to enhance and manipulate scan datasets with additional metadata information.

## Key Features

- Metadata Management
  - Add or update metadata for scan datasets
  - Safe handling of non-existent scan IDs
  - Preserves existing metadata while adding new key-value pairs
- Database Integration
  - Seamless integration with FSDB (File System Database)
  - Support for scan dataset manipulation
  - Error handling for database operations

## Usage Examples

```python
from plantdb.commons.fsdb import dummy_db
from plantdb.commons.fsdb.fsdb_tools import add_metadata_to_scan

# Create a dummy database with a scan
db = dummy_db(with_scan=True)

# Add metadata to an existing scan
scan_id = "myscan_001"
new_metadata = {"Name": "Example Scan", "Date": "2025-09-06"}
updated_scan = add_metadata_to_scan(db, scan_id, new_metadata)

# Clean up
db.disconnect()
```
"""

from plantdb.commons.log import get_logger

logger = get_logger(__name__)


def add_metadata_to_scan(db, scan_id, metadata):
    """Add metadata to a scan dataset.

    Parameters
    ----------
    db : plantdb.commons.fsdb.FSDB
        A local database instance hosting the ``Scan`` instance.
    scan_id : str
        The identifier of the ``Scan`` instance in the local database.
    metadata : dict
        The metadata dictionary to assign to the ``Scan`` instance.

    Returns
    -------
    plantdb.commons.fsdb.Scan
        The updated ``Scan`` instance with the new metadata.

    Examples
    --------
    >>> from plantdb.commons.fsdb import dummy_db
    >>> from plantdb.commons.fsdb.fsdb_tools import add_metadata_to_scan
    >>> db = dummy_db(with_scan=True)
    >>> scan = db.get_scan("myscan_001")
    >>> print(scan.metadata)
    {'test': 1}
    >>> scan = add_metadata_to_scan(db, "myscan_001", {"Name": "Example Scan"})
    >>> print(scan.metadata)
    {'test': 1, 'Name': 'Example Scan'}
    >>> db.disconnect()  # clean up (delete) the temporary dummy database
    """
    if db.scan_exists(scan_id) is False:
        logger.warning(
            f"Scan '{scan_id}' does not exist in the database. Cannot add metadata."
        )
        return None
    # Get or create the scan instance
    scan = db.get_scan(scan_id, create=False)
    # Add metadata to the scan
    scan.set_metadata(metadata)
    # Return the updated scan instance
    return scan
