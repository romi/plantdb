#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# File System Database (FSDB) Module

A robust file system-based database implementation that provides a structured way to store and manage plant phenotyping data with hierarchical organization.
This module serves as the core storage engine for the ROMI plant database ecosystem, offering efficient file operations, metadata management, and data validation capabilities.

## Key Features

- **Hierarchical Data Organization**: Implements a three-level hierarchy (`Scan` → `Fileset` → `File`) for structured data storage
- **Metadata Management**: Comprehensive handling of metadata at all hierarchy levels
- **File Operations**:
  - Secure file import and export
  - Raw data reading and writing
  - File validation and integrity checks
- **Error Handling**: Custom exceptions for database-specific error conditions
- **Path Management**: Utilities for handling file system paths and directory structures
- **Data Serialization**: Tools for consistent data serialization and deserialization
- **Validation Framework**: Built-in validation mechanisms for data integrity

## Usage Examples

```python
from plantdb.commons.fsdb import DB

# Initialize and connect to database
db = DB("/path/to/database")
db.connect()

# Create a new scan
scan = db.create_scan("experiment_001")

# Add a fileset to the scan
fileset = scan.create_fileset("raw_images")

# Add metadata to the fileset
fileset.set_metadata({
    "capture_date": "2025-09-06",
    "device": "camera_01",
    "resolution": "4K"
})

# Create a file in the fileset
image_file = fileset.create_file("image_001.jpg")

# Import actual file data
image_file.import_file("/path/to/source/image.jpg")

# Clean up
db.disconnect()
```
"""

from .core import FSDB
from .core import Scan
from .core import Fileset
from .core import File
from .core import dummy_db
from .exceptions import (
    NotAnFSDBError,
    ScanNotFoundError,
    FilesetNotFoundError,
    FilesetNoIDError,
    FileNoIDError,
    FileNoFileNameError
)

__all__ = [
    'FSDB',
    'Scan',
    'Fileset',
    'File',
    'dummy_db',
    'NotAnFSDBError',
    'ScanNotFoundError',
    'FilesetNotFoundError',
    'FilesetNoIDError',
    'FileNoIDError',
    'FileNoFileNameError',
]
