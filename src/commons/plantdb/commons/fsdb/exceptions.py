#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Custom FSDB Exceptions

A collection of specialized exception classes designed to handle various error scenarios in a file system database (FSDB) implementation. These exceptions provide clear, specific error handling for database operations, scan management, and fileset manipulations.

## Key Features
- **Database Validation**
  - `NotAnFSDBError` for invalid database instances
- **Scan Management** : exceptions for scan-related errors:
  - `ScanExistsError` for existing scan directories
  - `ScanNotFoundError` for missing scan directories
- **Fileset Operations**: exceptions for fileset-related errors:
  - `FilesetExistsError` for existing fileset directories
  - `FilesetNotFoundError` for missing fileset directories
  - `FilesetNoIDError` for missing fileset identifiers
- **File Handling**: exceptions for file-related issues:
  - `FileExistsError` for existing file
  - `FileNotFoundError` for missing file
  - `FileNoIDError` for missing file identifiers
  - `FileNoFileNameError` for missing file names

## Usage Examples
```python
# Example of handling scan-related errors
try:
    scan = db.get_scan("non_existent_scan")
except ScanNotFoundError as e:
    print(f"Error: {e}")

# Example of handling fileset errors
try:
    fileset = scan.get_fileset("invalid_fileset")
except FilesetNotFoundError as e:
    print(f"Error: {e}")
```
"""

class NotAnFSDBError(Exception):
    def __init__(self, message):
        self.message = message


class ScanNotFoundError(Exception):
    """Could not find the scan directory."""
    def __init__(self, db, scan_id: str):
        super().__init__(f"Unknown scan id '{scan_id}' in database '{db.path()}'!")


class ScanExistsError(Exception):
    """Could not find the scan directory."""
    def __init__(self, db, scan_id: str):
        super().__init__(f"Scan id '{scan_id}' already exists in database '{db.path()}'!")


class FilesetNotFoundError(Exception):
    """Could not find the fileset directory."""
    def __init__(self, scan, fs_id: str):
        super().__init__(f"Unknown fileset id '{fs_id}' in scan '{scan.id}'!")


class FilesetExistsError(Exception):
    """The fileset directory already exists."""
    def __init__(self, scan, fs_id: str):
        super().__init__(f"Fileset id '{fs_id}' already exists in scan '{scan.id}'!")


class FileNotFoundError(Exception):
    """Could not find the file."""
    def __init__(self, fs, f_id: str):
        super().__init__(f"Unknown file id '{f_id}' in scan/fileset '{fs.scan.id}/{fs.id}'!")


class FileExistsError(Exception):
    """The file already exists."""
    def __init__(self, fs, f_id: str):
        super().__init__(f"File id '{f_id}' already exists in scan/fileset '{fs.scan.id}/{fs.id}'!")


class FilesetNoIDError(Exception):
    """No 'id' entry could be found for this fileset."""


class FileNoIDError(Exception):
    """No 'id' entry could be found for this file."""


class FileNoFileNameError(Exception):
    """No 'file' entry could be found for this file."""
