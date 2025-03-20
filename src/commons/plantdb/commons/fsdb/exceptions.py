#!/usr/bin/env python
# -*- coding: utf-8 -*-

class NotAnFSDBError(Exception):
    def __init__(self, message):
        self.message = message


class ScanNotFoundError(Exception):
    """Could not find the scan directory."""
    def __init__(self, db, scan_id: str):
        super().__init__(f"Unknown scan id '{scan_id}' in database '{db.path()}'!")


class FilesetNotFoundError(Exception):
    """Could not find the fileset directory."""
    def __init__(self, scan, fs_id: str):
        super().__init__(f"Unknown fileset id '{fs_id}' in scan '{scan.id}'!")


class FilesetNoIDError(Exception):
    """No 'id' entry could be found for this fileset."""


class FileNoIDError(Exception):
    """No 'id' entry could be found for this file."""


class FileNoFileNameError(Exception):
    """No 'file' entry could be found for this file."""
