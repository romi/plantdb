#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 
# plantdb - Data handling tools for the ROMI project
# 
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
# 
# This file is part of plantdb.
# 
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
# 
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

"""
plantdb.db
===========

API for the database module in the ROMI project.

A database ``DB`` contains a list of scans ``Scan`` distinguishable by their id.
A ``Scan`` can be made of several list of files ``Fileset``.
A ``Fileset`` is made of a list of files ``Files``.
A ``File`` can be an image, text of bytes.

"""

class DB(object):
    """Class defining the database object `DB`.

    Abstract class defining the API used to communicate with a database in the
    ROMI project.
    """

    def __init__(self):
        pass

    def connect(self, login_data=None):
        """Connect to the database.

        Parameters
        ----------
        login_data : list or dict, optional
            Use this to access to a `DB` with credentials.

        """
        raise NotImplementedError

    def disconnect(self):
        """Disconnect from the database.

        """
        raise NotImplementedError

    def get_scans(self):
        """Get the list of scans saved in the database.

        """
        raise NotImplementedError

    def get_scan(self, id, create=False):
        """Get a scan saved in the database.

        Parameters
        ----------
        id : str
            Id of the scan to retrieve
        create :  bool
            Create the scan if it does not exist (default : False)

        Returns
        -------
        db.Scan

        """
        raise NotImplementedError

    def create_scan(self, id):
        """Create a new scan object in the database.

        Parameters
        ----------
        id : str
            Id of the scan to retrieve

        Returns
        -------
        db.Scan

        """
        raise NotImplementedError

    def delete_scan(self, id):
        """Delete a scan from the DB.

        Parameters
        ----------
        id : str
            Id of the scan to delete

        """
        raise NotImplementedError




class Scan(object):
    """Class defining the scan object `Scan`.

    Abstract class defining the API used to represent a scan in the ROMI project.

    Attributes
    ----------
    db : db.DB
        Database where to find the scan
    id : int
        Id of the scan in the database `DB`
    """

    def __init__(self, db, id):
        """
        Parameters
        ----------
        db : DB
            Db to create the scan in
        id : str
            Scan id
        """
        self.db = db
        self.id = id

    def get_id(self):
        """Get scan id

        Returns
        -------
        str
        """
        return self.id

    def get_db(self):
        """Get parent db

        Returns
        -------
        db.DB
        """
        return self.db

    def get_filesets(self):
        """Get all sets of files

        Returns
        -------
        list
        """
        raise NotImplementedError

    def get_fileset(self, id, create=False):
        """Get a fileset with a given id

        Parameters
        ----------
        id : str
        create :  bool
            Create the fileset if it does not exist (default : False)

        Returns
        -------
        db.Fileset
        """
        raise NotImplementedError

    def get_metadata(self, key=None):
        """Get metadata associated to scan

        Parameters
        ----------
        key : str
            Metadata key to retrieve (defaults to None)

        Returns
        -------
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan

        If value is None, scan metadata is set to data.
        If value is not None data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value
        value
            Value to set (default is None)
        """
        raise NotImplementedError

    def create_fileset(self, id):
        """ create a set of files

        Parameters
        ----------
        id : str
            Id of the new fileset
        """
        raise NotImplementedError

    def delete_fileset(self, fileset_id):
        """Delete a fileset from the DB.

        Parameters
        ----------
        id : str
            Id of the fileset to delete
        """
        raise NotImplementedError



class Fileset(object):
    """Class defining a set of files `Fileset` contained in a `Scan`.

    Abstract class defining the API used to represent a set of files in the ROMI
    project.

    Notes
    -----
    Files can be 2D images, RGB pictures, text,...

    Attributes
    ----------
    db : db.DB
        database where to find the scan
    id : int
        id of the scan in the database `DB`
    scan : db.Scan
        scan containing the set of files
    """

    def __init__(self, db, scan, id):
        self.db = db
        self.scan = scan
        self.id = id

    def get_id(self):
        """Get scan id

        Returns
        -------
        str
        """
        return self.id

    def get_db(self):
        """Get parent db

        Returns
        -------
        db.DB
        """
        return self.db

    def get_scan(self):
        """Get parent scan

        Returns
        -------
        db.Scan
        """
        return self.scan

    def get_files(self):
        """Get all files

        Returns
        -------
        list
        """
        raise NotImplementedError

    def get_file(self, id, create=False):
        """Get file with given id

        Parameters
        ----------
        id : str
            File id
        create :  bool
            Create the file if it does not exist (default : False)

        Returns
        -------
        db.File
        """
        raise NotImplementedError

    def get_metadata(self, key=None):
        """Get metadata associated to scan

        Parameters
        ----------
        key : str
            Metadata key to retrieve (defaults to None)

        Returns
        -------
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan

        If value is None, scan metadata is set to data.
        If value is not None data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value
        value
            Value to set (default is None)
        """
        raise NotImplementedError

    def create_file(self, id):
        """ create a file

        Parameters
        ----------
        id : str
            Id of the new file
        """
        raise NotImplementedError

    def delete_file(self, file_id):
        """Delete a file from the DB.

        Parameters
        ----------
        id : str
            Id of the file to delete
        """
        raise NotImplementedError


class File(object):
    """Class defining a file `File` contained in a `Fileset`.

    Abstract class defining the API used to represent a file in the ROMI project.

    Attributes
    ----------
    db : DB
        database where to find the scan
    fileset : db.Fileset
        set of file containing the file
    id : str
        id of the scan in the database `DB`
    filename : str
        file format (default = None, can be deduced when importing file)
    """

    def __init__(self, db, fileset, id):
        self.db = db
        self.fileset = fileset
        self.id = id
        self.filename = None

    def get_id(self):
        """Get file id

        Returns
        -------
        str
        """
        return self.id

    def get_db(self):
        """Get parent db

        Returns
        -------
        db.DB
        """
        return self.fileset.scan.db

    def get_scan(self):
        """Get parent scan

        Returns
        -------
        db.Scan
        """
        return self.fileset.scan

    def get_fileset(self):
        """Get parent fileset

        Returns
        -------
        db.Fileset
        """
        return self.fileset


    def get_metadata(self, key=None):
        """Get metadata associated to scan

        Parameters
        ----------
        key : str
            Metadata key to retrieve (defaults to None)

        Returns
        -------
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan

        If value is None, scan metadata is set to data.
        If value is not None data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value
        value
            Value to set (default is None)
        """
        raise NotImplementedError

    def import_file(self, path):
        """Import an existing file to the File object.

        Parameters
        ----------
        path : str
        """
        raise NotImplementedError

    def write_raw(self, buffer, ext=""):
        """Writes bytes to a file

        Parameters
        ----------
        buffer : bytearray
            Data
        """
        raise NotImplementedError

    def read_raw(self):
        """Reads bytes from a file

        Returns
        -------
        buffer : bytearray
        """
        raise NotImplementedError

    def write(self, str, ext=""):
        """Writes text to a file

        Parameters
        ----------
        data : str
            Data
        """
        raise NotImplementedError

    def read(self):
        """Reads text from a file

        Returns
        -------
        str
        """
        raise NotImplementedError

    def write_raw(self, buffer, ext=""):
        """Writes bytes to a file

        Parameters
        ----------
        buffer : bytearray
            Data
        """
        raise NotImplementedError

    def read_raw(self):
        """Reads bytes from a file

        Returns
        -------
        buffer : bytearray
        """
        raise NotImplementedError


class DBBusyError(OSError):
    """ Raises an error if the database is busy.

    This error is raised when the database is busy and an operation cannot be
     done on it.
    """
    def __init__(self, message):
        self.message = message
