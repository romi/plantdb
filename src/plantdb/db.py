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
API for the database module in the ROMI project.

The following classes are defined:
  * A database ``DB`` contains a list of scans ``Scan`` distinguishable by their id.
  * A ``Scan`` can be made of several list of files ``Fileset``.
  * A ``Fileset`` is made of a list of files ``Files``.
  * A ``File`` can be an image, text of bytes.

It should be subclassed to implement an actual database interface.
"""
from copy import deepcopy


class DB(object):
    """Class defining the database object ``DB``.

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
            Use this to access to a ``DB`` with credentials.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def disconnect(self):
        """Disconnect from the database.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_scans(self):
        """Get the list of scans saved in the database.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_scan(self, id, create=False):
        """Get a scan saved in the database.

        Parameters
        ----------
        id : str
            Id of the scan instance to retrieve.
        create : bool, optional
            Create the scan if it does not exist, default to ``False``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def create_scan(self, id):
        """Create a new scan object in the database.

        Parameters
        ----------
        id : str
            Id of the scan to retrieve

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def delete_scan(self, id):
        """Delete a scan from the DB.

        Parameters
        ----------
        id : str
            Id of the scan to delete

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError


class Scan(object):
    """Class defining the scan object ``Scan``.

    Abstract class defining the API used to represent a scan in the ROMI project.

    Attributes
    ----------
    db : plantdb.db.DB
        Database instance where to find the scan.
    id : int
        Id of the scan instance.
    """

    def __init__(self, db, id):
        """Constructor.

        Parameters
        ----------
        db : plantdb.db.DB
            Database instance where to find the scan.
        id : str
            Id of the scan instance.
        """
        self.db = db
        self.id = id

    def get_id(self):
        """Get the scan instance id.

        Returns
        -------
        str
            Id of the scan instance.
        """
        return deepcopy(self.id)

    def get_db(self):
        """Get parent database instance.

        Returns
        -------
        plantdb.db.DB
            Database instance where to find the scan.
        """
        return self.db

    def get_filesets(self):
        """Get all sets of files.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_fileset(self, id, create=False):
        """Get a fileset with a given id.

        Parameters
        ----------
        id : str
            Id of the fileset to be retrieved.
        create : bool, optional
            Create the fileset if it does not exist, default to ``False``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_metadata(self, key=None, default=None):
        """Get metadata associated to scan.

        Parameters
        ----------
        key : str
            Metadata key to retrieve, default to ``None``.
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is ``None``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan.

        If value is ``None``, scan metadata is set to data.
        If value is not ``None`` data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value.
        value : any, optional
            Value to set, default to ``None``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def create_fileset(self, id):
        """Create a fileset.

        Parameters
        ----------
        id : str
            Id of the new fileset.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def delete_fileset(self, fileset_id):
        """Delete a fileset from the DB.

        Parameters
        ----------
        id : str
            Id of the fileset to delete.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError


class Fileset(object):
    """Class defining a set of files ``Fileset`` contained in a ``Scan``.

    Abstract class defining the API used to represent a set of files in the ROMI
    project.

    Notes
    -----
    Files can be 2D images, RGB pictures, text,...

    Attributes
    ----------
    db : plantdb.db.DB
        Database instance where to find the scan and fileset.
    scan : plantdb.db.Scan
        Scan instance containing the fileset.
    id : int
        Id of the fileset instance.
    """

    def __init__(self, scan, id):
        """Constructor.

        Parameters
        ----------
        scan : plantdb.db.Scan
            Scan instance containing the fileset.
        id : str
            Id of the fileset instance.
        """
        self.db = scan.get_db()
        self.scan = scan
        self.id = id

    def get_id(self):
        """Get the fileset instance id.

        Returns
        -------
        str
            The id of the fileset instance.
        """
        return deepcopy(self.id)

    def get_db(self):
        """Get parent database instance.

        Returns
        -------
        plantdb.db.DB
            The parent database instance.
        """
        return self.db

    def get_scan(self):
        """Get parent scan instance.

        Returns
        -------
        plantdb.db.Scan
            The parent scan instance.
        """
        return self.scan

    def get_files(self):
        """Get all files.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_file(self, id, create=False):
        """Get file with given id.

        Parameters
        ----------
        id : str
            File instance id.
        create : bool, optional
            Create the file if it does not exist, default to ``False``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def get_metadata(self, key=None, default=None):
        """Get metadata associated to scan.

        Parameters
        ----------
        key : str
            Metadata key to retrieve, default to ``None``.
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is ``None``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan.

        If value is ``None``, scan metadata is set to data.
        If value is not ``None`` data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value to set as metadata.
        value : any, optional
            Value to set, default to ``None``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def create_file(self, id):
        """Create a file.

        Parameters
        ----------
        id : str
            Id of the new file.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def delete_file(self, file_id):
        """Delete a file.

        Parameters
        ----------
        id : str
            Id of the file to delete.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError


class File(object):
    """Class defining a file ``File`` contained in a ``Fileset``.

    Abstract class defining the API used to represent a file in the ROMI project.

    Attributes
    ----------
    db : plantdb.db.DB
        Database instance where to find the scan, fileset and file.
    scan : plantdb.db.Scan
        Scan instance containing the fileset and file.
    fileset : plantdb.db.Fileset
        Fileset instance containing the file.
    id : str
        Id of the file instance.
    """

    def __init__(self, fileset, id, **kwargs):
        """Constructor.

        Parameters
        ----------
        fileset : plantdb.db.Fileset
            Instance containing the file.
        id : str
            Id of the file instance.

        Other Parameters
        ----------------
        ext : str
            The extension of the file.
        """
        self.db = fileset.get_db()
        self.scan = fileset.get_scan()
        self.fileset = fileset
        self.id = id
        ext = kwargs.get('ext', '')
        self.filename = id + ext if ext != '' else None

    def get_id(self):
        """Get file id.

        Returns
        -------
        str
            The id of the file instance.
        """
        return deepcopy(self.id)

    def get_db(self):
        """Get parent database instance.

        Returns
        -------
        plantdb.db.DB
            The parent database instance.
        """
        return self.db

    def get_scan(self):
        """Get parent scan instance.

        Returns
        -------
        plantdb.db.Scan
            The parent scan instance.
        """
        return self.scan

    def get_fileset(self):
        """Get parent fileset.

        Returns
        -------
        plantdb.db.Fileset
            The parent fileset instance.
        """
        return self.fileset

    def get_metadata(self, key=None, default=None):
        """Get metadata associated to scan.

        Parameters
        ----------
        key : str, optional
            Metadata key to retrieve (defaults to ``None``)
        default : Any, optional
            The default value to return if the key do not exist in the metadata.
            Default is ``None``.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """Get metadata associated to scan.

        If value is ``None``, scan metadata is set to data.
        If value is not ``None`` data is a key and is set to value.

        Parameters
        ----------
        data : str or dict
            Key or value to set as metadata
        value : any, optional
            Value to set (default is ``None``)

        Raises
        ------
        NotImplementedError
       """
        raise NotImplementedError

    def import_file(self, path):
        """Import an existing file to the ``File`` object.

        Parameters
        ----------
        path : str
            Path of the file to import

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def write_raw(self, buffer, ext=""):
        """Writes bytes to a file.

        Parameters
        ----------
        buffer : bytearray
            Data to write
        ext : str, optional
            File extension to use

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def read_raw(self):
        """Reads bytes from a file.

        Returns
        -------
        bytearray
            File buffer

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def write(self, str, ext=""):
        """Writes to a file.

        Parameters
        ----------
        data : str
            Data to write
        ext : str, optional
            File extension to use

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError

    def read(self):
        """Reads from a file.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError


class DBBusyError(OSError):
    """Raises an error if the database is busy.

    This error is raised when the database is busy and an operation cannot be done on it.
    """

    def __init__(self, message):
        self.message = message
