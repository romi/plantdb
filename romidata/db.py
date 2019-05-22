# -*- python -*-
# -*- coding: utf-8 -*-
# 
# romidata - Data handling tools for the ROMI project
# 
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
# 
# This file is part of romidata.
# 
# romidata is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
# 
# romidata is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with romidata.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------


"""
API for the database module in the ROMI project.
------------------------------------------------

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
        """Connect to the database
        """
        raise NotImplementedError

    def disconnect(self):
        """disconnect from the database
        """
        raise NotImplementedError

    def get_scans(self):
        """get the list of scans saved in the database
        """
        raise NotImplementedError

    def get_scan(self, id):
        """get a scan saved in the database

        Parameters
        __________
        id : str
            id of the scan to retrieve

        Returns
        _______
        db.Scan
        """
        raise NotImplementedError

    def create_scan(self, id):
        """ create a new scan object in the database

        Parameters
        __________
        id : str
            id of the scan to retrieve

        Returns
        _______
        db.Scan
        """
        raise NotImplementedError


class Scan(object):
    """Class defining the scan object `Scan`.

    Abstract class defining the API used to represent a scan in the ROMI project.

    Attributes
    ----------
    db : DB
        database where to find the scan
    id : int
        id of the scan in the database `DB`
    """

    def __init__(self, db, id):
        """
        Parameters
        __________

        db : DB
            db to create the scan in
        id : str
            scan id
        """
        self.db = db
        self.id = id

    def get_id(self):
        """get scan id

        Returns
        _______
        str
        """
        return self.id

    def get_db(self):
        """ get parent db

        Returns
        _______
        db.DB
        """
        return self.db

    def get_filesets(self):
        """ get all sets of files

        Returns
        _______
        list
        """
        raise NotImplementedError

    def get_fileset(self, id):
        """get a fileset with a given id

        Parameters
        __________
        id : str

        Returns
        _______
        db.Fileset
        """
        raise NotImplementedError

    def get_metadata(self, key=None):
        """ get metadata associated to scan

        Parameters
        __________
        key : str
            metadata key to retrieve (defaults to None)

        Returns
        _______
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """ get metadata associated to scan

        if value is None, scan metadata is set to data. If value is not None
        data is a key and is set to value.

        Parameters
        __________
        data : str or dict
            key or value
        value
            value to set (default is None)
        """
        raise NotImplementedError

    def create_fileset(self, id):
        """ create a set of files

        Parameters
        __________
        id : str
            id of the new fileset
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
    db : DB
        database where to find the scan
    id : int
        id of the scan in the database `DB`
    scan : Scan
        scan containing the set of files
    """

    def __init__(self, db, scan, id):
        self.db = db
        self.scan = scan
        self.id = id

    def get_id(self):
        """get scan id

        Returns
        _______
        str
        """
        return self.id

    def get_db(self):
        """ get parent db

        Returns
        _______
        db.DB
        """
        return self.db

    def get_scan(self):
        """ get parent scan

        Returns
        _______
        db.Scan
        """
        return self.scan

    def get_files(self):
        """ get all files

        Returns
        _______
        list
        """
        raise NotImplementedError

    def get_file(self, id):
        """get file with given id

        Parameters
        __________
        id : str

        Returns
        _______
        db.File
        """
        raise NotImplementedError

    def get_metadata(self, key=None):
        """ get metadata associated to scan

        Parameters
        __________
        key : str
            metadata key to retrieve (defaults to None)

        Returns
        _______
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """ get metadata associated to scan

        if value is None, scan metadata is set to data. If value is not None
        data is a key and is set to value.

        Parameters
        __________
        data : str or dict
            key or value
        value
            value to set (default is None)
        """
        raise NotImplementedError

    def create_file(self, id):
        """ create a file

        Parameters
        __________
        id : str
            id of the new file
        """
        raise NotImplementedError


class File(object):
    """Class defining a file `File` contained in a `Fileset`.

    Abstract class defining the API used to represent a file in the ROMI project.

    Attributes
    ----------
    db : DB
        database where to find the scan
    fileset : Fileset
        set of file containing the file
    id : int
        id of the scan in the database `DB`
    """

    def __init__(self, db, fileset, id):
        self.db = db
        self.fileset = fileset
        self.id = id

    def get_id(self):
        """get file id

        Returns
        _______
        str
        """
        return self.id

    def get_db(self):
        """ get parent db

        Returns
        _______
        db.DB
        """
        return self.fileset.scan.db

    def get_scan(self):
        """ get parent scan

        Returns
        _______
        db.Scan
        """
        return self.fileset.scan

    def get_fileset(self):
        """ get parent fileset

        Returns
        _______
        db.FileSet
        """
        return self.fileset


    def get_metadata(self, key=None):
        """ get metadata associated to scan

        Parameters
        __________
        key : str
            metadata key to retrieve (defaults to None)

        Returns
        _______
        dict or value
        """
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        """ get metadata associated to scan

        if value is None, scan metadata is set to data. If value is not None
        data is a key and is set to value.

        Parameters
        __________
        data : str or dict
            key or value
        value
            value to set (default is None)
        """
        raise NotImplementedError

    def write_image(self, type, image):
        """Writes an image to the file

        Parameters
        __________
        type : str
            File extension of the image (e.g. "jpg" or "png")
        image : numpy.array
            Image data (NxM or NxMx3)
        """
        raise NotImplementedError

    def write_text(self, type, string):
        """Writes a string to a file

        Parameters
        __________
        type : str
            File extension of the file
        string : str
            data
        """
        raise NotImplementedError

    def write_bytes(self, type, buffer):
        """Writes bytes to a file

        Parameters
        __________
        type : str
            File extension of the file
        buffer : bytearray
            data
        """
        raise NotImplementedError

    def import_file(self, path):
        """Import an existing file to the File object.

        Parameters
        __________
        path : str
        """
        raise NotImplementedError

    def read_image(self):
        """Reads image data from a file

        Returns
        _______
        image : numpy.array
        """
        raise NotImplementedError

    def read_text(self):
        """Reads string from a file

        Returns
        _______
        string : str
        """
        raise NotImplementedError

    def read_bytes(self):
        """Reads bytes from a file

        Returns
        _______
        buffer : bytearray
        """
        raise NotImplementedError

class DBBusyError(OSError):
    """This  error is raised when the Database is busy and an operation cannot be done on it.
    """
    def __init__(self, message):
        self.message = message
