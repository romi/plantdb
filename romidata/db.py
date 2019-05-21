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

A database ``DB`` contains a list of scans ``Scan`` distinguishable by their id.
A ``Scan`` can be made of several list of files ``Fileset``.
A ``Fileset`` is made of a list of files ``Files``.
A ``File`` can be an image, text of bytes.

TODO: add ``store`` method ?
"""

class DBBusyError(OSError):
    def __init__(self, message):
        self.message = message


class DB(object):
    """Class defining the database object `DB`.

    Abstract class defining the API used to communicate with a database in the
    ROMI project.

    Methods
    -------
    connect:
        connect to the database
    disconnect:
        disconnect from the database
    get_scans:
        get the list of scans saved in the database
    get_scan:
        get a scan save in the database
    create_scan:
        create a new scan object in the database

    """

    def __init__(self):
        pass

    def connect(self, login_data=None):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    def get_scans(self):
        raise NotImplementedError

    def get_scan(self, id):
        raise NotImplementedError

    def create_scan(self, id):
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

    Methods
    -------
    get_id
        get scan id
    get_filesets
        get all sets of files
    get_fileset
        get the set of files associated to scan id
    get_metadata
        get metadata associated to scan
    set_metadata
        set metadata associated to scan
    create_fileset
        create a set of files

    """

    def __init__(self, db, id):
        self.db = db
        self.id = id

    def get_id(self):
        return self.id

    def get_filesets(self):
        raise NotImplementedError

    def get_fileset(self, id):
        raise NotImplementedError

    def get_metadata(self, key=None):
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        raise NotImplementedError

    def create_fileset(self, id):
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

    Methods
    -------
    get_id
        get scan id
    get_db
        get database object
    get_scan
        get scan object
    get_files
        get the list of files associated to scan
    get_metadata
        get metadata associated to ``Fileset``
    set_metadata
        set metadata associated to ``Fileset``
    create_file
        create a file in the given ``Fileset``

    """

    def __init__(self, db, scan, id):
        self.db = db
        self.scan = scan
        self.id = id

    def get_id(self):
        return self.id

    def get_db(self):
        return self.db

    def get_scan(self):
        return self.scan

    def get_files(self):
        raise NotImplementedError

    def get_file(self, id):
        raise NotImplementedError

    def get_metadata(self, key=None):
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        raise NotImplementedError

    def create_file(self, id):
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

    Methods
    -------
    get_id
        get scan id
    get_db
        get database object
    get_fileset
        get list of files
    get_metadata
        get metadata associated to ``File``
    set_metadata
        set metadata associated to ``File``
    write_image
        method writing an image
    write_text
        method writing text
    import_file
        prepare file import from given path and add it to Fileset, use ``read_images``, ``read_text`` or ``read_bytes`` to actually import it!
    read_images
        read the file, given as ``path`` in ``import_file``, as an image
    read_text
        read the file, given as ``path`` in ``import_file``, as text
    read_bytes
        read the file, given as ``path`` in ``import_file``, as bytes

    """

    def __init__(self, db, fileset, id):
        self.db = db
        self.fileset = fileset
        self.id = id

    def get_id(self):
        return self.id

    def get_db(self):
        return self.db

    def get_fileset(self):
        return self.fileset

    def get_metadata(self, key=None):
        raise NotImplementedError

    def set_metadata(self, data, value=None):
        raise NotImplementedError

    def write_image(self, type, image):
        raise NotImplementedError

    def write_text(self, type, string):
        raise NotImplementedError

    def write_bytes(self, type, buffer):
        raise NotImplementedError

    def import_file(self, path):
        raise NotImplementedError

    def read_image(self):
        raise NotImplementedError

    def read_text(self):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError
