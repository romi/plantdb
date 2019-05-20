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


"""Implementation of a database as a local file structure.

Assuming the following file structure:

2018/
2018/images/
2018/images/rgb0001.jpg
2018/images/rgb0002.jpg

The 2018/files.json file then contains the following structure:

{
    "filesets": [
        {
            "id": "images",
            "files": [
                {
                    "id": "rgb00001",
                    "file": "rgb00001.jpg"
                },
                {
                    "id": "rgb00002",
                    "file": "rgb00002.jpg"
                }
            ]
        }
    ]
}

The metadata of the scan, filesets, and images are stored all as
json objects in a separate directory:

2018/metadata/
2018/metadata/metadata.json
2018/metadata/images.json
2018/metadata/images/rgb0001.json
2018/metadata/images/rgb0002.json
"""

import glob
import os
import sys
import json
import copy
import imageio 
from shutil import copyfile

from romidata import db

MARKER_FILE_NAME = "romidb" # This file must exist in the root of a folder for it to be considered a valid DB
LOCK_FILE_NAME = "lock" # This file prevents opening the DB if it is present in the root folder of a DB

class FSDB(db.DB):
    """Class defining the database object `DB`.

    Implementation of a database as a simple file structure with:
      * `images` folder containing image files
      * `metadata` folder containing JSON metadata associated to image files

    Attributes
    ----------
    basedir : str
        path to the base directory containing the database
    scans : list
        list of `Scan` objects found in the database
    is_connected : bool
        True iff the DB is connected (locked the directory)

    Methods
    -------
    connect:
        no connection required here
    disconnect:
        no connection required here
    get_scans:
        get the list of scans saved in the database
    get_scan:
        get a scan save in the database
    create_scan:
        create a new scan object in the database
    """

    def __init__(self, basedir):
        """Database constructor.

        Check given ``basedir`` directory exists and load accessible ``Scan``
        objects.

        Parameters
        ----------
        basedir : str
            root directory of the database

        Examples
        --------
        >>> from romidata import FSDB
        >>> db = FSDB('$HOME/example_path/')

        """
        if not os.path.isdir(basedir):
            raise IOError("Not a directory: %s" % basedir)
        if not _is_db(basedir):
            raise IOError("Not a DB. Check that there is a marker named %s in %s" % (MARKER_FILE_NAME, basedir))
        self.basedir = basedir
        self.lock_path = os.path.abspath(os.path.join(basedir, LOCK_FILE_NAME))
        self.scans = []
        self.is_connected = False

    def connect(self, login_data=None):
        """Connects to DB by creating a lock file.
        """
        if not self.is_connected:
            try:
                with open(self.lock_path, "x") as _:
                    self.scans = _load_scans(self)
                    self.is_connected = True
            except FileExistsError:
                raise IOError("File %s exists in DB root: DB is busy, cannot connect."%LOCK_FILE_NAME)

    def disconnect(self):
        """Disconnects by removing lock.
        """
        if self.is_connected:
            if _is_safe_to_delete(self.lock_path):
                os.remove(self.lock_path)
            else:
                raise IOError("Could not remove lock, maybe you messed with the lock_path attribute?")
            self.scans = []
            self.is_connected = False

    def __del__(self):
        self.disconnect()


    def get_scans(self):
        """Get the list of scans saved in the database.

        Returns
        -------
        scans : list
            list of `Scan` objects found in the database
        """
        return self.scans

    def get_scan(self, id):
        """Get a scan saved in the database under given `id`.

        Parameters
        ----------
        id : int
            identifier of the scan to get

        Returns
        -------
        scan : Scan
            `Scan` object associated to given `id`
        """
        for scan in self.scans:
            if scan.get_id() == id:
                return scan
        return None

    def create_scan(self, id):
        """Create a new scan object in the database.

        Parameters
        ----------
        id : int
            identifier of the scan to create

        Returns
        -------
        scan: Scan
            a new scan object from the database
        """
        if not _is_valid_id(id):
            raise IOError("Invalid id")
        if self.get_scan(id) != None:
            raise IOError("Duplicate scan name: %s" % id)
        scan = Scan(self, id)
        _make_scan(scan)
        self.scans.append(scan)
        return scan

    def delete_scan(self, scan_id):
        """
        Deletes a scan

        Parameters
        ----------
            scan_id (str): id of the scan to remove.
        """
        scan = self.get_scan(scan_id)
        if scan is None:
            raise IOError("Invalid id")
        _delete_scan(scan)
        self.scans.remove(scan)


class Scan(db.Scan):
    """Class defining the scan object `Scan`.

    Implementation of a scan as a list of files with attached metadata.

    Attributes
    ----------
    db : FSDB
        database where to find the scan
    id : int
        id of the scan in the database `db`
    metadata : dict
        dictionary of metadata attached to `Scan` object
    filesets : list
        list of `Fileset` attached to `Scan` object

    """

    def __init__(self, db, id):
        super().__init__(db, id)
        self.metadata = None
        self.filesets = []

    def get_filesets(self):
        return self.filesets  # Copy?

    
    def get_fileset(self, id, create=False):
        for fileset in self.filesets:
            if fileset.get_id() == id:
                return fileset
        if create:
            return self.create_fileset(id)
        return None

    def get_metadata(self, key=None):
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_scan_metadata(self)

    def create_fileset(self, id):
        if not _is_valid_id(id):
            raise IOError("Invalid id")
        if self.get_fileset(id) != None:
            raise IOError("Duplicate fileset name: %s" % id)
        fileset = Fileset(self.db, self, id)
        _make_fileset(fileset)
        self.filesets.append(fileset)
        self.store()
        return fileset

    def store(self):
        _store_scan(self)

    def delete_fileset(self, fileset_id):
        fs = self.get_fileset(fileset_id)
        if fs is None:
            raise IOError("Invalid ID: %s"%fileset_id)
        _delete_fileset(fs)
        self.filesets.remove(fs)
        self.store()

        
class Fileset(db.Fileset):

    def __init__(self, db, scan, id):
        super().__init__(db, scan, id)
        self.metadata = None
        self.files = []

    def get_files(self):
        return self.files

    def get_file(self, id, create=False):
        ids = [f.id for f in self.files]
        if id not in ids and not create:
            return None
        if id not in ids and create:
            return self.create_file(id)
        return self.files[ids.index(id)]

    def get_metadata(self, key=None):
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_fileset_metadata(self)

    def create_file(self, id):
        file = File(self.db, self, id, None)
        self.files.append(file)
        self.store()
        return file

    def delete_file(self, file_id):
        x = self.get_file(file_id)
        if x is None:
            raise IOError("Invalid file ID: %s"%file_id)
        _delete_file(x)
        self.files.remove(x)
        self.store()
    
    def store(self):
        self.scan.store()


class File(db.File):

    def __init__(self, db, fileset, id, filename):
        super().__init__(db, fileset, id)
        self.filename = filename
        self.metadata = None

    def get_metadata(self, key=None):
        return _get_metadata(self.metadata, key)

    def set_metadata(self, data, value=None):
        if self.metadata == None:
            self.metadata = {}
        _set_metadata(self.metadata, data, value)
        _store_file_metadata(self)

    def write_image(self, type, image):
        filename = _get_filename(self, type)
        path = _file_path(self, filename)
        imageio.imwrite(path, image)
        self.filename = filename
        self.store()

    def write_text(self, type, string):
        filename = _get_filename(self, type)
        path = _file_path(self, filename)
        with open(path, "w") as f:
            f.write(string)
        self.filename = filename
        self.store()

    def write_bytes(self, type, buffer):
        filename = _get_filename(self, type)
        path = _file_path(self, filename)
        with open(path, "wb") as f:
            f.write(buffer)
        self.filename = filename
        self.store()

    def import_file(self, path):
        filename = os.path.basename(path)
        newpath = _file_path(self, filename)
        copyfile(path, newpath)
        self.filename = filename
        self.store()

    def read_image(self):
        path = _file_path(self, self.filename)
        return imageio.imread(path)

    def read_text(self):
        path = _file_path(self, self.filename)
        with open(path, "r") as f:
            return f.read()

    def read_bytes(self):
        path = _file_path(self, self.filename)
        with open(path, "rb") as f:
            return f.read()

    def store(self):
        self.fileset.store()


##################################################################
#
# the ugly stuff...
#

# load the database

def _load_scans(db):
    """Load defined scans in given database object.

    List sub-directories of ``db.basedir``
    """
    scans = []
    names = os.listdir(db.basedir)
    for name in names:
        scan = Scan(db, name)
        if (os.path.isdir(_scan_path(scan))
                and os.path.isfile(_scan_files_json(scan))):
            scan.filesets = _load_scan_filesets(scan)
            scan.metadata = _load_scan_metadata(scan)
            scans.append(scan)
            # scan.store()
    return scans


def _load_scan_filesets(scan):
    filesets = []
    files_json = _scan_files_json(scan)
    with open(files_json, "r") as f:
        structure = json.load(f)
    filesets_info = structure["filesets"]
    if isinstance(filesets_info, list):
        for fileset_info in filesets_info:
            try:
                fileset = _load_fileset(scan, fileset_info)
                filesets.append(fileset)
            except:
                id = fileset_info.get("id")
                print("Warning: unable to load fileset %s, deleting..."%id)
                # scan.delete_fileset(id)
    else:
        raise IOError("%s: filesets is not a list" % files_json)
    return filesets


def _load_fileset(scan, fileset_info):
    fileset = _parse_fileset(scan.db, scan, fileset_info)
    fileset.files = _load_fileset_files(fileset, fileset_info)
    fileset.metadata = _load_fileset_metadata(fileset)
    return fileset


def _parse_fileset(db, scan, fileset_info):
    id = fileset_info.get("id")
    if id == None:
        raise IOError("Fileset: No ID")
    fileset = Fileset(db, scan, id)
    path = _fileset_path(fileset)
    if not os.path.isdir(path):
        raise IOError(
            "Fileset: Fileset directory doesn't exists: %s" % path)
    return fileset


def _load_fileset_files(fileset, fileset_info):
    files = []
    files_info = fileset_info.get("files", [])
    if isinstance(files_info, list):
        for file_info in files_info:
            try:
                file = _load_file(fileset, file_info)
                files.append(file)
            except:
                id = file_info.get("id")
                print("Warning: unable to load file %s, deleting..."%id)
                # fileset.delete_file(id)
    else:
        raise IOError("files.json: expected a list for files")
    return files


def _load_file(fileset, file_info):
    file = _parse_file(fileset, file_info)
    file.metadata = _load_file_metadata(file)
    return file


def _parse_file(fileset, file_info):
    id = file_info.get("id")
    if id == None:
        raise IOError("File: No ID")
    filename = file_info.get("file")
    if filename == None:
        raise IOError("File: No filename")
    file = File(fileset.db, fileset, id, filename)
    path = _file_path(file, filename)
    if not os.path.isfile(path):
        raise IOError("File: File doesn't exists: %s" % path)
    return file


# load/store metadata from disk

def _load_metadata(path):
    if os.path.isfile(path):
        with open(path, "r") as f:
            r = json.load(f)
        if not isinstance(r, dict):
            raise IOError("Not a JSON object: %s" % path)
        return r
    else:
        return {}


def _load_scan_metadata(scan):
    return _load_metadata(_scan_metadata_path(scan))


def _load_fileset_metadata(fileset):
    return _load_metadata(_fileset_metadata_path(fileset))


def _load_file_metadata(file):
    return _load_metadata(_file_metadata_path(file))


def _mkdir_metadata(path):
    dir = os.path.dirname(path)
    if not os.path.isdir(dir):
        os.makedirs(dir)


def _store_metadata(path, metadata):
    _mkdir_metadata(path)
    with open(path, "w") as f:
        json.dump(metadata, f, sort_keys=True,
                  indent=4, separators=(',', ': '))


def _store_scan_metadata(scan):
    _store_metadata(_scan_metadata_path(scan),
                    scan.metadata)


def _store_fileset_metadata(fileset):
    _store_metadata(_fileset_metadata_path(fileset),
                    fileset.metadata)


def _store_file_metadata(file):
    _store_metadata(_file_metadata_path(file),
                    file.metadata)


#

def _get_metadata(metadata, key):
    # Do a deepcopy of the return value because we don't want to
    # caller the inadvertedly change the values.
    if metadata == None:
        return {}
    elif key == None:
        return copy.deepcopy(metadata)
    else:
        return copy.deepcopy(metadata.get(str(key)))


def _set_metadata(metadata, data, value):
    if isinstance(data, str):
        if value == None:
            raise IOError("No value given for key %s" % data)
        # Do a deepcopy of the value because we don't want to caller
        # the inadvertedly change the values.
        metadata[data] = copy.deepcopy(value)
    elif isinstance(data, dict):
        for key, value in data.items():
            _set_metadata(metadata, key, value)
    else:
        raise IOError("Invalid key: ", data)


#

def _make_fileset(fileset):
    path = _fileset_path(fileset)
    if not os.path.isdir(path):
        os.makedirs(path)


def _make_scan(scan):
    path = _scan_path(scan)
    if not os.path.isdir(path):
        os.makedirs(path)


# paths

def _get_filename(file, type):
    return file.id + "." + type


def _scan_path(scan):
    return os.path.join(scan.db.basedir,
                        scan.id)


def _fileset_path(fileset):
    return os.path.join(fileset.db.basedir,
                        fileset.scan.id,
                        fileset.id)


def _file_path(file, filename):
    return os.path.join(file.db.basedir,
                        file.fileset.scan.id,
                        file.fileset.id,
                        filename)


def _scan_files_json(scan):
    return os.path.join(scan.db.basedir,
                        scan.id,
                        "files.json")


def _scan_metadata_path(scan):
    return os.path.join(scan.db.basedir,
                        scan.id,
                        "metadata",
                        "metadata.json")


def _fileset_metadata_path(fileset):
    return os.path.join(fileset.db.basedir,
                        fileset.scan.id,
                        "metadata",
                        fileset.id + ".json")


def _file_metadata_path(file):
    return os.path.join(file.db.basedir,
                        file.fileset.scan.id,
                        "metadata",
                        file.fileset.id,
                        file.id + ".json")


# store a scan to disk

def _file_to_dict(file):
    return {"id": file.get_id(), "file": file.filename}


def _fileset_to_dict(fileset):
    files = []
    for f in fileset.get_files():
        files.append(_file_to_dict(f))
    return {"id": fileset.get_id(), "files": files}


def _scan_to_dict(scan):
    filesets = []
    for fileset in scan.get_filesets():
        filesets.append(_fileset_to_dict(fileset))
    return {"filesets": filesets}


def _store_scan(scan):
    structure = _scan_to_dict(scan)
    files_json = _scan_files_json(scan)
    with open(files_json, "w") as f:
        json.dump(structure, f, sort_keys=True,
                  indent=4, separators=(',', ': '))


def _is_valid_id(id):
    return True # haha  (FIXME!)

def _is_db(path):
    return os.path.exists(os.path.join(path, MARKER_FILE_NAME))

def _is_safe_to_delete(path):
    """ A path is safe to delete only if it's a subfolder of a db.
    """
    path = os.path.abspath(path)
    while True:
        if _is_db(path):
            return True
        newpath = os.path.abspath(os.path.join(path, os.path.pardir))
        if newpath == path:
            return False
        path = newpath

def _delete_file(file):
    fullpath = os.path.join(file.fileset.scan.db.basedir, file.fileset.scan.id, file.fileset.id, file.filename)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    if os.path.exists(fullpath):
        os.remove(fullpath)


def _delete_fileset(fileset):
    for f in fileset.files:
        fileset.delete_file(f.id)
    fullpath = os.path.join(fileset.scan.db.basedir, fileset.scan.id, fileset.id)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    for f in glob.glob(os.path.join(fullpath, "*")):
        os.remove(f)
    if os.path.exists(fullpath):
        os.rmdir(fullpath)

def _delete_scan(scan):
    for f in scan.filesets:
        scan.delete_fileset(f.id)
    fullpath = os.path.join(scan.db.basedir, scan.id)
    if not _is_safe_to_delete(fullpath):
        raise IOError("Cannot delete files outside of a DB.")
    for f in glob.glob(os.path.join(fullpath, "*")):
        os.remove(f)
    if os.path.exists(fullpath):
        os.rmdir(fullpath)
