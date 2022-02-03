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
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------

import os
import subprocess

from plantdb.fsdb import LOCK_FILE_NAME
from plantdb.fsdb import MARKER_FILE_NAME
from plantdb.fsdb import _is_db


class FSDBSync():
    """Class for sync between two FSDB databases.

    It checks for validity of both source and target by checking:
        * That there is a marker file in the DB path root
        * That the DB is not busy by checking for the lock file in the DB path root.
    It locks the two databases during the sync. The sync is done using rsync as a subprocess


    Attributes
    ----------
    source_str : str
        Source path
    target_str : str
        Target path
    source : dict
        Source path description
    target : dict
        Target path description
    """

    def __del__(self):
        try:
            self.unlock()
        except:
            return

    def __init__(self, source, target):
        """
        Parameters
        ----------
        source : str
            Source database path (remote or local)
        target : str
            Target database path (remote or local)
        """
        self.source_str = source
        self.target_str = target
        self.source = _fmt_path(source)
        self.target = _fmt_path(target)

    def unlock(self):
        """Unlock the source and target DB after sync.
        """
        for x in [self.source, self.target]:
            if x["type"] == "local":
                _unlock_local(x)
            elif x["type"] == "remove":
                _unlock_remote(x)

    def lock(self):
        """Lock the source and target DB before sync.
        """
        for x in [self.source, self.target]:
            if x["type"] == "local":
                _lock_local(x)
            elif x["type"] == "remove":
                _lock_remote(x)

    def sync(self):
        """Sync the two DBs.
        """
        self.lock()
        subprocess.run(["rsync", "-av", self.source_str, self.target_str])
        self.unlock()


def _fmt_path(path):
    """Parses path by checking for a ':' sign.

    Parameters
    ----------
    path : str
        The path to format

    Returns
    -------
    dict
        A dictionary describing the parsed path.
    """
    if ':' in path:  # This is a remote path
        path_split = path.split(':')
        host, path = path_split
        if len(path_split) != 2:
            raise OSError("Invalid path format: %s" % path)
        return {
            "type": "remote",
            "host": host,
            "path": path,
            "lock_path": os.path.join(path, LOCK_FILE_NAME),
            "marker_path": os.path.join(path, MARKER_FILE_NAME)
        }
    else:  # This is a local path
        if not _is_db(path):
            raise OSError("Source is not a valid FSDB path: %s" % os.path.abspath(path))
        return {
            "type": "local",
            "path": os.path.abspath(path),
            "lock_path": os.path.abspath(os.path.join(path, LOCK_FILE_NAME)),
            "marker_path": os.path.abspath(os.path.join(path, MARKER_FILE_NAME))
        }


def _lock_local(d):
    path = d["path"]
    lock_path = d["lock_path"]
    try:
        with open(lock_path, "x") as _:
            pass
    except FileExistsError:
        raise IOError("Could not secure lock, %s is present in DB path." % LOCK_FILE_NAME)


def _lock_remote(d):
    host = d["host"]
    path = d["path"]
    marker_path = d["marker_path"]
    lock_path = d["lock_path"]

    try:
        x = subprocess.run(['ssh', host, 'stat', marker_path], check=True)
    except:
        raise OSError("Not a FSDB path, missing %s." % MARKER_FILE_NAME)
    lock_path = os.path.join(path, LOCK_FILE_NAME)
    try:
        x = subprocess.run(["ssh", host, 'set -o noclobber; echo "$$" > %s' % lock_path], check=True)
    except:
        raise OSError("Could not secure lock, %s is present in DB path." % LOCK_FILE_NAME)


def _unlock_local(d):
    lock_path = d["lock_path"]
    os.remove(lock_path)


def _unlock_remote(d):
    lock_path = d["lock_path"]
    x = subprocess.run(["ssh", d["host"], 'rm %s' % lock_path])
