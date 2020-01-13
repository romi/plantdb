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
romidata.task
=============

ROMI Luigi Tasks

This module implements subclasses of ``luigi.Config``, ``luigi.Target`` and ``luigi.Tasks``. The goal is to
have luigi tasks work seemlessly with the database API implemented in ``romidata.db``.

A ``FilesetTarget`` is a luigi target corresponding to a ``Fileset`` object.

A ``RomiTask`` must implement two methods : ``run`` and ``requires``.
To check for a task completeness, the fileset existence is checked as well as all it's dependencies.
"""

import luigi
import os

from romidata import FSDB

db = None

class ScanParameter(luigi.Parameter):
    def serialize(self, scan):
        db_path = scan.db.basedir
        scan_id = scan.id
        return '/'.join([db_path, scan_id])

    def parse(self, x):
        global db
        path = x.rstrip('/')

        path = path.split('/')
        db_path = '/'.join(path[:-1])
        scan_id = path[-1]
        if db is None: # TODO: cannot change DB during run..
            db = FSDB(db_path)
            db.connect()
        scan = db.get_scan(scan_id)
        if scan is None:
            scan = db.create_scan(scan_id)
        return scan

class DatabaseConfig(luigi.Config):
    """Configuration for the database."""
    scan = ScanParameter()

class FilesetTarget(luigi.Target):
    """Implementation of a luigi Target for the romidata DB API.

    Attributes
    __________
    db : DB
        database object
    scan : Scan
        scan in which the target is
    fileset_id : str
        id if the target fileset

    """
    def __init__(self, scan, fileset_id):
        """
        Parameters
        __________

            db : romiscan.db.DB
                database object
            scan_id : str
                id of the scan where the fileset is located
            fileset_id : str
                id of the target fileset
        """
        self.db = scan.db
        self.scan = scan
        self.fileset_id = fileset_id

    def create(self):
        """Creates a target by creating the filset using the romidata DB API.

        Returns
        -------
            fileset (romiscan.db.Fileset)

        """
        return self.scan.create_fileset(self.fileset_id)

    def exists(self):
        """A target exists if the associated fileset exists and is not empty.

        Returns
        -------
        
            exists : bool
        """
        fs = self.scan.get_fileset(self.fileset_id)
        return fs is not None

    def get(self, create=True):
        """Returns the corresponding fileset object.

        Parameters
        ----------
        create : bool
            create the fileset if it does not exist in the database. (default is `True`)

        Returns
        -------
            fileset : romiscan.db.Fileset

        """
        return self.scan.get_fileset(self.fileset_id, create=create)

class RomiTask(luigi.Task):
    """Implementation of a luigi Task for the romidata DB API."""

    upstream_task = luigi.TaskParameter()
    output_file_id = luigi.Parameter(default="out")

    def requires(self):
        return self.upstream_task()


    def output(self):
        """Output for a RomiTask is a FileSetTarget, the fileset ID being
        the task ID.
        """
        fileset_id = self.task_id
        return FilesetTarget(DatabaseConfig().scan, fileset_id)

    def complete(self):
        """Checks if a task is complete by checking if Filesets corresponding
        to te task id exist.
        
        Contrary to original luigi Tasks, this check for completion
        of all required tasks to decide wether it is complete.
        """
        outs = self.output()
        if isinstance(outs, dict):
            outs = [outs[k] for k in outs.keys()]
        elif isinstance(outs, list):
            pass
        else:
            outs = [outs]

        if not all(map(lambda output: output.exists(), outs)):
            return False

        req = self.requires()
        if isinstance(req, dict):
            req = [req[k] for k in req.keys()]
        elif isinstance(req, list):
            pass
        else:
            req = [req]
        for task in req:
            if not task.complete():
                return False
        return True

    def input_file(self, file_id=None):
        """Helper function to get a file from
        the input fileset.

        Parameters
        ----------
        file_id : str
            id of the input file

        Returns
        _______
        db.File

        """
        return self.upstream_task().output_file(file_id)


    def output_file(self, file_id=None):
        """Helper function to get a file from
        the output  fileset.

        Parameters
        ----------
        file_id : str
            id of the input file

        Returns
        _______
        db.File

        """
        if file_id is None:
            file_id = self.get_task_family().split('.')[-1]
        return self.output().get().get_file(file_id, create=True)

class FilesetExists(luigi.Task):
    """A Task which requires a fileset with a given
    id to exist. 
    """
    fileset_id = None

    def requires(self):
        return []

    def run(self):
        if self.output().get() is None:
            raise OSError("Fileset %s does not exist"%self.fileset_id)

    def output(self):
        return FilesetTarget(DatabaseConfig().scan, self.fileset_id)

class ImagesFilesetExists(FilesetExists):
    """A Task which requires the presence of a fileset with id ``images``
    """
    fileset_id = "images"

class FileByFileTask(RomiTask):
    """This abstract class is a Task which take every file from a fileset
    and applies some function to it and saves it back
    to the target.
    """
    query = luigi.Parameter(default=None)
    type = None

    reader = None
    writer = None

    def f(self, f, outfs):
        """Function applied to every file in the fileset
        must return a file object
        """
        raise NotImplementedError

    def run(self):
        """Run the task on every file in the fileset.
        """
        input_fileset = self.input().get()
        output_fileset = self.output().get()
        for fi in input_fileset.get_files(query=self.query):
            outfi = self.f(fi, output_fileset)
            m = fi.get_metadata()
            outm = outfi.get_metadata()
            outfi.set_metadata({**m, **outm})


@RomiTask.event_handler(luigi.Event.FAILURE)
def mourn_failure(task, exception):
    """In the case of failure of a task, remove the corresponding fileset from
    the database.

    Parameters
    ----------
    task : RomiTask
        The task which has failed
    exception :
        The exception raised by the failure
    """
    output_fileset = task.output().get()
    scan = task.output().get().scan
    scan.delete_fileset(output_fileset.id)

class DummyTask(RomiTask):
    """A RomiTask which does nothing and requires nothing."""
    def requires(self):
        """ """
        return []

    def run(self):
        """ """
        return
