#!/usr/bin/env python3
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

This module implements subclasses of ``luigi.Config``, ``luigi.Target`` and ``luigi.Tasks``.
The goal is to have luigi tasks work seamlessly with the database API implemented in ``romidata.db``.

A ``FilesetTarget`` is a luigi target corresponding to a ``Fileset`` object.

A ``RomiTask`` must implement two methods : ``run`` and ``requires``.

To check for a task completeness, the fileset existence is checked as well as all it's dependencies.
"""
import json

import luigi
from romidata import FSDB
from romidata.log import logger

db = None


class ScanParameter(luigi.Parameter):
    """Register a luigi `Parameter` object to access `fsdb.Scan` class.
    Override the default implementation methods `parse` & `serialize`

    Notes
    -----
    The `parse` method connect to the given path to an `FSDB` database.

    """

    def parse(self, scan_path):
        """Convert the scan path to an `fsdb.Scan` object...
        Override the default implementation method for specialized parsing.

        Parameters
        ----------
        scan_path : str
            The value to parse, here the path to an `fsdb.Scan` dataset.

        Returns
        -------
        fsdb.Scan
            The object corresponding to given path.

        Notes
        -----
        Uses given path, eg. `/db/root/path/scan_id`, to defines:
          - the database root dir with `/db/root/path`
          - the scan dataset id with `scan_id`
        If the given scan dataset id does not exists, it is created.

        """
        global db
        path = scan_path.rstrip('/')
        path = path.split('/')
        # Defines the database root dir
        db_path = '/'.join(path[:-1])
        # Defines the scan dataset id
        scan_id = path[-1]
        # create & connect to `db` if not defined:
        if db is None:  # TODO: cannot change DB during run..
            db = FSDB(db_path)
            db.connect()
        # Get the scan dataset object or create one & return it
        scan = db.get_scan(scan_id)
        if scan is None:
            scan = db.create_scan(scan_id)
        return scan

    def serialize(self, scan):
        """Converts `fsdb.Scan` in its path.

        Opposite of `parse()`.

        Parameters
        ----------
        scan : fsdb.Scan
            The value to serialize, here an `fsdb.Scan` object.

        Returns
        -------
        str
            The path to the corresponding `Scan` object.

        """
        db_path = scan.db.basedir
        scan_id = scan.id
        return '/'.join([db_path, scan_id])


class DatabaseConfig(luigi.Config):
    """Configuration for the `FSBD` database. """
    scan = ScanParameter()


class FilesetTarget(luigi.Target):
    """Subclass luigi `Target` for `Fileset` as defined in romidata `FSDB` API.

    A `FilesetTarget` is used by `luigi.Task` (or subclass) methods:
     * `requires` to assert the existence of the `Fileset` prior to starting the task;
     * `output` to create a `Fileset` after running a task.

    Attributes
    ----------
    db : romidata.fsdb.FSDB
        An `FSDB` database instance.
    scan : romidata.fsdb.Scan
        A `Scan` dataset instance within `db`.
    fileset_id : str
        Id of the target `Fileset` instance within `scan`.

    Notes
    -----
    A luigi `Target` subclass requires to implement the `exists` method.

    Examples
    --------
    >>> from romidata.task import FilesetTarget
    >>> from romidata import FSDB
    >>> from romidata.fsdb import dummy_db
    >>> # - First, let's create a dummy FSDB database to play with:
    >>> db = dummy_db()
    >>> db.connect()
    >>> scan = db.create_scan("007")  # Add a `Scan` named `007` to the `FSDB` instance
    >>> fs = scan.create_fileset("required_fs")  # Add a `Fileset` named `required_fs` to the `Scan` instance

    >>> # - Now let's use `FilesetTarget` to check if a given `Fileset` exists in the FSDB (as used in Tasks `require` methods):
    >>> fst = FilesetTarget(scan, "required_fs")
    >>> fst.exists()  # `Fileset` exist but is empty
    False

    >>> # - Add a dummy test file to the `Fileset`:
    >>> fs.create_file('dummy_test_file')
    >>> fst.exists()  # `Fileset` exist and is not empty
    True

    >>> # - Now let's use `FilesetTarget` to create a new `Fileset` (as used in Tasks `output` methods):
    >>> out_fst = FilesetTarget(scan, "output_fs")
    >>> out_fs = out_fst.create()
    >>> type(out_fs)
    romidata.fsdb.Fileset
    >>> print(out_fs.id)
    'output_fs'

    """

    def __init__(self, scan, fileset_id):
        """FilesetTarget constructor.

        Parameters
        ----------
        scan : romidata.fsdb.Scan
            The `Scan` dataset instance where to find/create the `Fileset`.
        fileset_id : str
            Id of the target `Fileset`.

        """
        self.db = scan.db
        self.scan = scan
        self.fileset_id = fileset_id

    def create(self):
        """Creates a `Fileset` using the `romidata` FSDB API.

        The name of the created `Fileset` is given by `self.fileset_id`.

        Returns
        -------
        romidata.fsdb.Fileset
            The created `Fileset` instance.

        """
        return self.scan.create_fileset(self.fileset_id)

    def exists(self):
        """Assert the target `Fileset` exists.

        A target exists if the associated fileset exists and is not empty.

        Returns
        -------
        bool
            ``True`` if the target exists, else ``False``.

        """
        fs = self.scan.get_fileset(self.fileset_id)
        return fs is not None and len(fs.get_files()) > 0

    def get(self, create=True):
        """Returns the target `Fileset` instance, can be created.

        Parameters
        ----------
        create : bool
            If ``True`` (default), create the fileset if it does not exist in
             the database.

        Returns
        -------
        romidata.fsdb.Fileset
            The fetched/created `Fileset` instance.

        """
        return self.scan.get_fileset(self.fileset_id, create=create)


class RomiTask(luigi.Task):
    """Implementation of a luigi Task for the romidata DB API.

    Attributes
    ----------
    upstream_task : luigi.TaskParameter
        The upstream task.
    scan_id : luigi.Parameter, optional
        The scan id to use to get or create the FilesetTarget.

    """
    upstream_task = luigi.TaskParameter()
    scan_id = luigi.Parameter(default="")

    def requires(self):
        """Specify dependencies to other Task object.

        This methods will be overridden by the classes inheriting ``RomiTask``.

        Returns
        -------
        luigi.TaskParameter
            The upstream task.

        """
        return self.upstream_task()

    def output(self):
        """Defines the returned Target, for a RomiTask it is a FileSetTarget.

        The fileset ID being the task ID.

        Returns
        -------
        FilesetTarget
            A set of file(s) in the ???

        """
        # Get the `Fileset` id from the `task_id` attribute generated by `luigi.Task`
        # Can be overriding in inheriting class as for the `Vizualization` task
        # This will be usead as DIRECTORY NAME!
        fileset_id = self.task_id
        if self.scan_id == "":
            t = FilesetTarget(DatabaseConfig().scan, fileset_id)
        else:
            t = FilesetTarget(db.get_scan(self.scan_id), fileset_id)
        fs = t.get()
        params = dict(
            self.to_str_params(only_significant=False, only_public=False))
        for k in params.keys():
            try:
                params[k] = json.loads(params[k])
            except:
                continue
        fs.set_metadata("task_params", params)
        return t

    def input_file(self, file_id=None):
        """Helper function to get a file from the input fileset.

        Parameters
        ----------
        file_id : str
            Id of the input file

        Returns
        -------
        db.File

        """
        return self.upstream_task().output_file(file_id, False)

    def output_file(self, file_id=None, create=True):
        """Helper function to get a file from the output  fileset.

        Parameters
        ----------
        file_id : str
            Id of the input file

        Returns
        -------
        db.File

        """
        if file_id is None:
            file_id = self.get_task_name()
        return self.output().get().get_file(file_id, create)

    def get_task_name(self):
        return self.get_task_family().split('.')[-1]

class FilesetExists(RomiTask):
    """A Task which requires a fileset with a given id to exist."""
    fileset_id = luigi.Parameter()
    upstream_task = None

    def requires(self):
        return []

    def output(self):
        self.task_id = self.fileset_id
        return super().output()

    def run(self):
        if self.output().get() is None:
            raise OSError("Fileset %s does not exist" % self.fileset_id)

class ImagesFilesetExists(FilesetExists):
    """A Task which requires the presence of a fileset with id ``images``."""
    fileset_id = luigi.Parameter(default="images")


class FileByFileTask(RomiTask):
    """This abstract class is a Task which take every file from a fileset
    and applies some function to it and saves it back
    to the target.
    """
    query = luigi.DictParameter(default={})
    type = None

    reader = None
    writer = None

    def f(self, f, outfs):
        """Function applied to every file in the fileset must return a file object.

        Parameters
        ----------
        f: FSDB.File
            Input file
        outfs: FSDB.Fileset
            Output fileset

        Returns
        -------
        FSDB.File: this file must be created in outfs
        """
        raise NotImplementedError

    def run(self):
        """Run the task on every file in the fileset.
        """
        input_fileset = self.input().get()
        output_fileset = self.output().get()
        for fi in input_fileset.get_files(query=self.query):
            outfi = self.f(fi, output_fileset)
            if outfi is not None:
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


class Clean(RomiTask):
    """Cleanup a scan, keeping only the "images" fileset and removing all computed pipelines.

    Module: romiscan.tasks.scan
    Default upstream tasks: None

    Parameters
    ----------
    no_confirm : BoolParameter, default=False
        Do not ask for confirmation in the command prompt.

    """
    no_confirm = luigi.BoolParameter(default=False)
    upstream_task = None

    def requires(self):
        return []

    def complete(self):
        return False

    def confirm(self, c, default='n'):
        valid = {"yes": True, "y": True, "ye": True,
                 "no": False, "n": False}
        if c == '':
            return valid[default]
        else:
            return valid[c]

    def run(self):
        if not self.no_confirm:
            del_msg = "This is going to delete all filesets except the scan fileset (images)."
            confirm_msg = "Confirm? [y/N]"
            logger.critical(del_msg)
            logger.critical(confirm_msg)
            choice = self.confirm(input().lower())
        else:
            choice = True

        if not choice:
            logger.warning("Did not validate deletion.")
        else:
            scan = DatabaseConfig().scan
            logger.info(f"Cleaner got a scan named '{scan.id}'...")
            fs_ids = [fs.id for fs in scan.get_filesets()]
            logger.info(f"Found {len(fs_ids)} Filesets...")
            for fs in fs_ids:
                if fs != "images":
                    logger.warning(f"Deleting {fs} fileset...")
                    scan.delete_fileset(fs)
