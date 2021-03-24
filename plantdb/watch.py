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

import time

from plantdb.db import DBBusyError
from plantdb.runner import DBRunner
from watchdog.events import FileSystemEventHandler, DirCreatedEvent
from watchdog.observers import Observer


# logging.basicConfig(level=logging.INFO,
#     format='%(asctime)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S')

class FSDBWatcher():
    """Class for watching changes on a FSDB database and launching a task when it has changed.


    Attributes
    ----------
    observer : Observer
        Watchdog observer for the filesystem
    """
    def __init__(self, db, tasks, config):
        """Parameters
        ----------
        db : FSDB
            The target database
        tasks : list of RomiTask
            The list of tasks to do on change
        config : dict
            Configuration for the task
        """
        self.observer = Observer()
        handler = FSDBEventHandler(db, tasks, config)
        self.observer.schedule(handler, db.basedir, recursive=False)

    def start(self):
        self.observer.start()

    def stop(self):
        self.observer.stop()

    def join(self):
        self.observer.join()


class FSDBEventHandler(FileSystemEventHandler):
    def __init__(self, db, tasks, config):
        """Parameters
        ----------
        db : FSDB
            The target database
        tasks : list of RomiTask
            The list of tasks to do on change
        config : dict
            Configuration for the task
        """
        self.runner = DBRunner(db, tasks, config)
        self.running = False

    def on_created(self, event):
        """Run tasks on the database when it becomes available, if a new
        folder has been created (new scan)
        """
        if not isinstance(event, DirCreatedEvent):
            return
        while True:
            try:
                self.runner.run()
                self.running = False
                return
            except DBBusyError:
                print("DB Busy, waiting for it to be available...")
                time.sleep(1)
                continue
