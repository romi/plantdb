import logging
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler, DirCreatedEvent

from romidata.db import DBBusyError
from romidata.runner import DBRunner

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
