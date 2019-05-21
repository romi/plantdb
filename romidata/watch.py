import time

from watchdog.observers import Observer

from romidata.db import DBBusyError
from romidata.runner import DBRunner

class FSDBWatcher():
    """Class for watching changes on a FSDB database and launching a task when it has changed.


    Attributes
    __________
    db : FSDB
        the target database
    tasks : list of RomiTask
        the list of tasks to do on change
    config : dict
        configuration for the task
    observer : Observer
        watchdog observer for the filesystem
    """
    def __init__(self, db, tasks, config):
        """Parameters
        __________
        db : FSDB
            the target database
        tasks : list of RomiTask
            the list of tasks to do on change
        config : dict
            configuration for the task
        """
        self.db = db
        self.tasks = tasks
        self.config = config
        self.runner = DBRunner(db, tasks, config)
        self.observer = Observer()
        self.observer.schedule(self.run, path, recursive=True)


    def start(self):
        """Start watching the database.
        """
        self.observer.start()

    def stop(self):
        """Stop watching the database.
        """
        self.observer.join()
        self.observer.stop()

    def run(self):
        """Run tasks on the database when it becomes available.
        """
        if self.running:
            return
        self.running = True
        while True:
            try:
                self.runner.run()
                break
            except DBBusyError:
                print("DB Busy, waiting for it to be available...")
                time.sleep(1)
                continue
        self.running = False

