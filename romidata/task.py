import luigi

class DatabaseConfig(luigi.Config):
    """Configuration for the database."""
    db = luigi.Parameter()
    scan_id = luigi.Parameter()

class FilesetTarget(luigi.Target):
    """Implementation of a luigi Target for the romidata DB API.

    Parameters
    __________

        db : romiscan.db.DB
            database object
        scan_id : str
            id of the scan where the fileset is located
        fileset_id : str
            id of the target fileset
    """
    def __init__(self, db, scan_id, fileset_id):
        self.db = db
        db.connect()
        scan = db.get_scan(scan_id)
        if scan is None:
            raise Exception("Scan does not exist")
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
    def output(self):
        """Output for a RomiTask is a FileSetTarget, the fileset ID being
        the task ID.
        """
        fileset_id = self.task_id
        return FilesetTarget(DatabaseConfig().db, DatabaseConfig().scan_id, fileset_id)

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
