import luigi

class DBRunner(object):
    """Class for running a given task on a database using luigi.

    Attributes
    __________
    db : DB
        target database
    """

    def __init__(self, db, tasks, config):
        """
        Parameters
        __________
        db : DB
            target database
        tasks : list or RomiTask
            tasks
        config : dict
            luigi configuration for tasks
        """
        if not isinstance(lst, (list, tuple))
            tasks = [tasks]
        self.db = db
        self.tasks = tasks
        luigi_config = luigi.configuration.get_config()
        luigi_config.read_dict(config)

    def run_scan(self, scan_id):
        """Run the tasks on a single scan.

        Parameters
        __________
        scan_id : str
            id of the scan to process
        """
        self.db.connect()
        scan = self.db.get_scan(scan_id)
        db_config = {}
        db_config['DatabaseConfig'] = {
            'db' : db,
            'scan_id' : scan
        }
        luigi_config = luigi.configuration.get_config()
        luigi_config.read_dict(db_config)
        luigi.build(tasks=self.tasks,
                    local_scheduler=True)
        self.db.disconnect()

    def run(self):
        """Run the tasks on all scans in the DB
        """
        self.db.connect()
        for scan in self.db.get_scans():
            self.run_scan(scan.id)
        self.db.disconnect()
