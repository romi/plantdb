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
        if not isinstance(tasks, (list, tuple)):
            tasks = [tasks]
        self.db = db
        self.tasks = tasks
        luigi_config = luigi.configuration.get_config()
        luigi_config.read_dict(config)

    def _run_scan_connected(self, scan):
        db_config = {}
        db_config['worker'] = {
            "no_install_shutdown_handler" : True
        }
        db_config['DatabaseConfig'] = {
            'db' : self.db,
            'scan_id' : scan
        }
        luigi_config = luigi.configuration.get_config()
        luigi_config.read_dict(db_config)
        tasks = [t() for t in self.tasks]
        luigi.build(tasks=tasks,
                    local_scheduler=True)

    def run_scan(self, scan_id):
        """Run the tasks on a single scan.

        Parameters
        __________
        scan_id : str
            id of the scan to process
        """
        self.db.connect()
        scan = self.db.get_scan(scan_id)
        self._run_scan_connected(scan)
        self.db.disconnect()

    def run(self):
        """Run the tasks on all scans in the DB
        """
        self.db.connect()
        for scan in self.db.get_scans():
            print("scan = %s"%scan.id)
            self._run_scan_connected(scan)
        print("done")
        self.db.disconnect()
