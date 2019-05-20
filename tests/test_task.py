import unittest
import tempfile
import os

from romidata import RomiTask, DatabaseConfig, FilesetTarget, FSDB
from romidata.testing import TemporaryCloneDB, DBTestCase

import luigi


class TouchFileTask(RomiTask):
    def requires(self):
        return[]
    def run(self):
        x = self.output().get()
        y = x.create_file("hello")
        y.write_text("txt", "hello")


class TestFilesetTarget(DBTestCase):
    def test_target(self):
        db = self.get_test_db()
        target = FilesetTarget(db, "testscan", "test_target")
        assert(target.get(create=False) is None)
        assert(not target.exists())
        target.create()
        assert(target.exists())
        assert(target.get() is not None)

class TestRomiTask(DBTestCase):
   def test_romi_task(self):
        db = self.get_test_db()
        DatabaseConfig.db = db
        DatabaseConfig.scan_id = "testscan"
        task = TouchFileTask()
        assert(not task.complete())
        luigi.build(tasks=[task], local_scheduler=True)
        assert(task.complete())

if __name__ == "__main__":
    unittest.main()



