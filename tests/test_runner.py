import unittest
import tempfile
import time
import os

from romidata import FSDB
from romidata.runner import DBRunner
from romidata.testing import DBTestCase

import luigi


class TestFSDBRunner(DBTestCase):
    def test_run_scan(self):
        db = self.get_test_db()
        runner = DBRunner(db, [], {})
        runner.run_scan("testscan")

    def test_run(self):
        db = self.get_test_db()
        runner = DBRunner(db, [], {})
        runner.run()

if __name__ == "__main__":
    unittest.main()



