import json

import luigi
from romidata import FilesetTarget, DatabaseConfig
from romidata.log import logger
from romidata.task import db


class RomiTask(luigi.Task):
    """Implementation of a luigi Task for the romidata DB API.

    Attributes
    ----------
    upstream_task : luigi.TaskParameter
        The upstream task.
    output_file_id : luigi.Parameter, optional
        The output fileset name, default is 'out'.
    scan_id : luigi.Parameter, optional
        The scan id to use to get or create the FilesetTarget.

    """
    upstream_task = luigi.TaskParameter()
    output_file_id = luigi.Parameter(default="out")
    scan_id = luigi.Parameter(default="")

    def requires(self):
        return self.upstream_task()

    def output(self):
        """Output for a RomiTask is a FileSetTarget, the fileset ID being
        The task ID.
        """
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
        return self.upstream_task().output_file(file_id)

    def output_file(self, file_id=None):
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
            file_id = self.get_task_family().split('.')[-1]
        return self.output().get().get_file(file_id, create=True)


class FilesetExists(RomiTask):
    """A Task which requires a fileset with a given
    id to exist.
    """
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
    """A Task which requires the presence of a fileset with id ``images``
    """
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
            fs_ids = [fs.id for fs in scan.get_filesets()]
            for fs in fs_ids:
                if fs != "images":
                    scan.delete_fileset(fs)
