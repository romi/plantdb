import os
from setuptools import setup, find_packages
import subprocess

opts = dict(name="romidata",
            packages=find_packages(),
            scripts=['bin/romi_fsdb_sync', 'bin/romi_import_folder', 'bin/romi_run_task', 'bin/romi_import_folder', 'bin/romi_import_file'],
            setup_requires=['setuptools_scm'],
            use_scm_version=True,
            install_requires=[
                'luigi',
                'toml'
            ]
            )

if __name__ == '__main__':
    setup(**opts)
