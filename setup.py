import os
from setuptools import setup, find_packages
import subprocess

opts = dict(name="romidata",
            packages=find_packages(),
            scripts=['bin/fsdb-sync', 'bin/run-task', 'bin/scanner-rest-api'],
            setup_requires=['setuptools_scm'],
            use_scm_version=True,
            install_requires=[
                'luigi'
            ]
            )

if __name__ == '__main__':
    setup(**opts)
