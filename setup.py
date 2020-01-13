import os
from setuptools import setup, find_packages
import subprocess


label = subprocess.check_output(["git", "describe"]).decode().strip()

opts = dict(name="romidata",
            packages=find_packages(),
            scripts=['bin/fsdb-sync'],
            setup_requires=['setuptools_scm'],
            use_scm_version=True
            )

if __name__ == '__main__':
    setup(**opts)
