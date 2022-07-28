#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

opts = dict(
    name="plantdb",
    version="0.12",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=False,
    scripts=[
        'bin/romi_fsdb_sync',
        'bin/romi_import_folder',
        'bin/romi_import_file',
        'bin/romi_scanner_rest_api'
    ],
    description='Database package for the ROMI project.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://docs.romi-project.eu/Scanner/",
    package_data ={"tests": ["test_fsdb.py", "test_io.py", "test_sync.py"]},
    zip_safe=False,
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)"
    ],
)

if __name__ == '__main__':
    setup(**opts)
