#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

opts = dict(
    name="plantdb",
    version="0.12.99",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=False,
    scripts=[
        'bin/fsdb_check',
        'bin/fsdb_sync',
        'bin/fsdb_import_images',
        'bin/fsdb_import_folder',
        'bin/fsdb_import_file',
        'bin/fsdb_rest_api'
    ],
    description='Database package for the ROMI project.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://docs.romi-project.eu/plant_imager/",
    package_data={"tests":
                      ["test_fsdb.py", "test_io.py", "test_romi_import_images.py", "test_sync.py", "test_webcache.py"]
                  },
    zip_safe=False,
    python_requires='>=3.8',
    install_requires=[
        'appdirs',
        'colorlog',
        'flask',
        'flask-cors',
        'flask-restful',
        'imagecodecs',
        'imageio>=2.16.2',
        'numpy',
        'open3d>=0.9.0.0',
        'pillow',
        'toml',
        'tqdm',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)"
    ],
)

if __name__ == '__main__':
    setup(**opts)
