import os
from setuptools import setup, find_packages

NAME = "romidata"
DESCRIPTION = "Data handling tools for the ROMI project"
LICENSE = "Gnu LGPL"
VERSION = "0.5dev0"

opts = dict(name=NAME,
            description=DESCRIPTION,
            license=LICENSE,
            version=VERSION,
            packages=find_packages(),
            install_requires=[
                "toml",
                "luigi",
                "numpy",
                "imageio",
                "dirsync",
                "watchdog"
            ],
            )

if __name__ == '__main__':
    setup(**opts)
