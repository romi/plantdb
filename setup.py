import os
from setuptools import setup, find_packages

NAME = "romidata"
DESCRIPTION = "Data handling tools for the ROMI project"
LICENSE = "Gnu LGPL"

opts = dict(name=NAME,
            description=DESCRIPTION,
            license=LICENSE,
            classifiers=CLASSIFIERS,
            platforms=PLATFORMS,
            version=VERSION,
            packages=find_packages(),
            install_requires=[
                "json",
                "toml"
            ],
            )

if __name__ == '__main__':
    setup(**opts)
