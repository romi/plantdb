from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

opts = dict(
    name="plantdb",
    packages=find_packages(),
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
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    install_requires=[
        'appdirs',
        'dirsync',
        'flask',
        'flask-cors',
        'flask-restful',
        'imageio',
        'luigi',
        'numpy',
        'pillow',
        'toml',
        'watchdog'
    ],
    python_requires='>=3.6',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)"
    ],
    # If True, include any files specified by your MANIFEST.in:
    include_package_data=False
)

if __name__ == '__main__':
    setup(**opts)
