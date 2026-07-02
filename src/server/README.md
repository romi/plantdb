# [![ROMI_logo](https://github.com/romi/plantdb/blob/ab29155f0bc0ad755c3455c4db3eb56c4bbd1b0e/docs/assets/images/ROMI_logo_green_25.svg)](https://romi-project.eu) / PlantDB.server

[![Licence](https://img.shields.io/github/license/romi/plantdb?color=lightgray)](https://www.gnu.org/licenses/lgpl-3.0.en.html)
[![Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fromi%2Fplantdb%2Frefs%2Fheads%2Fdev%2Fsrc%2Fcommons%2Fpyproject.toml&logo=python&logoColor=white)]()
[![GitHub branch check runs](https://img.shields.io/github/check-runs/romi/plantdb/dev)](https://github.com/romi/plantdb)
[![PyPI - Version](https://img.shields.io/pypi/v/plantdb.server?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.server/)

Server-side component of the ROMI plant database system.

Provides a robust REST API server implementation for managing plant phenotyping data.

Features include:

- File system database management
- Data synchronization services
- Command-line tools for database management

## Overview

PlantDB is a library for the ROMI (Robotics for Microfarms) plant database ecosystem.
It is designed for plant and agricultural research facilities and robotics labs that require lightweight plant data management infrastructure.

It consists of three components:

1. `plantdb.commons`: provides a **Python API** for interacting with plant data
2. `plantdb.server`: provides the _server-side_ REST API to interact with plant data
3. `plantdb.client`: provides the _client-side_ REST API to interact with plant data

For comprehensive documentation of the _PlantImager_ project, visit: [https://docs.romi-project.eu/plant_imager/](https://docs.romi-project.eu/plant_imager/)

API documentation for the `plantdb` library is available at: [https://romi.github.io/plantdb/](https://romi.github.io/plantdb/)

## Environment Setup

We strongly recommend using isolated environments to install ROMI libraries.

### Python venv

To create a new Python virtual environment for PlantDB:

```shell
python -m venv .venv         # create a pyvenv named `.venv` in the current directory
source .venv/bin/activate    # activate the virtual environment
pip install ipython          # optional interactive workbench
```

### Conda

This documentation uses `conda` as both an environment and package manager.
If you don't have `miniconda3` installed, please refer to the [official documentation](https://docs.conda.io/en/latest/miniconda.html).

To create a new conda environment for PlantDB:

``` shell
conda create -n plantdb 'python=3.10' ipython
```

To use it, you need to activate it with:

```shell
conda activate plantdb  # activate your environment
```

## Installation

### User - Pre Built Packages

Activate your environment and install the packages using `pip`:

``` shell
pip install plantdb.commons plantdb.server plantdb.client
```

### Developers - From Sources

To contribute to the development, you will need to install the sources:

```shell
git clone https://github.com/romi/plantdb.git
cd plantdb
# Install 'plantdb.commons'...
pip install -e src/commons/.[io]
# Install 'plantdb.server'...
pip install -e src/server/.
# Install 'plantdb.client'...
pip install -e src/client/
```

## Usage

### Test with Toy Dataset

To run the server with a temporary test database in debug mode:

```shell
fsdb_rest_api --test --debug
```

### Development

To start the REST API server for a local plant database:

```shell
fsdb_rest_api -db /path/to/your/database --host 127.0.0.1 --port 8080
```

### Production

To start the REST API server in production:

```shell
uwsgi --http :5000 --module plantdb.server.cli.wsgi:application --callable application --master
```

For detailed usage instructions and a full endpoint reference, see:
 - [How to Run the REST API](https://romi.github.io/plantdb/site/rest_api/rest_api_usage/)
 - [REST API Endpoints](https://romi.github.io/plantdb/site/rest_api/rest_api_endpoints/)
