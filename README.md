# PlantDB

[![Anaconda-Server Badge](https://anaconda.org/romi-eu/plantdb/badges/version.svg)](https://anaconda.org/romi-eu/plantdb)
[![Anaconda-Server Badge](https://anaconda.org/romi-eu/plantdb/badges/platforms.svg)](https://anaconda.org/romi-eu/plantdb)
[![Anaconda-Server Badge](https://anaconda.org/romi-eu/plantdb/badges/license.svg)](https://anaconda.org/romi-eu/plantdb)

The documentation of the _Plant Imager_ project can be found here: https://docs.romi-project.eu/plant_imager/

The API documentation of the `plantdb` library can be found here: https://romi.github.io/plantdb/ 

## About

This library is intended to:

1. provide a **Python API** to interact with the data, as for the `plant-3d-vision` library
2. run in the background as a **REST API** serving JSON information from the DB, as for the `plant-3d-explorer` library

## Installation

We strongly advise to create isolated environments to install the ROMI libraries.

We often use `conda` as an environment and python package manager.
If you do not yet have `miniconda3` installed on your system, have a look [here](https://docs.conda.io/en/latest/miniconda.html).

The `plantdb` package is available from the `romi-eu` channel.

### Existing conda environment
To install the `plantdb` conda package in an existing environment, first activate it, then proceed as follows:
```shell
conda install plantdb -c romi-eu
```

### New conda environment
To install the `plantdb` conda package in a new environment, here named `romi`, proceed as follows:
```shell
conda create -n romi plantdb -c romi-eu
```

### Installation from sources
To install this library, simply clone the repo and use `pip` to install it and the required dependencies.
Again, we strongly advise to create a `conda` environment.

All this can be done as follows:
```shell
git clone https://github.com/romi/plantdb.git -b dev  # git clone the 'dev' branch of plantdb
cd plantdb
conda create -n romi 'python =3.10'
conda activate romi  # do not forget to activate your environment!
python -m pip install -e .  # install the sources
```

Note that the `-e` option is to install the `plantdb` sources in "developer mode".
That is, if you make changes to the source code of `plantdb` you will not have to `pip install` it again.

### Tests the library
First you need to install the tests tools:
```shell
python -m pip install -e .[test]
```

Then, to test the `plantdb` library:
 - Run all tests with verbose output (from the `plantdb` root directory):
    ```shell
    nose2 -s tests/ -v
    ```
 - Run all tests with coverage report (from the `plantdb` root directory):
    ```shell
    nose2 -s tests/ --with-coverage
    ```


## Getting started

### Set up a local database

1. You need to create a directory where to put the data, *e.g.* `/data/ROMI_DB` and add a file called `romidb`:
   ```shell
   mkdir -p /data/ROMI_DB
   touch /data/ROMI_DB/romidb
   ```
2. Then define its location in an environment variable `ROMI_DB`:
   ```shell
   export ROMI_DB=/data/ROMI_DB
   ```

**Notes**:
> To permanently set this directory as the location of the DB, add it to your `~/.bashrc` or `~/.profile` file.

### Example datasets

To populate your database with example datasets, you may use the `shared_fsdb` CLI as follows:
```shell
shared_fsdb $ROMI_DB --dataset all
```

### Docker image

A docker image, named `roboticsmicrofarms/plantdb`, is distributed by the ROMI group.
If you want to use it, simply do:

```shell
docker run -p 5000:5000 -v $ROMI_DB:/myapp/db -it roboticsmicrofarms/plantdb
```

**Obviously you have to install docker first!**


## Usage

### Python API

Here is a minimal example how to use the `plantdb` library in Python:

```python
# Get the environment variable $ROMI_DB
import os

db_path = os.environ['ROMI_DB']
# Use it to connect to DB:
from plantdb import FSDB

db = FSDB(db_path)
db.connect()
# Access to a dataset named `real_plant` (from the example database)
dataset = db.get_scan("real_plant")
# Get the 'images' fileset contained in this dataset
img_fs = dataset.get_fileset('images')
```

A detailed documentation of the Python API is available here: https://romi.github.io/plantdb/reference.html

### Serve the REST API

Then you can start the REST API with `fsdb_rest_api`:

```shell
fsdb_rest_api -db $ROMI_DB
```

You should see something like:
```
n scans = 5
 * Serving Flask app "fsdb_rest_api" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Open your favorite browser here:

- scans: http://0.0.0.0:5000/scans
- 'real_plant_analyzed' dataset: http://0.0.0.0:5000/scans/real_plant_analyzed

A detailed documentation of the REST API is available here: https://romi.github.io/plantdb/webapi.html


## Developers & contributors

### Unitary tests

Some tests are defined in the `tests` directory.
We use `nose2` to call them as follows:

```shell
nose2 -v -C
```

Notes:

- the configuration file used by `nose2` is `unittests.cfg`
- the `-C` option generate a coverage report, as defined by the `.coveragerc` file.
- this requires the `nose2` & `coverage` packages listed in the `requirements.txt` file.

You first have to install the library from sources as explained [here](#installation-from-sources).

### Conda packaging
Start by installing the required `conda-build` & `anaconda-client` conda packages in the `base` environment as follows:
```shell
conda install -n base conda-build anaconda-client
```

#### Build a conda package
To build the `romitask` conda package, from the root directory of the repository and the `base` conda environment, run:
```shell
conda build conda/recipe/ -c conda-forge --user romi-eu
```

If you are struggling with some of the modifications you made to the recipe, 
notably when using environment variables or Jinja2 stuffs, you can always render the recipe with:
```shell
conda render conda/recipe/
```

The official documentation for `conda-render` can be found [here](https://docs.conda.io/projects/conda-build/en/stable/resources/commands/conda-render.html).

#### Upload a conda package
To upload the built packages, you need a valid account (here `romi-eu`) on [anaconda.org](www.anaconda.org) & to log ONCE
with `anaconda login`, then:
```shell
anaconda upload ~/miniconda3/conda-bld/linux-64/plantdb*.tar.bz2 --user romi-eu
```

#### Clean builds
To clean the source and build intermediates:
```shell
conda build purge
```

To clean **ALL** the built packages & build environments:
```shell
conda build purge-all
```

### Docker `build.sh` & `run.sh` scripts

To facilitate the use of docker, we created two scripts, `build.sh` & `run.sh`, located in the `docker` folder in the
sources.

To build a new `roboticsmicrofarms/plantdb` image, from the root folder, simply do:

```shell
./docker/build.sh
```

To run the latest `roboticsmicrofarms/plantdb` image, from the root folder, simply do:

```shell
./docker/run.sh
```

Use the `-h` option to get help on using the scripts.
