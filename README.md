# PlantDB

Documentation about the "Plant Imager" project can be found [here](https://docs.romi-project.eu/plant_imager/home/).

## Getting started

This library is intended to run in the background as a REST API serving JSON information from the DB.
Typically, these are used by the `plant-3d-explorer` or `plant-3d-vision` libraries.

### Setup

You need to create a directory where to put the data, *e.g.* `/data/ROMI/DB` and add a file called `romidb`define it in an environment variable `DB_LOCATION`:

```shell
mkdir -p /data/ROMI/DB
touch /data/ROMI/DB/romidb
```

Then define its location in an environment variable `DB_LOCATION`:

```shell
export DB_LOCATION=/data/ROMI/DB
```

**Notes**:
> To permanently set this directory as the location of the DB, add it to your `~/.bashrc` file.

### Example datasets

To populate your database with example datasets, we provide some examples [here](https://media.romi-project.eu/data/test_db_small.tar.gz).

Make sure you have `wget`:

```shell
sudo apt-get install wget
```

Then download the test archive and extract it to the location od the DB:

```shell
wget https://media.romi-project.eu/data/test_db_small.tar.gz
tar -xf test_db_small.tar.gz -C $DB_LOCATION
```

### Docker image

A docker image, named `roboticsmicrofarms/plantdb`, is distributed by the ROMI group.
If you want to use it, simply do:

```shell
docker run -p 5000:5000 -v $DB_LOCATION:/myapp/db -it roboticsmicrofarms/plantdb
```

**Obviously you have to install docker first!**

## Installation

### Conda package

To install the `plantdb` conda package, simply do:

```shell
conda create -n plantdb plantdb -c romi-eu -c open3d-admin --force
```

To test package install, in the activated environment import `plantdb` in python:

```shell
conda activate plantdb
python -c 'import plantdb'
```

**Obviously you have to install anaconda/miniconda first!**

### From sources

#### System requirements

You will need:

- `git`
- `python3-pip`

On Debian and Ubuntu, you can install them with:

```shell
sudo apt-get update && apt-get install -y git python3-pip
```

#### Install sources in a conda environment

You may want to install `poetry` system-wide and skip the optional step.
To do so, have a look at the official [installation](https://python-poetry.org/docs/#installation) page.

Let's assume you want to create a new `romi` conda environment (if not replace `romi` by the desired name for the conda environment):

1. Clone the sources:
    ```shell
    git clone https://github.com/romi/plantdb.git
    cd plantdb/  # move to the cloned folder
    ```
2. Create a conda environment named `romi` (or use your own and skip this step):
    ```shell
    conda create -n romi python=3.7
    conda activate romi
    ```
3. OPTIONAL - Install poetry with `pip` if not available system-wide:
    ```shell
    python3 -m pip install poetry
    ```
4. Install sources (add `--no-dev` if you don't want the tools to build the documentation):
    ```shell
    poetry install
    ```
5. Test import of `plantdb` library:
    ```shell
    conda activate romi
    python3 -c 'import plantdb'
    ```
6. Test `plantdb` library:
   Run all tests with verbose output (from the `plantdb` root directory):
    ```shell
    nose2 -s tests/ -v
    ```
   Run all tests with coverage report (from the `plantdb` root directory):
    ```shell
    nose2 -s tests/ --with-coverage
    ```

To manually install tests tools:

```shell
poetry add --dev nose2[coverage] coverage[toml]
```

## Serve the REST API

Then you can start the REST API with `romi_scanner_rest_api`:

```shell
romi_scanner_rest_api
```

You should see something like:

```
n scans = 2
 * Serving Flask app "romi_scanner_rest_api" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://0.0.0.0:5000/ (Press CTRL+C to quit)
```

Open your favorite browser here:

- scans: http://0.0.0.0:5000/scans
- '2018-12-17_17-05-35' dataset: http://0.0.0.0:5000/scans/2018-12-17_17-05-35

## Python API

Here is a minimal example how to use the `plantdb` library in Python:

```python
# Get the environment variable $DB_LOCATION
import os

db_path = os.environ['DB_LOCATION']
# Use it to connect to DB:
from plantdb import FSDB

db = FSDB(db_path)
db.connect()
# Access to a dataset named `2018-12-17_17-05-35` (from the example database)
dataset = db.get_scan("2018-12-17_17-05-35")
# Get the 'images' fileset contained in this dataset
img_fs = dataset.get_fileset('images')
```

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

### Conda packaging

To build `plantdb` conda package, from the `base` conda environment, run:

```shell
conda build conda/recipe/ -c conda-forge --user romi-eu
```

This requires the `conda-build` package to be installed in the `base` environment!

```shell
conda install conda-build
```

To upload the built package, you need a valid account (here `romi-eu`) on [anaconda.org](www.anaconda.org) & to log ONCE
with `anaconda login`, then:

```shell
anaconda upload ~/miniconda3/conda-bld/linux-64/plantdb*.tar.bz2 --user romi-eu
```

This requires the `anaconda-client` package to be installed in the `base` environment!

```shell
conda install anaconda-client
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
