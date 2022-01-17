# PlantDB

Documentation about the "Plant Scanner" project can be found [here](https://docs.romi-project.eu/Scanner/home/).

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
docker run -v 
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

1. Clone the sources:
    ```shell
    git clone https://github.com/romi/plantdb.git
    ```
2. Create a conda environment:
    ```shell
    cd plantdb/
    conda env create -f conda/env/plantdb.yaml
    ```
3. Install sources:
    ```shell
    conda activate plantdb
    python3 -m pip install -e .
    ```
4. Test import of `plantdb` library:
    ```shell
    conda activate plantdb
    python3 -c 'import plantdb'
    ```
5. Test `plantdb` library:
    Install dependencies for testing and coverage:
    ```shell
    conda activate plantdb
    conda install nose coverage
    ```
    Run all tests with highly verbose output (from the root directory):
    ```shell
    nosetests tests/ -x -s -v
    ```
    Run all tests with coverage report (from the root directory):
    ```shell
    nosetests tests/ --with-coverage --cover-package=plantdb
    ```

**Notes**:
> You may change the name of the conda environment (`-c` option) to something else, like an already existing conda environment (then skip step 2.).


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

### Conda packaging
To build `plantdb` conda package, from the `base` conda environment, run:
```shell
conda build conda_recipes/plantdb/ -c romi-eu -c open3d-admin --user romi-eu
```
**NOT DONE YET!**


### Docker `build.sh` & `run.sh` scripts
To facilitate the use of docker, we created two scripts, `build.sh` & `run.sh`, located in the `docker` folder in the sources.

To build a new `roboticsmicrofarms/plantdb` image, from the root folder, simply do:
```shell
./docker/build.sh
```

To run a `roboticsmicrofarms/plantdb` image, from the root folder, simply do:
```shell
./docker/run.sh
```

:::{info}
Use the `-h` option to get help on using the scripts.
:::