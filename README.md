# PlantDB

Documentation about the "Plant Scanner" project can be found [here](https://docs.romi-project.eu/Scanner/home/).

## System requirements
You will need:

- git
- python3-pip


On Debian and Ubuntu, you can install them with:
```shell
sudo apt-get update && apt-get install -y git python3-pip
```


## Install from sources in conda environment:

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
> You may change the name `plantdb` to something else, like an already existing conda environment (then skip step 2.).

## Conda packaging
**Notes**:
> This is not working YET!

### Install `plantdb` conda package:
```shell
conda create -n plantdb plantdb -c romi-eu -c open3d-admin --force
```
To test package install, in the activated environment import `plantdb` in python:
```shell
conda activate plantdb
python -c 'import plantdb'
```

### Build `plantdb` conda package:
From the `base` conda environment, run:
```shell
conda build conda_recipes/plantdb/ -c romi-eu -c open3d-admin --user romi-eu
```

## Usage
This library is intended to run in the background as a REST API serving JSON informations from the DB.
Typically these are used by the `3d-plantviewer` or `romiscan` libraries.

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
To populate your DB with example datasets, we provide some examples [here](https://media.romi-project.eu/data/test_db_small.tar.gz).

Make sure you have `wget`:
```shell
sudo apt-get install wget
```
Then download the test archive and extract it to the location od the DB:
```shell
wget https://media.romi-project.eu/data/test_db_small.tar.gz
tar -xf test_db_small.tar.gz -C $DB_LOCATION
```

### Serve the REST API
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

### Python API
Here is a minimal example how to access DB in Python:
```python
# Get the environment variable $DB_LOCATION
import os
db_path = os.environ['DB_LOCATION']
# Use it to connect to DB:
from plantdb import FSDB
db = FSDB(db_path)
db.connect()
dataset = db.get_scan("2018-12-17_17-05-35")
img_fs = dataset.get_fileset('images')
```