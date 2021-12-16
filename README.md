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

### USER - Install `plantdb` conda package
In an active conda envirnoment of your choosing, here named `<my_env>` for clarity, you can install the `plantdb` conda package with:
```shell
conda activate <my_env>
conda install plantdb -c romi-eu
```
To test package install, in the activated `<my_env>` environment, import `plantdb` in python:
```shell
python -c 'from plantdb import FSDB'
```

### DEVELOPER - Build `plantdb` conda package
From the `base` conda environment, run:
```shell
conda-build ./conda/recipe/ --channel conda-forge --user romi-eu  #  -c open3d-admin
```

## Usage
This library is intended to run in the background as a REST API serving JSON informations from the DB.
Typically, these are used by the `3d-plantviewer` or `romiscan` libraries.

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


## Minimal Python API

Here is a minimal example how to access DB in Python.
It assumes you got the example dataset, extracted it and that you defined its location under the environment variable `DB_LOCATION`:
```python
# Get the environment variable $DB_LOCATION
import os
db_path = os.environ['DB_LOCATION']
# Use it to connect to DB:
from plantdb import FSDB
db = FSDB(db_path)
db.connect()

# List the available scans:
scan_names = [scan.id for scan in db.get_scans()]
print(scan_names)
# Access the example scan named '2018-12-17_17-05-35':
dataset = db.get_scan("2018-12-17_17-05-35")

# Access the 'images' fileset in this `Scan`:
img_fs = dataset.get_fileset('images')
# List the available files in this `Fileset`:
file_names = [f.id for f in img_fs.get_files()]
print(file_names)
```