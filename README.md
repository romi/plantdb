# romidata

Documentation about the "Plant Scanner" project can be found [here](https://docs.romi-project.eu/Scanner/home/).

## System requirements
You will need:

- git
- python3-pip


On Debian and Ubuntu, you can install them with:
```bash
sudo apt-get update && apt-get install -y git python3-pip
```


## Install from sources in conda environment:

1. Clone the sources:
    ```bash
    git clone https://github.com/romi/romidata.git
    ```
2. Create a conda environment:
    ```bash
    conda create --name romidata_dev python=3.7
    ```
3. Install sources:
   ```bash
   conda activate romidata_dev
   cd romidata
   python3 -m pip install -e .
   ```
4. Test import of `romidata` library:
    ```bash
    conda activate romidata_dev
    python3 -c 'import romidata'
    ```
5. Test `romidata` library:

   Install dependencies for testing and coverage:
   ```bash
   conda activate romidata_dev
   conda install nose coverage
   ```
   Run all tests with highly verbose output (from the root directory):
   ```bash
   nosetests tests/ -x -s -v
   ```
   Run all tests with coverage report (from the root directory):
   ```bash
   nosetests tests/ --with-coverage --cover-package=romidata

   ```

**Notes**:
> You may change the name `romidata_dev` to something else, like an already existing conda environment (then skip step 2.).

## Conda packaging
**Notes**:
> This is not working YET!

### Install `romidata` conda package:
```bash
conda create -n romidata romidata -c romi-eu -c open3d-admin --force
```
To test package install, in the activated environment import `romidata` in python:
```bash
conda activate romidata
python -c 'import romidata'
```

### Build `romidata` conda package:
From the `base` conda environment, run:
```bash
conda build conda_recipes/romidata/ -c romi-eu -c open3d-admin --user romi-eu
```

## Usage
This library is intended to run in the background as a REST API serving JSON informations from the DB.
Typically these are used by the `3d-plantviewer` or `romiscan` libraries.

### Setup
You need to create a directory where to put the data, *e.g.* `/data/ROMI/DB` and add a file called `romidb`define it in an environment variable `DB_LOCATION`:
```bash
mkdir -p /data/ROMI/DB
touch /data/ROMI/DB/romidb
```
Then define its location in an environment variable `DB_LOCATION`:
```bash
export DB_LOCATION=/data/ROMI/DB
```
**Notes**:
> To permanently set this directory as the location of the DB, add it to your `~/.bashrc` file. 


### Example datasets
To populate your DB with example datasets, we provide some examples [here](https://media.romi-project.eu/data/test_db_small.tar.gz).

Make sure you have `wget`:
```bash
sudo apt-get install wget
```
Then download the test archive and extract it to the location od the DB:
```bash
wget https://media.romi-project.eu/data/test_db_small.tar.gz
tar -xf test_db_small.tar.gz -C $DB_LOCATION
```

### Serve the REST API
Then you can start the REST API with `romi_scanner_rest_api`:
```bash
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
from romidata import FSDB
db = FSDB(db_path)
db.connect()
dataset = db.get_scan("2018-12-17_17-05-35")
img_fs = dataset.get_fileset('images')
```