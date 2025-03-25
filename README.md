# PlantDB

[![Licence](https://img.shields.io/badge/license-LGPL3-black)](https://www.gnu.org/licenses/lgpl-3.0.en.html)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/plantdb.commons?logo=python&logoColor=white)](https://pypi.org/project/plantdb.commons/)

| Package         | PyPI                                                                                                                                     | Conda                                                                                                                                                                                                     |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plantdb.commons | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.commons?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.commons/) | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.commons?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.commons&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.commons)  |
| plantdb.server  | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.server?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.server/)    | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.server?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.server&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.server)     |
| plantdb.client  | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.client?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.client/)    | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.client?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.client&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.client)     |

## Overview

PlantDB is a library for the ROMI (Robotics for Microfarms) plant database ecosystem.
It is designed for plant and agricultural research facilities and robotics labs that require lightweight plant data management infrastructure.

It consists of three components:

1. `plantdb.commons`: provides a **Python API** for interacting with plant data
2. `plantdb.server`: provides the _server-side_ REST API to interact with plant data
3. `plantdb.client`: provides the _client-side_ REST API to interact with plant data

For comprehensive documentation of the _PlantImager_ project, visit: [https://docs.romi-project.eu/plant_imager/](https://docs.romi-project.eu/plant_imager/)

API documentation for the `plantdb` library is available at: [https://romi.github.io/plantdb/](https://romi.github.io/plantdb/)

### `plantdb.commons`
Core shared library for the ROMI plant database ecosystem.

This package provides common utilities and base functionality used by both server and client components.

Features include:
- Data management
- Common data models and schemas
- File system operations and validation
- Logging and debugging tools
- Data format specifications and validators

### `plantdb.server`
Server-side component of the ROMI plant database system.

Provides a robust REST API server implementation for managing plant phenotyping data.

Features include:
- File system database management
- Data synchronization services
- Command-line tools for database management

### `plantdb.client`
Client-side database library for the ROMI plant database ecosystem.

This package provides a Python interface for interacting with ROMI's plant database system,
enabling efficient storage, retrieval, and management of plant-related data.

Features include:
- REST API integration
- Data validation
- Streamlined access to plant phenotyping data.


## Environment Setup

We strongly recommend using isolated environments to install ROMI libraries.
This documentation uses `conda` as both an environment and package manager.
If you don't have`miniconda3` installed, please refer to the [official documentation](https://docs.conda.io/en/latest/miniconda.html).

The `plantdb` packages are available through:

- `pip`: from [PyPI](https://pypi.org/)
- `conda`: from the `romi-eu` channel on [anaconda.org](https://anaconda.org/romi-eu)

To create a new conda environment for PlantDB:
``` shell
conda create -n plantdb 'python=3.10' ipython
```

## Installation

### For Users

Activate your environment and install the packages using either `pip` or `conda`:

#### Using pip:
``` shell
conda activate plantdb  # activate your environment first!
pip install plantdb.commons plantdb.server plantdb.client
```

#### Using conda:
``` shell
conda activate plantdb  # activate your environment first!
conda install -c romi-eu plantdb plantdb.server plantdb.client
```

### For Developers

To install the library for development:

1. Clone the repository:
   ``` shell
   git clone https://github.com/romi/plantdb.git -b dev  # clone the 'dev' branch
   cd plantdb
   conda activate plantdb  # activate your environment first!
   ```

2. Install components:
#### Core library:
``` shell
python -m pip install -e src/commons/.
```

#### Client-side library:
``` shell
python -m pip install -e src/client/.
```

#### Server-side library:
``` shell
python -m pip install -e src/server/.
```

**Info**:
> The `-e` flag installs the source in "developer mode," allowing code changes to take effect without re-installation.


### Running Tests

Install testing tools:
``` shell
python -m pip install -e .[test]
```

Run tests:

- All tests with verbose output:
  ``` shell
  nose2 -s tests/ -v
  ```

- Tests with coverage report:
  ``` shell
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

**Info**:
> To make this setting permanent, add the export command to your `~/.bashrc` or `~/.profile` file.


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
from plantdb.commons.fsdb import FSDB

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

To build the `plantdb` conda packages, from the root directory of the repository and the `base` conda environment, run:

1. Build the packages locally without uploading
    ```shell
    for pkg in commons server client; do
      echo "Building plantdb-$pkg package..."
      export PLANTDB_SUBPACKAGE=$pkg
      conda build conda/recipe/ --no-anaconda-upload
    done
    ```

2. Test installing the built packages
    After building, conda will show the path to the built package. You can install it to verify it works:
    ```shell
    # Create a test environment
    conda create -n plantdb_test python=3.10 -y
    conda activate plantdb_test
    
    # Install the locally built package
    # Replace with actual path from conda build output
    conda install --use-local plantdb-commons
    conda install --use-local plantdb-server
    conda install --use-local plantdb-client
    
    # Test importing the packages
    python -c "import plantdb.commons; print('Successfully imported plantdb.commons')"
    python -c "import plantdb.server; print('Successfully imported plantdb.server')"
    python -c "import plantdb.client; print('Successfully imported plantdb.client')"
    
    # Clean up
    conda deactivate
    conda env remove -n plantdb_test
    ```

**Note:**
If you encounter issues, you can use the --debug flag for more verbose output when building the package.
```shell
conda build conda/recipe/commons --debug --no-anaconda-upload
```

#### Render the recipe
If you are struggling with some of the modifications you made to the recipe,
notably when using environment variables or Jinja2 stuffs, you can always render the recipe with:

```shell
conda render conda/recipe/
```

The official documentation for
`conda-render` can be found [here](https://docs.conda.io/projects/conda-build/en/stable/resources/commands/conda-render.html).

#### Upload a conda package

To upload the built packages, you need a valid account (here
`romi-eu`) on [anaconda.org](www.anaconda.org) & to log ONCE
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
