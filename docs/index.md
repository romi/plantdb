# Welcome to PlantDB

[![Licence](https://img.shields.io/badge/license-LGPL3-black)](https://www.gnu.org/licenses/lgpl-3.0.en.html)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/plantdb.commons?logo=python&logoColor=white)](https://pypi.org/project/plantdb.commons/)

| Package         | PyPI                                                                                                                                     | Conda                                                                                                                                                                                                     |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| plantdb.commons | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.commons?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.commons/) | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.commons?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.commons&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.commons)  |
| plantdb.server  | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.server?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.server/)    | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.server?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.server&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.server)     |
| plantdb.client  | [![PyPI - Version](https://img.shields.io/pypi/v/plantdb.client?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.client/)    | [![Conda - Version](https://img.shields.io/conda/vn/mosaic/plantdb.client?logo=anaconda&logoColor=white&label=romi-eu%2Fplantdb.client&color=%2344A833)](https://anaconda.org/romi-eu/plantdb.client)     |

![ROMI_ICON2_greenB.png](assets/images/ROMI_ICON2_greenB.png)

## Overview

PlantDB is a library for the ROMI (Robotics for Microfarms) plant database ecosystem.
It is designed for plant and agricultural research facilities and robotics labs that require lightweight plant data management infrastructure.

It consists of three components:

1. `plantdb.commons`: provides a **Python API** for interacting with plant data
2. `plantdb.server`: provides the _server-side_ REST API to interact with plant data
3. `plantdb.client`: provides the _client-side_ REST API to interact with plant data

For comprehensive documentation of the _PlantImager_ project, visit: [https://docs.romi-project.eu/plant_imager/](https://docs.romi-project.eu/plant_imager/)

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

