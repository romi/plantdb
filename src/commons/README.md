# PlantDB

[![Licence](https://img.shields.io/badge/license-LGPL3-black)](https://www.gnu.org/licenses/lgpl-3.0.en.html)
[![PyPI - Version](https://img.shields.io/pypi/v/plantdb.commons?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.commons/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/plantdb.commons?logo=python&logoColor=white)](https://pypi.org/project/plantdb.commons/)

Core shared library for the ROMI (Robotics for Microfarms) plant database ecosystem.

This package provides common utilities and base functionality used by both server and client components.

Features include:
- Data management
- Common data models and schemas
- File system operations and validation
- Logging and debugging tools
- Data format specifications and validators

## Overview

PlantDB is a library designed for plant and agricultural research facilities and robotics labs that require lightweight plant data management infrastructure.

It consists of three components:

1. `plantdb.commons`: provides a **Python API** for interacting with plant data
2. `plantdb.server`: provides the _server-side_ REST API to interact with plant data
3. `plantdb.client`: provides the _client-side_ REST API to interact with plant data

For comprehensive documentation of the _PlantImager_ project, visit: [https://docs.romi-project.eu/plant_imager/](https://docs.romi-project.eu/plant_imager/)

API documentation for the `plantdb` library is available at: [https://romi.github.io/plantdb/](https://romi.github.io/plantdb/)

## Environment Setup

We strongly recommend using isolated environments to install ROMI libraries.

This documentation uses `conda` as both an environment and package manager.
If you don't have`miniconda3` installed, please refer to the [official documentation](https://docs.conda.io/en/latest/miniconda.html).

To create a new conda environment for PlantDB:
``` shell
conda create -n plantdb 'python=3.10' ipython
```

## Installation

Activate your environment and install the packages using `pip`:

``` shell
conda activate plantdb  # activate your environment first!
pip install plantdb.commons plantdb.server plantdb.client
```
