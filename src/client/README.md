# PlantDB.Client

[![Licence](https://img.shields.io/badge/license-LGPL3-black)](https://www.gnu.org/licenses/lgpl-3.0.en.html)
[![PyPI - Version](https://img.shields.io/pypi/v/plantdb.client?logo=pypi&logoColor=white)](https://pypi.org/project/plantdb.client/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/plantdb.client?logo=python&logoColor=white)](https://pypi.org/project/plantdb.client/)

Client-side database library for the ROMI (Robotics for Microfarms) plant database ecosystem.

This package provides a Python interface for interacting with ROMI's plant database system,
enabling efficient storage, retrieval, and management of plant-related data.

Features include:
- REST API integration
- Data validation
- Streamlined access to plant phenotyping data.

## Overview

PlantDB is a library designed for plant and agricultural research facilities and robotics labs requiring lightweight plant data management infrastructure.

It has three components:

1. `plantdb.commons`: provides a **Python API** for interacting with plant data
2. `plantdb.server`: provides the _server-side_ REST API to interact with plant data
3. `plantdb.client`: provides the _client-side_ REST API to interact with plant data

For comprehensive documentation of the _PlantImager_ project, visit: [https://docs.romi-project.eu/plant_imager/](https://docs.romi-project.eu/plant_imager/)

API documentation for the `plantdb` library is available at: [https://romi.github.io/plantdb/](https://romi.github.io/plantdb/)

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

Activate your environment and install the packages using either `pip` or `conda`:

### Using pip:
``` shell
conda activate plantdb  # activate your environment first!
pip install plantdb.commons plantdb.server plantdb.client
```

### Using conda:
``` shell
conda activate plantdb  # activate your environment first!
conda install -c romi-eu plantdb plantdb.server plantdb.client
```
