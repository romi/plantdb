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
   ```bash
   conda activate romidata_dev
   conda install nose
   nosetests tests -x -s -v -I test_watch.py -I test_io.py -I test_fsdb.py
   ```

**Notes**:
> Previous tests `test_watch.py`, `test_io.py` & `test_fsdb.py` appears to be broken!
>
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