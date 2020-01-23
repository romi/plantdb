# data-storage

## Install `romidata` conda package:
```bash
conda create -n romidata romidata -c romi-eu -c open3d-admin --force
```
To test package install, in the activated environment import `romidata` in python:
```bash
conda activate romidata
python -c 'import romidata'
```

## Install from sources in conda environment:

1. Clone the sources:
    ```bash
    git clone https://github.com/romi/conda_recipes.git
    git clone https://github.com/romi/data-storage.git
    ```
2. Create a conda environment:
    ```bash
    conda env create -n romidata_dev -f conda_recipes/environments/romidata.yaml
    ```
3. Install sources:
   ```bash
   conda activate romidata_dev
   cd data-storage
   python setup.py develop
   ```
4. Test `romidata` library:
   ```bash
   conda activate romidata_dev
   conda install nose
   nosetests tests -x -s -v -I test_watch.py -I test_io.py -I test_fsdb.py
   ```

## Build `romidata` conda package:
From the `base` conda environment, run:
```bash
conda build conda_recipes/romidata/ -c romi-eu -c open3d-admin --user romi-eu
```