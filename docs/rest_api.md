# PlantDB REST API

Hereafter we introduce the URLS to use to communicate via the REST API.
The Python implementation is done in the `plantdb.rest_api` module and the CLI to start a Flask server is in `fsdb_rest_api`.

## REST URLs

### `/scans`

Here are the specifications for the `/scans` URL:

* Resource: [`plantdb.rest_api.ScanList`](reference/rest_api.md#classes)
* Arguments: `filterQuery`
* Returns: a JSON compliant list of dictionaries.
* Example:
  * Get all scans:
  [http://127.0.0.1:5000/scans](http://127.0.0.1:5000/scans)
  * Search for "arabidopsis" in metadata:
  [http://127.0.0.1:5000/scans?filterQuery=arabidopsis](http://127.0.0.1:5000/scans?filterQuery=arabidopsis)

### `/scans/<scan_id>`

Here are the specifications for the `/scans/<scan_id>` URLs:

* Resource: [`plantdb.rest_api.Scan`](reference/rest_api.md#classes)
* Arguments: none
* Returns: a JSON compliant list of dictionaries with detailed information about the scan dataset
* Examples with the test database:
  * Get scan info for the `"real_plant_analyzed"` dataset:
  [http://127.0.0.1:5000/scans/real_plant_analyzed](http://127.0.0.1:5000/scans/real_plant_analyzed)
  * Get scan info for the `"virtual_plant_analyzed"` dataset:
  [http://127.0.0.1:5000/scans/virtual_plant_analyzed](http://127.0.0.1:5000/scans/virtual_plant_analyzed)

!!! warning
    This requires the `Colmap` task!
    In other words it will fail (response `500`) if this task is missing from the dataset.


### `/files/<path>`

Here are the specifications for the `/files/<path>` URLs:

* Resource: [`plantdb.rest_api.File`](reference/rest_api.md#classes)
* Arguments: none
* Returns: The content of the file.
* Examples  `real_plant_analyzed` dataset (from the test database):
  * Get the first image of the scan (`00000_rgb`):
  [http://127.0.0.1:5000/files/real_plant_analyzed/images/00000_rgb.jpg](http://127.0.0.1:5000/files/real_plant_analyzed/images/00000_rgb.jpg)
  * Get the first mask image of the scan (`00000_rgb`):
  [http://127.0.0.1:5000/files/real_plant_analyzed/Masks_1__0__1__0____channel____rgb_5619aa428d/00000_rgb.png](http://127.0.0.1:5000/files/real_plant_analyzed/Masks_1__0__1__0____channel____rgb_5619aa428d/00000_rgb.png)

### `/archive/<scan_id>`

Here are the specifications for the `/archive/<scan_id>` URLs:

* Resource: [`plantdb.rest_api.Archive`](reference/rest_api.md#classes)
* Arguments: none
* Returns: A zip file containing the dataset.
* Examples:
  * Get `real_plant_analyzed` archive:
  [http://127.0.0.1:5000/archive/real_plant_analyzed/](http://127.0.0.1:5000/archive/real_plant_analyzed/)
  * Get `virtual_plant_analyzed` archive:
  [http://127.0.0.1:5000/archive/virtual_plant_analyzed/](http://127.0.0.1:5000/archive/virtual_plant_analyzed/)

### `/image/<scan_id>/<fileset_id>/<file_id>`

Here are the specifications for the `/image/<scan_id>/<fileset_id>/<file_id>` URLs:

* Resource: [`plantdb.rest_api.Image`](reference/rest_api.md#classes)
* Arguments: `size` in {`orig`, `large`, `thumb`} to control the max size (width or height) of the image to return.
* Returns: The image file, resized by default.
* Examples with the `real_plant_analyzed` dataset (from the test database) and the first image of the scan (`00000_rgb`):
  * Get the preview image:
  [http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb](http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb)
  * Get the original image:
  [http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb?size=orig](http://127.0.0.1:5000/image/real_plant_analyzed/images/00000_rgb?size=orig)

### `/pointcloud/<scan_id>/<fileset_id>/<file_id>`

Here are the specifications for the `/pointcloud/<scan_id>/<fileset_id>/<file_id>` URLs:

* Resource: [`plantdb.rest_api.PointCloud`](reference/rest_api.md#classes)
* Arguments: `size` in {`orig`, `preview`} or a `float` to control the voxel-size of the pointcloud to returns.
* Returns: The point cloud file, preview size by default.
* Examples with the `real_plant_analyzed` dataset:
  * Get the preview point cloud:
  [http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud](http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud)
  * Get the original point cloud:
  [http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=orig](http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=orig)
  * Get the point cloud with a voxel size of `2.3`:
  [http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=2.3](http://127.0.0.1:5000/pointcloud/real_plant_analyzed/PointCloud_1_0_1_0_10_0_7ee836e5a9/PointCloud?size=2.3)

### `/pcGroundTruth/<scan_id>/<fileset_id>/<file_id>`

Here are the specifications for the `/pcGroundTruth/<scan_id>/<fileset_id>/<file_id>` URLs:

* Resource: [`plantdb.rest_api.PointCloudGroundTruth`](reference/rest_api.md#classes)
* Arguments: `size` in {`orig`, `preview`} or a `float` to control the voxel-size of the pointcloud to returns.
* Returns: The ground-truth pointcloud file, original size by default.

### `/mesh/<scan_id>/<fileset_id>/<file_id>`

Here are the specifications for the `/mesh/<scan_id>/<fileset_id>/<file_id>` URLs:

* Resource: [`plantdb.rest_api.Mesh`](reference/rest_api.md#classes)
* Arguments: `size` in {`orig`}, no control of the size of the mesh to returns.
* Returns: The mesh file, original size by default.
* Examples with the `real_plant_analyzed` dataset:
  * Get the original mesh:
  [http://127.0.0.1:5000/mesh/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh](http://127.0.0.1:5000/mesh/real_plant_analyzed/TriangleMesh_9_most_connected_t_open3d_00e095c359/TriangleMesh)

### `/refresh`
Refresh the list of scans in the `plantdb.fsdb.FSDB` database.

Here are the specifications for the `/refresh` URL:

* Resource: [`plantdb.rest_api.Refresh`](reference/rest_api.md#classes)
* Arguments: none
* Returns: `200` on completion.
* Example:
[http://127.0.0.1:5000/refresh](http://127.0.0.1:5000/refresh)


## Scan summary
Information about scans dataset, obtained with the '/scans' URL, are grouped in a JSON dictionary.
The template can be accessed here: [`plantdb.rest_api.get_scan_template`](reference/rest_api.md#methods)

It is organized as follows:

```json
{
  "id": "scan_id",
  "metadata": {
    "date": "01-01-00 00:00:00",
    "species": "N/A",
    "plant": "N/A",
    "environment": "N/A",
    "nbPhotos": 0,
    "files": {
      "metadatas": null,
      "archive": null
    }
  },
  "thumbnailUri": "",
  "hasPointCloud": false,
  "hasMesh": false,
  "hasSkeleton": false,
  "hasTreeGraph": false,
  "hasAngleData": false,
  "hasAutomatedMeasures": false,
  "hasManualMeasures": false,
  "hasSegmentation2D": false,
  "hasPcdGroundTruth": false,
  "hasPointCloudEvaluation": false,
  "hasSegmentedPointCloud": false,
  "error": false
  }
```

### id
The name of the corresponding scan dataset.

### metadata
Some metadata gathered from the corresponding scan dataset.

JSON dictionary with fields:

  * "date": String, example "2019-02-01 13:35:42"
  * "plant": String
  * "species": String
  * "nbPhotos": Number
  * "environment": String
  * "files": Object
      * "files.metadatas": String, URL, example "/files/<scanid>/metadata/metadata.json"
      * "files.archive": String, URL, example "/files/<scanid>/Visualization/scan.zip"

### thumbnailUri
A path to the thumbnail image to use as "preview" to represent the dataset in the webapp.  
* thumbnailUri: example "/files/<scanid>/Visualization/thumbnail_pict20190201_134037_0.jpg"

### has*
All of the `has*` entries indicate if there is an output for a list of selected tasks:

  * `hasPointCloud`: output of the task `PointCloud` has a `PointCloud.ply` file
  * `hasMesh`: output of the task `TriangleMesh` has a `TriangleMesh.ply` file
  * `hasSkeleton`: output of the task `CurveSkeleton` has a `CurveSkeleton.json` file
  * `hasAngleData`: output of the task `AngleAndInternodes` has a `AnglesAndInternodes.json` file
  * `hasAutomatedMeasures`: output of the task `AutomatedMeasures` has a `AnglesAndInternodes` file
  * `hasSegmentation2D`: output of the task `Segmentation2D` has a `` file
  * `hasSegmentedPcdEvaluation`: output of the task `Segmentation2DEvaluation` has a `` file
  * `hasPointCloudEvaluation`: output of the task `PointCloudEvaluation` has a `` file
  * `hasSegmentedPointCloud`: output of the task `SegmentedPointCloud` has a `SegmentedPointCloud.ply` file
  * `hasPcdGroundTruth`: output of the task `PointCloudGroundTruth` has a `PointCloudGroundTruth.ply` file

Finally, `hasManualMeasures` refers to the presence of a JSON file with manual measurements saved as `measures.json`.
This file should be present at the scan dataset root directory.


## Scan detailed summary
Information about a specific scan dataset, obtained with the '/scans/<scan_id>' URL, are grouped in a JSON dictionary.

It has the same information as those gathered with the '/scans' URL, but also add:
  * the **files URI** when the following `has*` entries are `True`:
    * `hasPointCloud`, under `"filesUri"/"pointCloud"`
    * `hasMesh`, under `"filesUri"/"mesh"`
    * `hasSkeleton`, under `"filesUri"/"skeleton"`
    * `hasTreeGraph`, under `"filesUri"/"tree"`
  * the **files data** when the following `has*` entries are `True`: 
    * `hasSkeleton`, under `"data"/"skeleton"`
    * `hasAngleData`, under:
      * `"data"/"angles"/"angles"` for the angle values
      * `"data"/"angles"/"internodes"` for the internodes values
    * `hasManualMeasures`, under:
      *   `"data"/"angles"/"measured_angles"` for the angle values
      *   `"data"/"angles"/"measured_internodes"` for the internodes values
  * the **reconstruction bounding-box** (formerly known as 'workspace') under `"workspace"`, for example `"{"x": [340, 440], "y": [330, 410], "z": [-180, 105]}"`
  * the **camera model** and its **intrinsics parameters** under `"camera"/"model"`, see [here](#camera-model) for more details
  * the list of **camera poses**, a.k.a. **extrinsic parameters**, under `"camera"/"poses"`, see [here](#camera-poses) for more details

### Camera model

The details about the camera poses accessible under `"camera"/"model"` are:
  * `id`: the id of the camera model, should always be `1` if we use a single camera
  * `model`: the name of the camera model, should always be 'OPENCV' as this is how we save it
  * `width`: the width of the images
  * `height`: the height of the images
  * `params`: the 'OPENCV' intrinsic camera parameters, that is `fx`, `fy`, `cx`, `cy`, `k1`, `k2`, `p1`, `p2`.  

For example with the first image:
```json
{
  "id": 1,
  "model": "OPENCV",
  "width": 1440,
  "height": 1080,
  "params": [1166.9518889440105, 1166.9518889440105, 720, 540, -0.0013571157486977348, -0.0013571157486977348, 0, 0]
}
```

### Camera poses

The details about the camera poses accessible under `"camera"/"poses"` are:
  * "id": index of the picture (starts at 1 by COLMAP convention)
  * "rotmat": Rotation matrix, 3x3 array 
  * "tvec": Translation vector, 3x1 array
  * "thumbnailUri": URI to the thumbnail image, example "/images/<scanid>/images/<image_id>?size=thumb.jpg"
  * "photoUri": URI to the original image, example "/images/<scanid>/images/<image_id>?size=orig.jpg"

For example with the first image:
```json
{
  "id": 1,
  "tvec": [369.4279687732083, 120.36109311437637, -62.07043190848918],
  "rotmat": [
    [0.06475585405884698, -0.9971710205080586, 0.038165890845442085],
    [-0.3390191175518756, -0.0579549181538338, -0.9389926865509284],
    [0.9385481965778085, 0.04786630673761355, -0.34181295964290737]
  ],
  "photoUri": "/tmp/ROMI_DB/real_plant_analyzed/images/00000_rgb.jpg",
  "thumbnailUri": "/tmp/ROMI_DB/real_plant_analyzed/webcache/6fbae08f195837c511af7c2864d075dd5cd153bc.jpeg", 
  "isMatched": true
}
```
