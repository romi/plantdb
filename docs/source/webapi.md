# REST URLs

## /scans

* Arguments: none
* Returns: JSON with a ScanList array.

NOTE: Only scans that have a Visualisation fileset are included

## /scans/\[scanid\]

* Arguments: none
* Returns: JSON with a scan details object

## /files/\[path\]

* Arguments: none
* Returns: The content of the file

# Object descriptions

## ScanList

* Array of scan summary objects

## Scan summary object

Object with fields:

* id: String
* thumbnailUri: String, URL, example "/files/\[scanid\]/Visualization/thumbnail_pict20190201_134037_0.jpg"
* metadata: Scan metadata (see below)
* hasPointCloud: Boolean
* hasMesh: Boolean
* hasSkeleton: Boolean
* hasAngleData: Boolean

## Scan details object

Object with fields:

* id: String
* thumbnailUri: String, URL, example "/files/\[scanid\]/Visualization/thumbnail_pict20190201_134037_0.jpg"
* metadata: Metadata object
* hasPointCloud: Boolean
* hasMesh: Boolean
* hasSkeleton: Boolean
* hasAngleData: Boolean
* camera: Camera object
* data: Data object
* workspace: Workspace object
* filesUri:
    * mesh: String URL, if hasMesh is true
    * pointCloud: String URL, if hasPointCloud is true

## Metadata object

Object with fields:

* date: String, example "2019-02-01 13:35:42"
* plant: String
* species: String
* nbPhotos: Number
* environment: String
* files: Object
    * files.metadatas: String, URL, example "/files/\[scanid\]/metadata/metadata.json"
    * files.archive: String, URL, example "/files/\[scanid\]/Visualization/scan.zip"

## Camera object

Object with fields:

* model: Model object
* poses: Array of Pose objects

## Model object

Object with fields:

* id: Number
* width: Number
* height: Number
* model: String ("OPENCV", ...)
* params: distortion parameters
    * For "OPENCV": 9x1 array, example [4202.147, 4202.147, 3000, 2000, 0.0050338, 0.0050338, 0, 0]

## Pose object

Object with fields:

* id: String, ID of the picture
* rotmat: Rotation matrix, Array 3x3
* tvec: Translation vector, Array 3x1
* thumbnailUri: String, URL, example "/files/\[scanid\]/Visualization/thumbnail_\[id\].jpg"
* photoUri: String, URL, example "/files/\[scanid\]/Visualization/image_\[id\].jpg"

## Data object

Object with fields:

* angles: Array of Angle objects, required if hasAngleData is true
* skeleton: Skeleton object, required if hasSkeleton is true

## Angles object

Object with fields:

* angles: Array of numbers
* measured_angles: Array of numbers
* internodes: Array of numbers
* measured_internodes: Array of numbers
* fruit_points: Array of ???

## Workspace object

Object with fields:

* x: [min, max]
* y: [min, max]
* z: [min, max]

## Skeleton object

Object with fields:

* points: Array of points [x, y, z]
* lines: Array of [pointIndex1, pointIndex2]



