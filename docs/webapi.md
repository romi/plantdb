

## REST URLs

### /scans

* Arguments: none
* Returns: ScanList JSON 


### /scans/<scanid>

* Arguments: none
* Returns: Detailed scan JSON  

### /files/<path>

* Arguments: none
* Returns: The content of the file  


## Object descriptions

### ScanList

* Array of scan summaries


### Scan (summary)

Obtact with fields:
* id: String
* metadata: Scan metadata (see below)
* hasPointCloud: Boolean
* hasMesh: Boolean
* hasSkeleton: Boolean
* hasAngleData: Boolean
* thumbnailUri: String, URL, example "/files/<scanid>/Visualization/thumbnail_pict20190201_134037_0.jpg"


### Scan (detailed)
Obtact with fields:
* id: String
* metadata: Scan metadata (see below)
* hasPointCloud: Boolean
* hasMesh: Boolean
* hasSkeleton: Boolean
* hasAngleData: Boolean
* thumbnailUri: String, URL, example "/files/<scanid>/Visualization/thumbnail_pict20190201_134037_0.jpg"
* camera: Camera object
* data: Data object


### Scan metadata
Object with fields:
* date: String, example "2019-02-01_13-35-42"
* plant: String
* species: String
* nbPhotos: Number
* environment: String
* files: Object
    * files.metadatas: String, URL, example "/files/<scanid>/metadata/metadata.json"
    * files.archive: String, URL, example "/files/<scanid>/Visualization/scan.zip"


### Camera
Object with fields:
* model: Camera model
* poses: Array of Pose objects


### Camera model
Object with fields:
* id: Number
* width: Number
* height: Number
* model: String ("OPENCV", ...)
* params: distortion parameters
    * For "OPENCV": 9x1 array, example [4202.147, 4202.147, 3000, 2000, 0.0050338, 0.0050338, 0, 0]


### Pose object
Object with fields:
* id: String, ID of the picture
* rotmat: Rotation matrix, Array 3x3
* tvec: Translation vector, Array 3x1
* thumbnailUri: String, URL, example "/files/<scanid>/Visualization/thumbnail_<id>.jpg"
* photoUri: String, URL, example "/files/<scanid>/Visualization/image_<id>.jpg"


### Data object
Object with fields:
* angles: 
* skeleton: Skeleton object


### Angles object
Object with fields:
* angles: Array of numbers
* measured_angles: Array of numbers
* internodes: Array of numbers
* measured_internodes: Array of numbers
* fruit_points: Array of ???


### Skeleton object
Object with fields:
* points: Array of points [x, y, z]
* lines: Array of [pointIndex1, pointIndex2]



