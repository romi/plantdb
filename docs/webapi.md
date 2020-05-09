

## REST URLs

### /scans

* Arguments: none
* Returns: ScanList JSON 


### /scans/<scanid>

* Arguments: none
* Returns: Detailed scan JSON  


## Object descriptions

### ScanList

* Array of scan summaries


### Scan (summary)

Fields:
* id: String
* metadata: Scan metadata (see below)
* hasPointCloud: Boolean
* hasMesh: Boolean
* hasSkeleton: Boolean
* hasAngleData: Boolean
* thumbnailUri: String, URL, example "/files/<scanid>/Visualization/thumbnail_pict20190201_134037_0.jpg"

### Scan (detailed)


### Scan metadata
* date: String, example "2019-02-01_13-35-42"
* plant: String
* species: String
* nbPhotos: Number
* environment: String
* files: Object
    * files.metadatas: String, URL, example "/files/<scanid>/metadata/metadata.json"
    * files.archive: String, URL, example "/files/<scanid>/Visualization/scan.zip"


### Skeleton



