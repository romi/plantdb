# PlantDB Docker Documentation

## Introduction

This document describes how to build, test, and deploy the PlantDB application using Docker.
PlantDB is a database application for the ROMI project that handles plant-related data management.

## Docker Workflow

### Prerequisites

- Docker installed on your system
- Access to the `roboticsmicrofarms` DockerHub repository (for pushing images)
- Clone of the PlantDB repository

### Step-by-Step Process

For convenience, we'll define a `NEW_TAG` environment variable to use throughout the entire workflow.
This variable represents the version number you want to assign to your build.

```shell
# Set your desired version number
export NEW_TAG=0.14.4
```

#### 1. Running Unit Tests

Before building the runtime image and publishing it, verify it passes all tests:

```shell
# Run the test suite inside the container
./docker/build.sh --test-only --no-cache
```

#### 2. Building the Docker Image

The following command builds a Docker image with the specified tag:

```shell
# Build using the tag set in NEW_TAG
./docker/build.sh -t $NEW_TAG
```

#### 3. Tagging as Latest

After successful testing, tag the version as "latest" for easier referencing:

```shell
# Tag the specific version as 'latest'
docker tag roboticsmicrofarms/plantdb:$NEW_TAG roboticsmicrofarms/plantdb:latest
```

#### 4. Pushing Images to the Registry

Finally, push both the versioned and latest tags to DockerHub:

```shell
# Push the specific version
docker push roboticsmicrofarms/plantdb:$NEW_TAG

# Push the 'latest' version
docker push roboticsmicrofarms/plantdb:latest
```

#### 5. Production Deployment

To deploy the PlantDB application as a standalone container, use our `docker/run.sh` script as follows:

```shell
export ROMI_DB="/path/to/my/database" && \ 
./docker/run.sh -t $NEW_TAG --production 
```


### Complete Workflow Command

For convenience, you can execute the entire workflow with a single command sequence:

```shell
# Set version and execute entire workflow
export NEW_TAG=0.14.4 && \
./docker/build.sh -t $NEW_TAG && \
./docker/run.sh --unittest && \
docker tag roboticsmicrofarms/plantdb:$NEW_TAG roboticsmicrofarms/plantdb:latest && \
docker push roboticsmicrofarms/plantdb:$NEW_TAG && \
docker push roboticsmicrofarms/plantdb:latest
```

This will:
1. Set your version number
2. Build the Docker image with that version tag
3. Run unit tests to verify functionality
4. Tag the image as 'latest'
5. Push both the versioned and latest images to the DockerHub registry

## Troubleshooting

If you encounter issues during the build or test process, check the logs for specific error messages.
For permission issues with the registry, ensure you are logged in to DockerHub with appropriate credentials.