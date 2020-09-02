#!/bin/bash

user=$USER
vtag="latest"
git_branch='dev'

usage() {
  echo "USAGE:"
  echo "  ./build.sh [[-t] [-u] [-b]] | [-h]]
    "

  echo "DESCRIPTION:"
  echo "  Build the docker image using Dockerfile in same location as 'roboticsmicrofarms/romidb'.
    "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'.
    "
  echo "  -u, --user
    User name to create inside docker image, default to '$user'.
    "
  echo "  -b, --branch
    Git branch to use for cloning 'romidata' inside docker image, default to '$git_branch'.
    "
  echo "  -h, --help
    Output a usage message and exit.
    "
}

while [ "$1" != "" ]; do
  case $1 in
  -t | --tag)
    shift
    vtag=$1
    ;;
  -u | --user)
    shift
    user=$1
    ;;
  -b | --branch)
    shift
    git_branch=$1
    ;;
  -h | --help)
    usage
    exit
    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

docker build -t roboticsmicrofarms/romidb:$vtag \
  --build-arg USER_NAME=$user \
  --build-arg ROMIDATA_BRANCH=$git_branch \
  .
