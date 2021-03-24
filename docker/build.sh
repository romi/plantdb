#!/bin/bash

user=$USER
vtag="latest"
git_branch='dev'

usage() {
  echo "USAGE:"
  echo "  ./build.sh [OPTIONS]
    "

  echo "DESCRIPTION:"
  echo "  Build a docker image named 'roboticsmicrofarms/plantdb' using Dockerfile in same location.
    "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'.
    "
  echo "  -u, --user
    User name to create inside docker image, default to '$user'.
    "
  echo "  -b, --branch
    Git branch to use for cloning 'plantdb' inside docker image, default to '$git_branch'.
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

docker build -t roboticsmicrofarms/plantdb:$vtag \
  --build-arg USER_NAME=$user \
  --build-arg PLANTDB_BRANCH=$git_branch \
  .
