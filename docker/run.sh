#!/bin/bash

host_db="/data/ROMI/DB"
vtag="latest"

usage() {
  echo "USAGE:"
  echo "  ./run.sh [[-t] [-p]] | [-h]]
    "

  echo "DESCRIPTION:"
  echo "  Run 'roboticsmicrofarms/romidb:<vtag>' container with a mounted local (host) database and expose it to port 5000.
    "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'.
    "
  echo "  -p, --database_path
    Path to the host database to mount inside docker container, default to '$host_db'.
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
  -p | --database_path)
    shift
    host_db=$1
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

docker run -it -p 5000:5000 \
  -v $host_db:/home/$USER/db \
  roboticsmicrofarms/romidb:$vtag
