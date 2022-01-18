#!/bin/bash

vtag="latest"
host_db="/data/ROMI/DB"
unittest="nosetests plantdb/tests/ --with-coverage --cover-package=plantdb"
cmd=""

usage() {
  echo "USAGE:"
  echo "  ./run.sh [OPTIONS]
    "

  echo "DESCRIPTION:"
  echo "  Run 'roboticsmicrofarms/plantdb' container with a mounted local (host) database and expose it to port 5000.
    "

  echo "OPTIONS:"
  echo "  -t, --tag
    Docker image tag to use, default to '$vtag'."
  echo "  -c, --cmd
    Defines the command to run at container startup.
    By default start an active container serving the database trough the REST API on port 5000."
  echo "  -db, --database
    Path to the host database to mount inside docker container, default to '$host_db'."
  echo " --unittest
    Runs unit tests defined in plantdb/tests/."
  echo "  -h, --help
    Output a usage message and exit."
}

while [ "$1" != "" ]; do
  case $1 in
  -t | --tag)
    shift
    vtag=$1
    ;;
  -c | --cmd)
    shift
    cmd=$1
    ;;
  -db | --database)
    shift
    host_db=$1
    ;;
  --unittest)
    cmd=$unittest
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

if [ "$cmd" = "" ]; then
  # Start in interactive mode:
  docker run \
    -p 5000:5000 \
    -v $host_db:/myapp/db \
    -it roboticsmicrofarms/plantdb:$vtag \
    bash -c "$cmd"
else
  # Start in non-interactive mode (run the command):
  docker run \
    -p 5000:5000 \
    -v $host_db:/myapp/db \
    roboticsmicrofarms/plantdb:$vtag \
    bash -c "$cmd"
fi
